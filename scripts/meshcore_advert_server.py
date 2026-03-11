#!/usr/bin/env python3
"""Server für ADVERT/PATH-Sammlung und Live-Karte."""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import threading
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

HTML_KARTE = """<!doctype html>
<html lang=\"de\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>MeshCore Repeater Karte</title>
  <link rel=\"stylesheet\" href=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.css\" />
  <style>
    body { margin: 0; font-family: sans-serif; }
    #karte { height: 100vh; width: 100vw; }
    .panel { position: absolute; z-index: 1000; top: 12px; left: 12px; background: #fff; padding: 8px 10px; border-radius: 6px; box-shadow: 0 1px 4px rgba(0,0,0,0.2); }
  </style>
</head>
<body>
  <div class=\"panel\">MeshCore Repeater Live-Karte</div>
  <div id=\"karte\"></div>
  <script src=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.js\"></script>
  <script>
    const karte = L.map('karte').setView([51.0, 10.0], 6);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom: 19 }).addTo(karte);
    const markerEbene = L.layerGroup().addTo(karte);
    const linienEbene = L.layerGroup().addTo(karte);

    async function aktualisieren() {
      const antwort = await fetch('/api/map-data');
      const daten = await antwort.json();
      markerEbene.clearLayers();
      linienEbene.clearLayers();

      const punkte = new Map();
      for (const n of daten.nodes) {
        if (typeof n.latitude !== 'number' || typeof n.longitude !== 'number') continue;
        punkte.set(n.prefix, [n.latitude, n.longitude]);
        const popup = `<b>${n.name || 'Unbenannt'}</b><br>Prefix: ${n.prefix}<br>Key: ${n.public_key || '-'}<br>Letztes ADVERT: ${n.last_seen || '-'}`;
        L.marker([n.latitude, n.longitude]).bindPopup(popup).addTo(markerEbene);
      }

      for (const e of daten.edges) {
        if (!punkte.has(e.von) || !punkte.has(e.nach)) continue;
        L.polyline([punkte.get(e.von), punkte.get(e.nach)], { color: '#2457ff', weight: 3, opacity: 0.7 })
          .bindPopup(`Verbindung: ${e.von} → ${e.nach}`)
          .addTo(linienEbene);
      }
    }

    aktualisieren();
    setInterval(aktualisieren, 5000);
  </script>
</body>
</html>
"""


def zeitstempel_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def prefix_aus_public_key(public_key: str | None) -> str | None:
    if not public_key:
        return None
    bereinigt = "".join(ch for ch in public_key if ch.isalnum())
    if len(bereinigt) < 4:
        return None
    return bereinigt[:4].lower()


def pfadsegmente(path_text: str | None) -> list[str]:
    if not path_text:
        return []
    return [eintrag.lower() for eintrag in re.findall(r"[0-9a-fA-F]{4}", path_text)]


class Datenbank:
    def __init__(self, pfad: Path):
        self.verbindung = sqlite3.connect(pfad, check_same_thread=False)
        self.verbindung.row_factory = sqlite3.Row
        self._sperre = threading.Lock()
        self._initialisieren()

    def _initialisieren(self) -> None:
        with self._sperre:
            self.verbindung.executescript(
                """
                CREATE TABLE IF NOT EXISTS repeaters (
                    prefix TEXT PRIMARY KEY,
                    name TEXT,
                    public_key TEXT,
                    latitude REAL,
                    longitude REAL,
                    last_seen TEXT
                );

                CREATE TABLE IF NOT EXISTS adverts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    received_at TEXT NOT NULL,
                    prefix TEXT,
                    name TEXT,
                    public_key TEXT,
                    latitude REAL,
                    longitude REAL,
                    path TEXT,
                    payload_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS paths (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    received_at TEXT NOT NULL,
                    source_prefix TEXT,
                    path TEXT,
                    payload_json TEXT NOT NULL
                );
                """
            )
            self.verbindung.commit()

    def speichere_event(self, payload: dict[str, Any]) -> None:
        typ = payload.get("payload_typename")
        if typ not in {"ADVERT", "PATH"}:
            raise ValueError("Nur ADVERT und PATH erlaubt")

        zeit = zeitstempel_utc()
        public_key = payload.get("adv_key") or payload.get("public_key")
        prefix = prefix_aus_public_key(public_key)
        path_text = payload.get("path")

        with self._sperre:
            if typ == "ADVERT":
                self.verbindung.execute(
                    """
                    INSERT INTO adverts (received_at, prefix, name, public_key, latitude, longitude, path, payload_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        zeit,
                        prefix,
                        payload.get("adv_name") or payload.get("name"),
                        public_key,
                        payload.get("adv_lat") or payload.get("latitude"),
                        payload.get("adv_lon") or payload.get("longitude"),
                        path_text,
                        json.dumps(payload, ensure_ascii=False),
                    ),
                )
                if prefix:
                    self.verbindung.execute(
                        """
                        INSERT INTO repeaters (prefix, name, public_key, latitude, longitude, last_seen)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(prefix) DO UPDATE SET
                            name=excluded.name,
                            public_key=excluded.public_key,
                            latitude=COALESCE(excluded.latitude, repeaters.latitude),
                            longitude=COALESCE(excluded.longitude, repeaters.longitude),
                            last_seen=excluded.last_seen
                        """,
                        (
                            prefix,
                            payload.get("adv_name") or payload.get("name"),
                            public_key,
                            payload.get("adv_lat") or payload.get("latitude"),
                            payload.get("adv_lon") or payload.get("longitude"),
                            zeit,
                        ),
                    )
            else:
                self.verbindung.execute(
                    """
                    INSERT INTO paths (received_at, source_prefix, path, payload_json)
                    VALUES (?, ?, ?, ?)
                    """,
                    (zeit, prefix, path_text, json.dumps(payload, ensure_ascii=False)),
                )
            self.verbindung.commit()

    def map_daten(self) -> dict[str, Any]:
        with self._sperre:
            nodes = [dict(zeile) for zeile in self.verbindung.execute("SELECT * FROM repeaters")]
            edges: set[tuple[str, str]] = set()

            for zeile in self.verbindung.execute("SELECT source_prefix, path FROM paths"):
                quelle = zeile["source_prefix"]
                segmente = pfadsegmente(zeile["path"])
                if quelle:
                    segmente = [quelle] + segmente
                for a, b in zip(segmente, segmente[1:]):
                    if a != b:
                        edges.add((a, b))

            for zeile in self.verbindung.execute("SELECT prefix, path FROM adverts WHERE path IS NOT NULL"):
                segmente = pfadsegmente(zeile["path"])
                if zeile["prefix"]:
                    segmente = [zeile["prefix"]] + segmente
                for a, b in zip(segmente, segmente[1:]):
                    if a != b:
                        edges.add((a, b))

        return {"nodes": nodes, "edges": [{"von": a, "nach": b} for a, b in sorted(edges)]}


class Handler(BaseHTTPRequestHandler):
    datenbank: Datenbank

    def _json_antwort(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
        roh = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(roh)))
        self.end_headers()
        self.wfile.write(roh)

    def do_GET(self) -> None:  # noqa: N802
        pfad = urlparse(self.path).path
        if pfad == "/":
            roh = HTML_KARTE.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(roh)))
            self.end_headers()
            self.wfile.write(roh)
            return

        if pfad == "/api/map-data":
            self._json_antwort(HTTPStatus.OK, self.datenbank.map_daten())
            return

        self._json_antwort(HTTPStatus.NOT_FOUND, {"fehler": "nicht gefunden"})

    def do_POST(self) -> None:  # noqa: N802
        pfad = urlparse(self.path).path
        if pfad != "/api/events":
            self._json_antwort(HTTPStatus.NOT_FOUND, {"fehler": "nicht gefunden"})
            return

        laenge = int(self.headers.get("Content-Length", "0"))
        inhalt = self.rfile.read(laenge)
        try:
            payload = json.loads(inhalt.decode("utf-8"))
        except json.JSONDecodeError:
            self._json_antwort(HTTPStatus.BAD_REQUEST, {"fehler": "Ungültiges JSON"})
            return

        if not isinstance(payload, dict):
            self._json_antwort(HTTPStatus.BAD_REQUEST, {"fehler": "JSON-Objekt erwartet"})
            return

        try:
            self.datenbank.speichere_event(payload)
        except ValueError as exc:
            self._json_antwort(HTTPStatus.BAD_REQUEST, {"fehler": str(exc)})
            return

        self._json_antwort(HTTPStatus.ACCEPTED, {"status": "ok"})


def parse_argumente() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ADVERT/PATH Server mit Live-Karte")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--db", type=Path, default=Path("data/meshcore_map.db"))
    return parser.parse_args()


def main() -> None:
    args = parse_argumente()
    args.db.parent.mkdir(parents=True, exist_ok=True)

    handler = Handler
    handler.datenbank = Datenbank(args.db)
    server = ThreadingHTTPServer((args.host, args.port), handler)
    print(f"[INFO] Server läuft auf http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[INFO] Server beendet")


if __name__ == "__main__":
    main()
