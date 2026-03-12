#!/usr/bin/env python3
"""Server für ADVERT/PATH-Sammlung und Live-Karte."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import os
import re
import sqlite3
import tempfile
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
    .titel-panel {
      position: absolute;
      z-index: 1000;
      top: 12px;
      left: 50%;
      transform: translateX(-50%);
      background: #fff;
      padding: 8px 12px;
      border-radius: 6px;
      box-shadow: 0 1px 4px rgba(0,0,0,0.2);
      font-weight: 600;
    }
    .fusszeile {
      position: absolute;
      z-index: 1000;
      bottom: 12px;
      left: 50%;
      transform: translateX(-50%);
      background: rgba(255, 255, 255, 0.9);
      padding: 6px 10px;
      border-radius: 6px;
      box-shadow: 0 1px 4px rgba(0,0,0,0.2);
      font-size: 0.85rem;
    }
    .prefix-marker-container {
      background: transparent;
      border: none;
    }
    .prefix-marker {
      width: 34px;
      height: 34px;
      border-radius: 50%;
      border: 2px solid #1f2937;
      background: #f8fafc;
      color: #111827;
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 0.8rem;
      font-weight: 700;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
      text-transform: lowercase;
      box-shadow: 0 1px 3px rgba(0, 0, 0, 0.35);
      line-height: 1;
      box-sizing: border-box;
    }
    .schluessel-zeile {
      display: block;
      max-width: 220px;
      overflow-wrap: anywhere;
      word-break: break-word;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
      font-size: 0.8rem;
    }
  </style>
</head>
<body>
  <div class=\"titel-panel\">MeshCore Repeater Live-Karte</div>
  <div id=\"karte\"></div>
  <footer class=\"fusszeile\">Copyright 2026 by Erik Schauer, do1ffe@darc.de</footer>
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
        punkte.set(n.id, [n.latitude, n.longitude]);
        const prefixAnzeige = Array.isArray(n.prefixes) && n.prefixes.length ? n.prefixes.join(', ') : (n.prefix || '-');
        const roherPrefix = n.prefix || (Array.isArray(n.prefixes) && n.prefixes.length ? n.prefixes[0] : null) || '--';
        const markerText = String(roherPrefix).slice(0, 2).toLowerCase();
        const popup = `<b>${n.name || 'Unbenannt'}</b><br>ID: ${n.id}<br>Prefix(e): ${prefixAnzeige}<br>Key: <span class="schluessel-zeile">${n.public_key || '-'}</span><br>Letztes ADVERT: ${n.last_seen || '-'}`;
        const icon = L.divIcon({
          className: 'prefix-marker-container',
          html: `<div class="prefix-marker">${markerText}</div>`,
          iconSize: [34, 34],
          iconAnchor: [17, 17],
          popupAnchor: [0, -16]
        });
        L.marker([n.latitude, n.longitude], { icon }).bindPopup(popup).addTo(markerEbene);
      }

      for (const e of daten.edges) {
        if (!punkte.has(e.von_id) || !punkte.has(e.nach_id)) continue;
        L.polyline([punkte.get(e.von_id), punkte.get(e.nach_id)], { color: '#2457ff', weight: 3, opacity: 0.7 })
          .bindPopup(`Verbindung: ${e.von_id} → ${e.nach_id}`)
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
    if len(bereinigt) < 2:
        return None
    return bereinigt[:2].lower()


def pfadsegmente(path_text: str | None) -> list[str]:
    if not path_text:
        return []
    # PATH-Segmente bleiben 4-stellig (2 Byte) wie im Protokoll.
    return [eintrag.lower() for eintrag in re.findall(r"[0-9a-fA-F]{4}", path_text)]


def prefix_aus_pfadsegment(segment: str | None) -> str | None:
    if not segment:
        return None
    bereinigt = "".join(ch for ch in segment if ch.isalnum())
    if len(bereinigt) < 2:
        return None
    return bereinigt[:2].lower()


def _normalisiere_prefix(prefix: str | None) -> str | None:
    if not prefix:
        return None
    normalisiert = prefix.strip().lower()
    if len(normalisiert) != 2:
        return None
    if not all(zeichen in "0123456789abcdef" for zeichen in normalisiert):
        return None
    return normalisiert


def _normalisiere_schluessel(schluessel: str | None) -> str | None:
    if not schluessel:
        return None
    bereinigt = "".join(zeichen for zeichen in schluessel if zeichen.isalnum()).lower()
    if not bereinigt:
        return None
    return bereinigt


def _schreibe_prefixdatei_atomar(dateipfad: Path, prefixe: list[str]) -> None:
    dateipfad.parent.mkdir(parents=True, exist_ok=True)
    inhalt = "\n".join(prefixe) + "\n"
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=dateipfad.parent, delete=False) as temp_datei:
        temp_datei.write(inhalt)
        temp_name = temp_datei.name
    os.replace(temp_name, dateipfad)


def _lock_pfad_fuer(dateipfad: Path) -> Path:
    return dateipfad.with_suffix(dateipfad.suffix + ".lock")


def initialisiere_unbenutzte_prefixe(dateipfad: Path) -> None:
    dateipfad.parent.mkdir(parents=True, exist_ok=True)
    lock_pfad = _lock_pfad_fuer(dateipfad)
    with lock_pfad.open("a+", encoding="utf-8") as lock_datei:
        fcntl.flock(lock_datei.fileno(), fcntl.LOCK_EX)
        if dateipfad.exists():
            return
        alle_prefixe = [f"{wert:02x}" for wert in range(256)]
        _schreibe_prefixdatei_atomar(dateipfad, alle_prefixe)


def _prefixe_aus_inhalt(inhalt: str) -> list[str]:
    prefixe: list[str] = []
    gesehen: set[str] = set()
    for zeile in inhalt.splitlines():
        normalisiert = _normalisiere_prefix(zeile)
        if not normalisiert or normalisiert in gesehen:
            continue
        gesehen.add(normalisiert)
        prefixe.append(normalisiert)
    return prefixe


def lese_unbenutzte_prefixe(dateipfad: Path) -> list[str]:
    initialisiere_unbenutzte_prefixe(dateipfad)
    lock_pfad = _lock_pfad_fuer(dateipfad)
    with lock_pfad.open("a+", encoding="utf-8") as lock_datei:
        fcntl.flock(lock_datei.fileno(), fcntl.LOCK_EX)
        inhalt = dateipfad.read_text(encoding="utf-8")
        return _prefixe_aus_inhalt(inhalt)


def markiere_prefix_als_benutzt(dateipfad: Path, prefix: str) -> None:
    normalisiert = _normalisiere_prefix(prefix)
    if not normalisiert:
        return
    initialisiere_unbenutzte_prefixe(dateipfad)
    lock_pfad = _lock_pfad_fuer(dateipfad)
    with lock_pfad.open("a+", encoding="utf-8") as lock_datei:
        fcntl.flock(lock_datei.fileno(), fcntl.LOCK_EX)
        inhalt = dateipfad.read_text(encoding="utf-8")
        vorhandene_prefixe = _prefixe_aus_inhalt(inhalt)
        neue_prefixe = [eintrag for eintrag in vorhandene_prefixe if eintrag != normalisiert]
        if len(neue_prefixe) == len(vorhandene_prefixe):
            return
        _schreibe_prefixdatei_atomar(dateipfad, neue_prefixe)


def distanz_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Berechnet die Großkreisdistanz zweier Koordinaten in Kilometern."""
    radius_erde_km = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    return 2 * radius_erde_km * math.atan2(math.sqrt(a), math.sqrt(1 - a))


class Datenbank:
    def __init__(self, pfad: Path, unbenutzte_prefix_datei: Path):
        self.unbenutzte_prefix_datei = unbenutzte_prefix_datei
        initialisiere_unbenutzte_prefixe(self.unbenutzte_prefix_datei)
        self.verbindung = sqlite3.connect(pfad, check_same_thread=False)
        self.verbindung.row_factory = sqlite3.Row
        self._sperre = threading.Lock()
        self._initialisieren()

    def _initialisieren(self) -> None:
        with self._sperre:
            self.verbindung.executescript(
                """
                CREATE TABLE IF NOT EXISTS repeaters (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    public_key TEXT,
                    latitude REAL,
                    longitude REAL,
                    last_seen TEXT
                );

                CREATE TABLE IF NOT EXISTS repeater_aliases (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    repeater_id INTEGER NOT NULL,
                    prefix TEXT NOT NULL,
                    UNIQUE(repeater_id, prefix),
                    FOREIGN KEY(repeater_id) REFERENCES repeaters(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS adverts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_schluessel TEXT,
                    received_at TEXT NOT NULL,
                    repeater_id INTEGER,
                    prefix TEXT,
                    name TEXT,
                    public_key TEXT,
                    latitude REAL,
                    longitude REAL,
                    path TEXT,
                    payload_json TEXT NOT NULL,
                    FOREIGN KEY(repeater_id) REFERENCES repeaters(id)
                );

                CREATE TABLE IF NOT EXISTS paths (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_schluessel TEXT,
                    received_at TEXT NOT NULL,
                    source_prefix TEXT,
                    path TEXT,
                    payload_json TEXT NOT NULL
                );
                """
            )
            self._migration_altbestand()
            self.verbindung.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_adverts_event_schluessel
                ON adverts(event_schluessel)
                """
            )
            self.verbindung.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_paths_event_schluessel
                ON paths(event_schluessel)
                """
            )
            self.verbindung.commit()

    def _migration_altbestand(self) -> None:
        spalten = {zeile["name"] for zeile in self.verbindung.execute("PRAGMA table_info(repeaters)")}
        if "id" in spalten and "prefix" not in spalten:
            return

        self.verbindung.executescript(
            """
            ALTER TABLE repeaters RENAME TO repeaters_alt;
            CREATE TABLE repeaters (
                id INTEGER PRIMARY KEY,
                name TEXT,
                public_key TEXT,
                latitude REAL,
                longitude REAL,
                last_seen TEXT
            );
            """
        )
        for zeile in self.verbindung.execute(
            "SELECT prefix, name, public_key, latitude, longitude, last_seen FROM repeaters_alt"
        ):
            cursor = self.verbindung.execute(
                """
                INSERT INTO repeaters (name, public_key, latitude, longitude, last_seen)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    zeile["name"],
                    zeile["public_key"],
                    zeile["latitude"],
                    zeile["longitude"],
                    zeile["last_seen"],
                ),
            )
            if zeile["prefix"]:
                self.verbindung.execute(
                    "INSERT OR IGNORE INTO repeater_aliases (repeater_id, prefix) VALUES (?, ?)",
                    (cursor.lastrowid, zeile["prefix"]),
                )
        self.verbindung.execute("DROP TABLE repeaters_alt")

        advert_spalten = {zeile["name"] for zeile in self.verbindung.execute("PRAGMA table_info(adverts)")}
        if "event_schluessel" not in advert_spalten:
            self.verbindung.execute("ALTER TABLE adverts ADD COLUMN event_schluessel TEXT")
        if "repeater_id" not in advert_spalten:
            self.verbindung.execute("ALTER TABLE adverts ADD COLUMN repeater_id INTEGER")

        path_spalten = {zeile["name"] for zeile in self.verbindung.execute("PRAGMA table_info(paths)")}
        if "event_schluessel" not in path_spalten:
            self.verbindung.execute("ALTER TABLE paths ADD COLUMN event_schluessel TEXT")

    @staticmethod
    def _event_schluessel(
        typ: str,
        *,
        public_key: str | None,
        prefix: str | None,
        path_text: str | None,
    ) -> str:
        if typ == "ADVERT":
            identitaet = public_key or prefix
            identitaet = (identitaet or json.dumps({"public_key": public_key, "prefix": prefix}, ensure_ascii=False, sort_keys=True))
            roh = f"ADVERT:{identitaet.strip().lower()}"
            return hashlib.sha256(roh.encode("utf-8")).hexdigest()

        if typ == "PATH":
            roh = f"PATH:{(prefix or '').strip().lower()}:{(path_text or '').strip().lower()}"
            return hashlib.sha256(roh.encode("utf-8")).hexdigest()

        roh = json.dumps({"typ": typ, "public_key": public_key, "prefix": prefix, "path": path_text}, ensure_ascii=False, sort_keys=True)
        return hashlib.sha256(roh.encode("utf-8")).hexdigest()

    @staticmethod
    def _koordinate(payload: dict[str, Any], feld_adv: str, feld_std: str) -> float | None:
        rohwert = payload.get(feld_adv)
        if rohwert is None:
            rohwert = payload.get(feld_std)
        if rohwert is None:
            return None
        try:
            return float(rohwert)
        except (TypeError, ValueError):
            return None

    def _repeater_fuer_advert(
        self,
        *,
        prefix: str,
        latitude: float | None,
        longitude: float | None,
        name: str | None,
        public_key: str | None,
        zeit: str,
    ) -> int:
        normalisierter_public_key = _normalisiere_schluessel(public_key)
        repeater_id: int | None = None

        if normalisierter_public_key:
            for zeile in self.verbindung.execute(
                """
                SELECT id, public_key
                FROM repeaters
                WHERE public_key IS NOT NULL
                """
            ):
                if _normalisiere_schluessel(zeile["public_key"]) == normalisierter_public_key:
                    repeater_id = zeile["id"]
                    break

        if repeater_id is None and not normalisierter_public_key:
            kandidaten = list(
                self.verbindung.execute(
                    """
                    SELECT r.id, r.latitude, r.longitude
                    FROM repeaters r
                    JOIN repeater_aliases a ON a.repeater_id = r.id
                    WHERE a.prefix = ?
                    """,
                    (prefix,),
                )
            )

            if latitude is not None and longitude is not None:
                distanz_kandidaten: list[tuple[float, int]] = []
                for kandidat in kandidaten:
                    if kandidat["latitude"] is None or kandidat["longitude"] is None:
                        continue
                    distanz = distanz_km(latitude, longitude, kandidat["latitude"], kandidat["longitude"])
                    if distanz <= 20.0:
                        distanz_kandidaten.append((distanz, kandidat["id"]))
                if distanz_kandidaten:
                    distanz_kandidaten.sort(key=lambda eintrag: eintrag[0])
                    repeater_id = distanz_kandidaten[0][1]
            elif kandidaten:
                repeater_id = kandidaten[0]["id"]

        if repeater_id is None:
            cursor = self.verbindung.execute(
                """
                INSERT INTO repeaters (name, public_key, latitude, longitude, last_seen)
                VALUES (?, ?, ?, ?, ?)
                """,
                (name, public_key, latitude, longitude, zeit),
            )
            repeater_id = cursor.lastrowid
        else:
            self.verbindung.execute(
                """
                UPDATE repeaters
                SET name = COALESCE(?, name),
                    public_key = COALESCE(?, public_key),
                    latitude = COALESCE(?, latitude),
                    longitude = COALESCE(?, longitude),
                    last_seen = ?
                WHERE id = ?
                """,
                (name, public_key, latitude, longitude, zeit, repeater_id),
            )

        self.verbindung.execute(
            "INSERT OR IGNORE INTO repeater_aliases (repeater_id, prefix) VALUES (?, ?)",
            (repeater_id, prefix),
        )
        return repeater_id

    def speichere_event(self, payload: dict[str, Any]) -> None:
        typ = payload.get("payload_typename")
        if typ not in {"ADVERT", "PATH"}:
            raise ValueError("Nur ADVERT und PATH erlaubt")

        zeit = zeitstempel_utc()
        public_key = payload.get("adv_key") or payload.get("public_key")
        prefix = prefix_aus_public_key(public_key)
        path_text = payload.get("path")
        name = payload.get("adv_name") or payload.get("name")
        latitude = self._koordinate(payload, "adv_lat", "latitude")
        longitude = self._koordinate(payload, "adv_lon", "longitude")

        with self._sperre:
            event_schluessel = self._event_schluessel(
                typ,
                public_key=public_key,
                prefix=prefix,
                path_text=path_text,
            )
            if typ == "ADVERT":
                repeater_id = None
                if prefix:
                    prefix = prefix.lower()[:2]
                    repeater_id = self._repeater_fuer_advert(
                        prefix=prefix,
                        latitude=latitude,
                        longitude=longitude,
                        name=name,
                        public_key=public_key,
                        zeit=zeit,
                    )
                    markiere_prefix_als_benutzt(self.unbenutzte_prefix_datei, prefix)
                self.verbindung.execute(
                    """
                    INSERT INTO adverts (
                        event_schluessel, received_at, repeater_id, prefix, name, public_key, latitude, longitude, path, payload_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT DO UPDATE SET
                        received_at = excluded.received_at,
                        repeater_id = COALESCE(excluded.repeater_id, adverts.repeater_id),
                        prefix = COALESCE(excluded.prefix, adverts.prefix),
                        name = COALESCE(excluded.name, adverts.name),
                        public_key = COALESCE(excluded.public_key, adverts.public_key),
                        latitude = COALESCE(excluded.latitude, adverts.latitude),
                        longitude = COALESCE(excluded.longitude, adverts.longitude),
                        path = COALESCE(excluded.path, adverts.path),
                        payload_json = excluded.payload_json
                    """,
                    (
                        event_schluessel,
                        zeit,
                        repeater_id,
                        prefix,
                        name,
                        public_key,
                        latitude,
                        longitude,
                        path_text,
                        json.dumps(payload, ensure_ascii=False),
                    ),
                )
            else:
                self.verbindung.execute(
                    """
                    INSERT INTO paths (event_schluessel, received_at, source_prefix, path, payload_json)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT DO UPDATE SET
                        received_at = excluded.received_at,
                        source_prefix = COALESCE(excluded.source_prefix, paths.source_prefix),
                        path = COALESCE(excluded.path, paths.path),
                        payload_json = excluded.payload_json
                    """,
                    (event_schluessel, zeit, prefix, path_text, json.dumps(payload, ensure_ascii=False)),
                )
            self.verbindung.commit()

    def map_daten(self) -> dict[str, Any]:
        with self._sperre:
            nodes = []
            for zeile in self.verbindung.execute(
                """
                SELECT
                    r.id,
                    r.name,
                    r.public_key,
                    r.latitude,
                    r.longitude,
                    r.last_seen,
                    GROUP_CONCAT(a.prefix) AS prefixes
                FROM repeaters r
                LEFT JOIN repeater_aliases a ON a.repeater_id = r.id
                GROUP BY r.id
                ORDER BY r.id
                """
            ):
                prefixes = [prefix for prefix in (zeile["prefixes"] or "").split(",") if prefix]
                nodes.append(
                    {
                        "id": zeile["id"],
                        "prefix": prefixes[0] if prefixes else None,
                        "prefixes": prefixes,
                        "name": zeile["name"],
                        "public_key": zeile["public_key"],
                        "latitude": zeile["latitude"],
                        "longitude": zeile["longitude"],
                        "last_seen": zeile["last_seen"],
                    }
                )

            alias_map: dict[str, list[int]] = {}
            for zeile in self.verbindung.execute("SELECT repeater_id, prefix FROM repeater_aliases"):
                alias_map.setdefault(zeile["prefix"], []).append(zeile["repeater_id"])

            repeater_positionen: dict[int, tuple[float, float]] = {}
            for knoten in nodes:
                latitude = knoten.get("latitude")
                longitude = knoten.get("longitude")
                if latitude is None or longitude is None:
                    continue
                repeater_positionen[knoten["id"]] = (float(latitude), float(longitude))

            gerichtete_kanten: set[tuple[int, int]] = set()

            def waehle_kandidat(prefix: str, vorherige_id: int | None) -> int | None:
                kandidaten = alias_map.get(prefix, [])
                if not kandidaten:
                    return None

                kandidaten_mit_koordinaten = [kid for kid in kandidaten if kid in repeater_positionen]
                if not kandidaten_mit_koordinaten:
                    return None

                if vorherige_id in repeater_positionen:
                    lat_vorher, lon_vorher = repeater_positionen[vorherige_id]
                    return min(
                        kandidaten_mit_koordinaten,
                        key=lambda kid: distanz_km(
                            lat_vorher,
                            lon_vorher,
                            repeater_positionen[kid][0],
                            repeater_positionen[kid][1],
                        ),
                    )
                return min(kandidaten_mit_koordinaten)

            def kante_hinzufuegen(von_id: int | None, nach_id: int | None) -> None:
                if von_id is None or nach_id is None or von_id == nach_id:
                    return
                if von_id not in repeater_positionen or nach_id not in repeater_positionen:
                    return
                lat_von, lon_von = repeater_positionen[von_id]
                lat_nach, lon_nach = repeater_positionen[nach_id]
                if distanz_km(lat_von, lon_von, lat_nach, lon_nach) <= 20.0:
                    gerichtete_kanten.add((von_id, nach_id))

            def aufgeloeste_ids(segmente: list[str], start_id: int | None = None) -> list[int]:
                ids: list[int] = []
                vorherige_id = start_id
                if start_id is not None:
                    ids.append(start_id)
                for segment in segmente:
                    segment_prefix = prefix_aus_pfadsegment(segment)
                    if not segment_prefix:
                        continue
                    kandidat = waehle_kandidat(segment_prefix, vorherige_id)
                    if kandidat is None:
                        continue
                    ids.append(kandidat)
                    vorherige_id = kandidat
                return ids

            for zeile in self.verbindung.execute("SELECT source_prefix, path FROM paths"):
                segmente = pfadsegmente(zeile["path"])
                start_id = None
                if zeile["source_prefix"]:
                    start_id = waehle_kandidat(zeile["source_prefix"], None)
                ids = aufgeloeste_ids(segmente, start_id)
                for a, b in zip(ids, ids[1:]):
                    kante_hinzufuegen(a, b)

            for zeile in self.verbindung.execute(
                "SELECT repeater_id, prefix, path FROM adverts WHERE path IS NOT NULL"
            ):
                segmente = pfadsegmente(zeile["path"])
                if zeile["repeater_id"]:
                    ids = aufgeloeste_ids(segmente, zeile["repeater_id"])
                    for a, b in zip(ids, ids[1:]):
                        kante_hinzufuegen(a, b)
                elif zeile["prefix"]:
                    start_id = waehle_kandidat(zeile["prefix"], None)
                    ids = aufgeloeste_ids(segmente, start_id)
                    for a, b in zip(ids, ids[1:]):
                        kante_hinzufuegen(a, b)

            edges: set[tuple[int, int]] = set()
            for von_id, nach_id in gerichtete_kanten:
                if (nach_id, von_id) not in gerichtete_kanten:
                    continue
                edges.add(tuple(sorted((von_id, nach_id))))

        return {
            "nodes": nodes,
            "edges": [{"von_id": a, "nach_id": b} for a, b in sorted(edges)],
        }


class Handler(BaseHTTPRequestHandler):
    datenbank: Datenbank
    unbenutzte_prefix_datei: Path

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

        if pfad == "/api/unused-prefixes":
            self._json_antwort(HTTPStatus.OK, {"prefixes": lese_unbenutzte_prefixe(self.unbenutzte_prefix_datei)})
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
    parser.add_argument("--port", type=int, default=8023)
    parser.add_argument("--db", type=Path, default=Path("data/meshcore_map.db"))
    parser.add_argument("--unused-prefix-file", type=Path, default=Path("data/unbenutzte_prefixe.txt"))
    return parser.parse_args()


def main() -> None:
    args = parse_argumente()
    args.db.parent.mkdir(parents=True, exist_ok=True)
    args.unused_prefix_file.parent.mkdir(parents=True, exist_ok=True)

    handler = Handler
    handler.unbenutzte_prefix_datei = args.unused_prefix_file
    handler.datenbank = Datenbank(args.db, args.unused_prefix_file)
    server = ThreadingHTTPServer((args.host, args.port), handler)
    print(f"[INFO] Server läuft auf http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[INFO] Server beendet")


if __name__ == "__main__":
    main()
