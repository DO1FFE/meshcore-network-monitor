#!/usr/bin/env python3
"""Server für ADVERT/PATH-Sammlung und Live-Karte."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import html
import json
import math
import os
import re
import sqlite3
import tempfile
import threading
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

ERLAUBTE_MAX_AGE_STUNDEN = {1, 3, 6, 12, 24, 168}
ERLAUBTE_MAX_AGE_WERTE_TEXT = ", ".join(str(wert) for wert in sorted(ERLAUBTE_MAX_AGE_STUNDEN)) + ", all"
CLIENT_TIMEOUT = timedelta(minutes=10)
AUFBEWAHRUNGSDAUER = timedelta(days=7)
MAXIMALE_POST_NUTZLAST_BYTES = 5 * 1024 * 1024

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
    .status-panel {
      position: absolute;
      z-index: 1000;
      bottom: 12px;
      right: 12px;
      background: rgba(255, 255, 255, 0.95);
      padding: 8px 10px;
      border-radius: 6px;
      box-shadow: 0 1px 4px rgba(0,0,0,0.2);
      font-size: 0.85rem;
      line-height: 1.4;
      min-width: 240px;
    }
    .filter-panel {
      position: absolute;
      z-index: 1000;
      top: 12px;
      right: 12px;
      background: rgba(255, 255, 255, 0.95);
      padding: 8px 10px;
      border-radius: 6px;
      box-shadow: 0 1px 4px rgba(0,0,0,0.2);
      font-size: 0.85rem;
      min-width: 180px;
    }
    .filter-panel label {
      display: block;
      margin-bottom: 4px;
      font-weight: 600;
    }
    .filter-auswahl {
      width: 100%;
      box-sizing: border-box;
      padding: 4px 6px;
      border: 1px solid #d1d5db;
      border-radius: 4px;
      font-size: 0.85rem;
      background: #fff;
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
    .client-button {
      margin-top: 6px;
      width: 100%;
      border: 1px solid #d1d5db;
      border-radius: 4px;
      background: #f8fafc;
      padding: 4px 6px;
      cursor: pointer;
      font-size: 0.82rem;
    }
    .popup-overlay {
      position: fixed;
      inset: 0;
      background: rgba(0, 0, 0, 0.45);
      display: none;
      align-items: center;
      justify-content: center;
      z-index: 2000;
    }
    .popup-overlay.sichtbar {
      display: flex;
    }
    .popup-inhalt {
      width: min(360px, calc(100vw - 32px));
      max-height: min(420px, calc(100vh - 32px));
      background: #fff;
      border-radius: 8px;
      box-shadow: 0 8px 24px rgba(0,0,0,0.35);
      padding: 12px;
      display: flex;
      flex-direction: column;
      gap: 8px;
    }
    .popup-kopfzeile {
      display: flex;
      justify-content: space-between;
      align-items: center;
      font-weight: 600;
    }
    .popup-schliessen {
      border: none;
      background: transparent;
      font-size: 1rem;
      cursor: pointer;
    }
    .client-liste {
      margin: 0;
      padding-left: 20px;
      overflow: auto;
      font-size: 0.85rem;
    }
    .download-panel {
      position: absolute;
      z-index: 1000;
      bottom: 12px;
      left: 12px;
    }
    .download-link {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      text-decoration: none;
      color: #0f172a;
      background: rgba(255, 255, 255, 0.95);
      border: 1px solid #d1d5db;
      border-radius: 6px;
      padding: 7px 10px;
      box-shadow: 0 1px 4px rgba(0,0,0,0.2);
      font-size: 0.85rem;
      font-weight: 600;
      transition: background-color 120ms ease-in-out;
    }
    .download-link:hover,
    .download-link:focus-visible {
      background: #e2e8f0;
    }
    .download-icon {
      width: 20px;
      height: 20px;
      border-radius: 50%;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      background: #1d4ed8;
      color: #fff;
      font-size: 0.8rem;
      line-height: 1;
      font-weight: 700;
    }
    @media (max-width: 820px) {
      .titel-panel {
        top: 10px;
        left: 12px;
        right: 12px;
        transform: none;
        max-width: none;
        font-size: 1.25rem;
        line-height: 1.2;
      }
      .filter-panel {
        top: 84px;
        left: 12px;
        right: 12px;
        min-width: 0;
      }
      .status-panel {
        left: 12px;
        right: 12px;
        bottom: calc(74px + env(safe-area-inset-bottom, 0px));
        min-width: 0;
        max-width: none;
      }
      .download-panel {
        left: 12px;
        right: 12px;
        bottom: calc(12px + env(safe-area-inset-bottom, 0px));
      }
      .download-link {
        justify-content: center;
        width: 100%;
      }
      .fusszeile {
        display: none;
      }
    }
  </style>
</head>
<body>
  <div class=\"titel-panel\">MeshCore Repeater Live-Karte</div>
  <div class=\"filter-panel\">
    <label for=\"zeitfilter-auswahl\">Zeitraum</label>
    <select id=\"zeitfilter-auswahl\" class=\"filter-auswahl\">
      <option value=\"1\">1 Stunde</option>
      <option value=\"3\">3 Stunden</option>
      <option value=\"6\">6 Stunden</option>
      <option value=\"12\">12 Stunden</option>
      <option value=\"24\">24 Stunden</option>
      <option value=\"168\">7 Tage</option>
      <option value=\"all\" selected>ALLE</option>
    </select>
  </div>
  <div id=\"karte\"></div>
  <div class="status-panel" id="status-panel">
    Gesamtanzahl Repeater: <span id="gesamt-repeater">0</span><br>
    Sichtbare Repeater: <span id="sichtbare-repeater">0</span><br>
    Letztes Datenpaket: <span id="letztes-datenpaket">-</span><br>
    Filter: <span id="aktiver-filter">ALLE</span><br>
    Verbundene Clients: <span id="anzahl-verbundene-clients">0</span>
    <button id="verbundene-clients-button" class="client-button" type="button">Verbundene Clients</button>
  </div>
  <div id="verbundene-clients-popup" class="popup-overlay" aria-hidden="true">
    <div class="popup-inhalt" role="dialog" aria-modal="true" aria-labelledby="verbundene-clients-titel">
      <div class="popup-kopfzeile">
        <span id="verbundene-clients-titel">Verbundene Clients</span>
        <button id="verbundene-clients-schliessen" class="popup-schliessen" type="button" aria-label="Schließen">✕</button>
      </div>
      <ol id="verbundene-clients-liste" class="client-liste"></ol>
    </div>
  </div>
  <div class="download-panel">
    <a
      class="download-link"
      href="https://github.com/DO1FFE/meshcore-network-monitor/releases/download/client-exe-latest/meshcore_companion_client.exe"
      target="_blank"
      rel="noopener noreferrer"
      aria-label="Client für Windows downloaden"
    >
      <span class="download-icon" aria-hidden="true">⬇</span>
      <span>Client für Windows downloaden</span>
    </a>
  </div>
  <footer class=\"fusszeile\">Copyright 2026 by Erik Schauer, do1ffe@darc.de</footer>
  <script src=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.js\"></script>
  <script>
    const karte = L.map('karte').setView([51.0, 10.0], 6);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom: 19 }).addTo(karte);
    const markerEbene = L.layerGroup().addTo(karte);
    const linienEbene = L.layerGroup().addTo(karte);
    const gesamtRepeaterElement = document.getElementById('gesamt-repeater');
    const sichtbareRepeaterElement = document.getElementById('sichtbare-repeater');
    const letztesDatenpaketElement = document.getElementById('letztes-datenpaket');
    const aktiverFilterElement = document.getElementById('aktiver-filter');
    const zeitfilterAuswahlElement = document.getElementById('zeitfilter-auswahl');
    const anzahlVerbundeneClientsElement = document.getElementById('anzahl-verbundene-clients');
    const verbundeneClientsButtonElement = document.getElementById('verbundene-clients-button');
    const verbundeneClientsPopupElement = document.getElementById('verbundene-clients-popup');
    const verbundeneClientsSchliessenElement = document.getElementById('verbundene-clients-schliessen');
    const verbundeneClientsListeElement = document.getElementById('verbundene-clients-liste');
    const markerKoordinaten = [];
    const filterBeschriftungen = {
      '1': 'letzte 1 Stunde',
      '3': 'letzte 3 Stunden',
      '6': 'letzte 6 Stunden',
      '12': 'letzte 12 Stunden',
      '24': 'letzte 24 Stunden',
      '168': 'letzte 7 Tage',
      'all': 'ALLE'
    };

    function mapDataUrl() {
      const wert = zeitfilterAuswahlElement.value;
      if (wert === 'all') {
        return '/api/map-data?max_age=all';
      }
      return `/api/map-data?max_age_hours=${encodeURIComponent(wert)}`;
    }

    function aktualisiereSichtbareRepeater() {
      const grenzen = karte.getBounds();
      const anzahlSichtbar = markerKoordinaten.filter(koordinaten => grenzen.contains(koordinaten)).length;
      sichtbareRepeaterElement.textContent = String(anzahlSichtbar);
    }

    async function verbundeneClientsAktualisieren() {
      const antwort = await fetch('/api/connected-clients');
      const daten = await antwort.json();
      const clients = Array.isArray(daten.clients) ? daten.clients : [];
      anzahlVerbundeneClientsElement.textContent = String(clients.length);
      verbundeneClientsListeElement.innerHTML = "";
      for (const name of clients) {
        const eintrag = document.createElement('li');
        eintrag.textContent = String(name);
        verbundeneClientsListeElement.appendChild(eintrag);
      }
      if (!clients.length) {
        const eintrag = document.createElement('li');
        eintrag.textContent = 'Keine verbundenen Clients';
        verbundeneClientsListeElement.appendChild(eintrag);
      }
    }

    async function aktualisieren() {
      const antwort = await fetch(mapDataUrl());
      const daten = await antwort.json();
      gesamtRepeaterElement.textContent = String(Array.isArray(daten.nodes) ? daten.nodes.length : 0);
      aktiverFilterElement.textContent = filterBeschriftungen[zeitfilterAuswahlElement.value] || 'unbekannt';
      markerEbene.clearLayers();
      linienEbene.clearLayers();
      markerKoordinaten.length = 0;

      const punkte = new Map();
      for (const n of daten.nodes) {
        if (typeof n.latitude !== 'number' || typeof n.longitude !== 'number') continue;
        punkte.set(n.id, [n.latitude, n.longitude]);
        const prefixAnzeige = Array.isArray(n.prefixes) && n.prefixes.length ? n.prefixes.join(', ') : (n.prefix || '-');
        const roherPrefix = n.prefix || n.id || '--';
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
        markerKoordinaten.push(L.latLng(n.latitude, n.longitude));
      }

      for (const e of daten.edges) {
        if (!punkte.has(e.von_id) || !punkte.has(e.nach_id)) continue;
        L.polyline([punkte.get(e.von_id), punkte.get(e.nach_id)], { color: '#2457ff', weight: 3, opacity: 0.7 })
          .bindPopup(`Verbindung: ${e.von_id} → ${e.nach_id}`)
          .addTo(linienEbene);
      }

      letztesDatenpaketElement.textContent = daten.last_packet_received || '-';
      await verbundeneClientsAktualisieren();
      aktualisiereSichtbareRepeater();
    }

    function popupSchliessen() {
      verbundeneClientsPopupElement.classList.remove('sichtbar');
      verbundeneClientsPopupElement.setAttribute('aria-hidden', 'true');
    }

    function popupOeffnen() {
      verbundeneClientsPopupElement.classList.add('sichtbar');
      verbundeneClientsPopupElement.setAttribute('aria-hidden', 'false');
    }

    verbundeneClientsButtonElement.addEventListener('click', popupOeffnen);
    verbundeneClientsSchliessenElement.addEventListener('click', popupSchliessen);
    verbundeneClientsPopupElement.addEventListener('click', event => {
      if (event.target === verbundeneClientsPopupElement) {
        popupSchliessen();
      }
    });

    karte.on('moveend zoomend resize', aktualisiereSichtbareRepeater);
    zeitfilterAuswahlElement.addEventListener('change', () => {
      aktualisieren();
    });
    aktualisieren();
    setInterval(aktualisieren, 5000);
  </script>
</body>
</html>
"""

HTML_ADMIN = """<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>MeshCore Administration</title>
  <style>
    body {
      margin: 0;
      font-family: sans-serif;
      background: #f8fafc;
      color: #0f172a;
    }
    main {
      max-width: 560px;
      margin: 0 auto;
      padding: 24px 16px 40px;
    }
    h1 {
      margin-top: 0;
      margin-bottom: 8px;
    }
    .beschreibung {
      margin-top: 0;
      margin-bottom: 20px;
      color: #334155;
    }
    .kartenlink {
      display: inline-block;
      margin-bottom: 18px;
      text-decoration: none;
      color: #1d4ed8;
      font-weight: 600;
    }
    .aktion {
      background: #ffffff;
      border: 1px solid #cbd5e1;
      border-radius: 8px;
      padding: 12px;
      margin-bottom: 12px;
    }
    .aktion h2 {
      margin: 0 0 6px 0;
      font-size: 1.1rem;
    }
    .aktion p {
      margin: 0 0 10px 0;
      color: #334155;
    }
    .button {
      border: 1px solid #cbd5e1;
      border-radius: 6px;
      background: #f1f5f9;
      color: #0f172a;
      padding: 8px 12px;
      font-size: 0.95rem;
      font-weight: 600;
      cursor: pointer;
    }
    .status {
      margin-bottom: 16px;
      padding: 10px 12px;
      border-radius: 6px;
      border: 1px solid #bfdbfe;
      background: #eff6ff;
      color: #1e3a8a;
    }
  </style>
</head>
<body>
  <main>
    <h1>Administration</h1>
    <p class="beschreibung">Hier können Verwaltungsaktionen ohne Client-Anpassungen ausgelöst werden.</p>
    <a class="kartenlink" href="/">Zurück zur Live-Karte</a>
    __STATUS_HINWEIS__
    <section class="aktion">
      <h2>Prefixe zurücksetzen</h2>
      <p>Setzt die Datei der unbenutzten Prefixe zurück.</p>
      <form method="post" action="/admin/reset-prefixes">
        <button class="button" type="submit">Prefixe löschen</button>
      </form>
    </section>
    <section class="aktion">
      <h2>Restliche Datenbank löschen</h2>
      <p>Löscht alle gespeicherten ADVERT-/PATH-Daten inklusive der Einträge aus der /double-Ansicht.</p>
      <form method="post" action="/admin/clear-database">
        <button class="button" type="submit">Restliche Datenbank löschen</button>
      </form>
    </section>
  </main>
</body>
</html>
"""


def zeitstempel_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def zeitstempel_nach_utc(zeit_text: str | None) -> datetime | None:
    if not zeit_text or not isinstance(zeit_text, str):
        return None
    try:
        zeitpunkt = datetime.fromisoformat(zeit_text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if zeitpunkt.tzinfo is None:
        return zeitpunkt.replace(tzinfo=timezone.utc)
    return zeitpunkt.astimezone(timezone.utc)


def max_age_filter_aus_parametern(parameter: dict[str, list[str]]) -> tuple[int | None, str | None]:
    max_age_wert = parameter.get("max_age", [None])[0]
    max_age_stunden_wert = parameter.get("max_age_hours", [None])[0]
    if max_age_wert is not None and max_age_stunden_wert is not None:
        raise ValueError("Bitte nur einen der Parameter max_age oder max_age_hours angeben")

    if max_age_wert is not None:
        if max_age_wert == "all":
            return None, "all"
        raise ValueError(f"Ungültiger Wert für max_age: {max_age_wert!r}. Erlaubt: {ERLAUBTE_MAX_AGE_WERTE_TEXT}")

    if max_age_stunden_wert is None:
        return None, "all"

    if max_age_stunden_wert == "all":
        return None, "all"

    if not max_age_stunden_wert.isdigit():
        raise ValueError(
            f"Ungültiger Wert für max_age_hours: {max_age_stunden_wert!r}. Erlaubt: {ERLAUBTE_MAX_AGE_WERTE_TEXT}"
        )

    max_age_stunden = int(max_age_stunden_wert)
    if max_age_stunden not in ERLAUBTE_MAX_AGE_STUNDEN:
        raise ValueError(
            f"Ungültiger Wert für max_age_hours: {max_age_stunden_wert!r}. Erlaubt: {ERLAUBTE_MAX_AGE_WERTE_TEXT}"
        )
    return max_age_stunden, None


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


def setze_unbenutzte_prefixe_zurueck(dateipfad: Path) -> None:
    initialisiere_unbenutzte_prefixe(dateipfad)
    lock_pfad = _lock_pfad_fuer(dateipfad)
    with lock_pfad.open("a+", encoding="utf-8") as lock_datei:
        fcntl.flock(lock_datei.fileno(), fcntl.LOCK_EX)
        alle_prefixe = [f"{wert:02x}" for wert in range(256)]
        _schreibe_prefixdatei_atomar(dateipfad, alle_prefixe)


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


def baue_doppelte_prefix_listeneintraege(
    doppelte_prefixe: list[dict[str, int | str]],
    unbenutzte_prefixe: list[str],
) -> list[tuple[str, str]]:
    eintraege_nach_prefix: dict[str, str] = {f"{wert:02x}": "1" for wert in range(256)}
    mehrfach_vergeben: set[str] = set()
    for eintrag in doppelte_prefixe:
        prefix = str(eintrag["prefix"])
        eintraege_nach_prefix[prefix] = str(eintrag["anzahl"])
        mehrfach_vergeben.add(prefix)

    for prefix in unbenutzte_prefixe:
        if prefix not in mehrfach_vergeben:
            eintraege_nach_prefix[prefix] = "*** BISHER UNBENUTZT ***"

    return sorted(eintraege_nach_prefix.items(), key=lambda eintrag: eintrag[0])


def baue_doppelte_prefix_hinweis(doppelte_prefixe: list[dict[str, int | str]]) -> str:
    if doppelte_prefixe:
        return ""
    return "<p><strong>Hinweis:</strong> Aktuell wurden keine doppelten Prefixe erkannt.</p>"


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
    """SQLite-Zugriffsschicht für ADVERT/PATH-Daten.

    Abgrenzung der Löschoperationen:
    - Prefix-Daten: Präfix-Aliaszuordnungen (`repeater_aliases`) sowie die
      prefixbezogenen Spaltenwerte in Events (`adverts.prefix`,
      `paths.source_prefix`) plus die Datei mit unbenutzten Prefixen.
    - Restliche Daten: verbleibende Nutzdaten ohne Prefix-Fokus
      (`adverts`, `paths`, `repeaters`).
    """

    def __init__(self, pfad: Path, unbenutzte_prefix_datei: Path):
        self.pfad = pfad
        self.unbenutzte_prefix_datei = unbenutzte_prefix_datei
        initialisiere_unbenutzte_prefixe(self.unbenutzte_prefix_datei)
        self.verbindung = sqlite3.connect(pfad, check_same_thread=False)
        self.verbindung.row_factory = sqlite3.Row
        self._sperre = threading.Lock()
        self._initialisieren()

    def _initialisieren(self) -> None:
        with self._sperre:
            self.verbindung.execute("PRAGMA foreign_keys = ON")
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
            self._loesche_veraltete_daten_gesperrt()

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

    def _loesche_veraltete_daten_gesperrt(self) -> None:
        zeitgrenze = (datetime.now(timezone.utc) - AUFBEWAHRUNGSDAUER).isoformat()
        self.verbindung.execute("DELETE FROM adverts WHERE received_at < ?", (zeitgrenze,))
        self.verbindung.execute("DELETE FROM paths WHERE received_at < ?", (zeitgrenze,))
        self.verbindung.execute("DELETE FROM repeaters WHERE last_seen IS NULL OR last_seen < ?", (zeitgrenze,))
        self.verbindung.execute(
            """
            DELETE FROM repeater_aliases
            WHERE repeater_id NOT IN (SELECT id FROM repeaters)
            """
        )
        self.verbindung.commit()

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
        normalisierter_name = name.strip() if isinstance(name, str) else None
        if normalisierter_name == "":
            normalisierter_name = None
        zu_speichernder_public_key = normalisierter_public_key
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

        if repeater_id is None and normalisierter_name:
            kandidaten_nach_name = list(
                self.verbindung.execute(
                    """
                    SELECT id
                    FROM repeaters
                    WHERE LOWER(name) = LOWER(?)
                    """,
                    (normalisierter_name,),
                )
            )
            if len(kandidaten_nach_name) == 1:
                repeater_id = kandidaten_nach_name[0]["id"]

        if repeater_id is None:
            cursor = self.verbindung.execute(
                """
                INSERT INTO repeaters (name, public_key, latitude, longitude, last_seen)
                VALUES (?, ?, ?, ?, ?)
                """,
                (name, zu_speichernder_public_key, latitude, longitude, zeit),
            )
            repeater_id = cursor.lastrowid
        else:
            self.verbindung.execute(
                """
                UPDATE repeaters
                SET name = COALESCE(?, name),
                    public_key = CASE WHEN ? IS NOT NULL THEN ? ELSE public_key END,
                    latitude = COALESCE(?, latitude),
                    longitude = COALESCE(?, longitude),
                    last_seen = ?
                WHERE id = ?
                """,
                (
                    name,
                    zu_speichernder_public_key,
                    zu_speichernder_public_key,
                    latitude,
                    longitude,
                    zeit,
                    repeater_id,
                ),
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
            self._loesche_veraltete_daten_gesperrt()
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

    def map_daten(self, max_age_stunden: int | None = None) -> dict[str, Any]:
        utc_jetzt = datetime.now(timezone.utc)
        zeitgrenze_dt: datetime | None = None
        zeitgrenze: str | None = None
        if max_age_stunden is not None:
            zeitgrenze_dt = utc_jetzt - timedelta(hours=max_age_stunden)
            zeitgrenze = zeitgrenze_dt.isoformat()

        with self._sperre:
            self._loesche_veraltete_daten_gesperrt()
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
                """,
            ):
                zeitpunkt_last_seen = zeitstempel_nach_utc(zeile["last_seen"])
                if zeitgrenze_dt is not None:
                    if zeitpunkt_last_seen is None or zeitpunkt_last_seen < zeitgrenze_dt:
                        continue
                prefixes = [prefix for prefix in (zeile["prefixes"] or "").split(",") if prefix]
                aktueller_prefix = prefix_aus_public_key(zeile["public_key"])
                nodes.append(
                    {
                        "id": zeile["id"],
                        "prefix": aktueller_prefix,
                        "prefixes": prefixes,
                        "name": zeile["name"],
                        "public_key": zeile["public_key"],
                        "latitude": zeile["latitude"],
                        "longitude": zeile["longitude"],
                        "last_seen": zeitpunkt_last_seen.isoformat() if zeitpunkt_last_seen else zeile["last_seen"],
                    }
                )

            erlaubte_repeater_ids = {knoten["id"] for knoten in nodes}

            alias_map: dict[str, list[int]] = {}
            for zeile in self.verbindung.execute("SELECT repeater_id, prefix FROM repeater_aliases"):
                if zeile["repeater_id"] not in erlaubte_repeater_ids:
                    continue
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

            pfad_abfrage = "SELECT source_prefix, path FROM paths"
            pfad_parameter: tuple[str, ...] = ()
            if zeitgrenze:
                pfad_abfrage += " WHERE received_at >= ?"
                pfad_parameter = (zeitgrenze,)
            for zeile in self.verbindung.execute(pfad_abfrage, pfad_parameter):
                segmente = pfadsegmente(zeile["path"])
                start_id = None
                if zeile["source_prefix"]:
                    start_id = waehle_kandidat(zeile["source_prefix"], None)
                ids = aufgeloeste_ids(segmente, start_id)
                for a, b in zip(ids, ids[1:]):
                    kante_hinzufuegen(a, b)

            advert_abfrage = "SELECT repeater_id, prefix, path FROM adverts WHERE path IS NOT NULL"
            advert_parameter: tuple[str, ...] = ()
            if zeitgrenze:
                advert_abfrage += " AND received_at >= ?"
                advert_parameter = (zeitgrenze,)
            for zeile in self.verbindung.execute(advert_abfrage, advert_parameter):
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

            letztes_advert_abfrage = "SELECT MAX(received_at) FROM adverts"
            letztes_path_abfrage = "SELECT MAX(received_at) FROM paths"
            letzte_parameter: tuple[str, ...] = ()
            if zeitgrenze:
                letztes_advert_abfrage += " WHERE received_at >= ?"
                letztes_path_abfrage += " WHERE received_at >= ?"
                letzte_parameter = (zeitgrenze,)
            letztes_advert = self.verbindung.execute(letztes_advert_abfrage, letzte_parameter).fetchone()[0]
            letztes_path = self.verbindung.execute(letztes_path_abfrage, letzte_parameter).fetchone()[0]
            letztes_datenpaket = max([zeit for zeit in (letztes_advert, letztes_path) if zeit], default=None)

        return {
            "nodes": nodes,
            "edges": [{"von_id": a, "nach_id": b} for a, b in sorted(edges)],
            "last_packet_received": letztes_datenpaket,
            "applied_filter_hours": max_age_stunden,
        }

    def doppelte_prefixe(self) -> list[dict[str, int | str]]:
        with self._sperre:
            self._loesche_veraltete_daten_gesperrt()
            zeilen = self.verbindung.execute(
                """
                SELECT prefix, COUNT(DISTINCT repeater_id) AS anzahl
                FROM repeater_aliases
                WHERE prefix GLOB '[0-9a-f][0-9a-f]'
                GROUP BY prefix
                HAVING COUNT(DISTINCT repeater_id) > 1
                ORDER BY prefix ASC
                """
            )
            return [{"prefix": zeile["prefix"], "anzahl": zeile["anzahl"]} for zeile in zeilen]

    def loesche_prefix_daten(self) -> None:
        """Löscht ausschließlich prefixbezogene Daten und setzt die Prefix-Datei zurück."""
        with self._sperre:
            self.verbindung.execute("DELETE FROM repeater_aliases")
            self.verbindung.execute("UPDATE adverts SET prefix = NULL")
            self.verbindung.execute("UPDATE paths SET source_prefix = NULL")
            setze_unbenutzte_prefixe_zurueck(self.unbenutzte_prefix_datei)
            self.verbindung.commit()

    def loesche_restliche_daten(self) -> None:
        """Löscht verbleibende Nutzdaten inklusive `/double`-relevanter Zuordnungen."""
        with self._sperre:
            self.verbindung.execute("DELETE FROM adverts")
            self.verbindung.execute("DELETE FROM paths")
            self.verbindung.execute("DELETE FROM repeater_aliases")
            self.verbindung.execute("DELETE FROM repeaters")
            setze_unbenutzte_prefixe_zurueck(self.unbenutzte_prefix_datei)
            self.verbindung.commit()

    def loesche_restliche_datenbank(self) -> None:
        """Abwärtskompatibler Alias für bestehende Aufrufer."""
        self.loesche_restliche_daten()


class Handler(BaseHTTPRequestHandler):
    datenbank: Datenbank
    unbenutzte_prefix_datei: Path
    verbundene_clients: dict[str, datetime] = {}
    verbundene_clients_lock = threading.Lock()

    @classmethod
    def _bereinige_und_liste_verbundene_clients(cls, jetzt: datetime | None = None) -> list[str]:
        zeitpunkt = jetzt or datetime.now(timezone.utc)
        grenze = zeitpunkt - CLIENT_TIMEOUT
        with cls.verbundene_clients_lock:
            abgelaufene = [name for name, last_seen in cls.verbundene_clients.items() if last_seen < grenze]
            for name in abgelaufene:
                cls.verbundene_clients.pop(name, None)
            return sorted(cls.verbundene_clients.keys())

    @classmethod
    def client_aktivitaet_markieren(cls, client_name: str, jetzt: datetime | None = None) -> None:
        name = (client_name or "").strip()
        if not name:
            return
        zeitpunkt = jetzt or datetime.now(timezone.utc)
        with cls.verbundene_clients_lock:
            cls.verbundene_clients[name] = zeitpunkt
        cls._bereinige_und_liste_verbundene_clients(zeitpunkt)

    def _json_antwort(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
        roh = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(roh)))
        self.end_headers()
        self.wfile.write(roh)

    def _html_antwort(self, status: HTTPStatus, html_text: str) -> None:
        roh = html_text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(roh)))
        self.end_headers()
        self.wfile.write(roh)

    def _admin_html(self, status_text: str | None = None) -> str:
        if not status_text:
            status_hinweis = ""
        else:
            escaped_status_text = html.escape(status_text, quote=True)
            status_hinweis = f"<p class=\"status\">{escaped_status_text}</p>"
        return HTML_ADMIN.replace("__STATUS_HINWEIS__", status_hinweis)

    def do_GET(self) -> None:  # noqa: N802
        aufgeteilt = urlparse(self.path)
        pfad = aufgeteilt.path
        parameter = parse_qs(aufgeteilt.query)
        if pfad == "/":
            self._html_antwort(HTTPStatus.OK, HTML_KARTE)
            return

        if pfad == "/admin":
            status_text = parameter.get("status", [None])[0]
            self._html_antwort(HTTPStatus.OK, self._admin_html(status_text))
            return

        if pfad == "/api/map-data":
            try:
                max_age_stunden, max_age_schalter = max_age_filter_aus_parametern(parameter)
            except ValueError as exc:
                self._json_antwort(HTTPStatus.BAD_REQUEST, {"fehler": str(exc)})
                return

            antwort = self.datenbank.map_daten(max_age_stunden=max_age_stunden)
            if max_age_schalter == "all":
                antwort["applied_filter_hours"] = "all"
            self._json_antwort(HTTPStatus.OK, antwort)
            return

        if pfad == "/api/unused-prefixes":
            self._json_antwort(HTTPStatus.OK, {"prefixes": lese_unbenutzte_prefixe(self.unbenutzte_prefix_datei)})
            return

        if pfad == "/api/connected-clients":
            clients = self._bereinige_und_liste_verbundene_clients()
            self._json_antwort(HTTPStatus.OK, {"clients": clients, "count": len(clients)})
            return

        if pfad == "/double":
            doppelte_prefixe = self.datenbank.doppelte_prefixe()
            unbenutzte_prefixe = lese_unbenutzte_prefixe(self.unbenutzte_prefix_datei)
            listeneintraege = baue_doppelte_prefix_listeneintraege(doppelte_prefixe, unbenutzte_prefixe)
            hinweis = baue_doppelte_prefix_hinweis(doppelte_prefixe)
            listenpunkte = "".join(
                f"<li><code>{prefix}</code>: {anzeige}</li>" for prefix, anzeige in listeneintraege
            )
            if not listenpunkte:
                listenpunkte = "<li>Keine mehrfach vergebenen 1-Byte Prefixe gefunden.</li>"
            roh = (
                "<!doctype html><html lang='de'><head><meta charset='utf-8'><title>Doppelte Prefixe</title>"
                "<style>body{font-family:sans-serif;margin:24px}code{font-family:monospace}</style></head><body>"
                "<h1>Mehrfach vergebene 1-Byte Prefixe</h1>"
                "<p>Sortierung: Hex-Werte von 00 bis ff.</p>"
                f"{hinweis}<ul>{listenpunkte}</ul></body></html>"
            ).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(roh)))
            self.end_headers()
            self.wfile.write(roh)
            return

        self._json_antwort(HTTPStatus.NOT_FOUND, {"fehler": "nicht gefunden"})

    def do_POST(self) -> None:  # noqa: N802
        pfad = urlparse(self.path).path
        if pfad == "/admin/reset-prefixes":
            self.datenbank.loesche_prefix_daten()
            self._html_antwort(HTTPStatus.OK, self._admin_html("Prefix-Datei wurde erfolgreich zurückgesetzt."))
            return

        if pfad == "/admin/clear-database":
            self.datenbank.loesche_restliche_daten()
            self._html_antwort(
                HTTPStatus.OK,
                self._admin_html("Die restliche Datenbank inklusive /double-Daten wurde erfolgreich gelöscht."),
            )
            return

        if pfad != "/api/events":
            self._json_antwort(HTTPStatus.NOT_FOUND, {"fehler": "nicht gefunden"})
            return

        try:
            laenge = int(self.headers.get("Content-Length", "0"))
        except (TypeError, ValueError):
            self._json_antwort(HTTPStatus.BAD_REQUEST, {"fehler": "Ungültiger Content-Length-Header"})
            return

        if laenge < 0:
            self._json_antwort(HTTPStatus.BAD_REQUEST, {"fehler": "Ungültiger Content-Length-Header"})
            return

        if laenge > MAXIMALE_POST_NUTZLAST_BYTES:
            self._json_antwort(
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                {"fehler": f"Payload zu groß (maximal {MAXIMALE_POST_NUTZLAST_BYTES} Bytes)"},
            )
            return

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

        client_name = payload.get("client_name")
        if isinstance(client_name, str):
            self.client_aktivitaet_markieren(client_name)

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
