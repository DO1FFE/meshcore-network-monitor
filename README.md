# MeshCore Companion Client + ADVERT/PATH Server

Dieses Repository enthält:

- einen **Client** (`scripts/meshcore_companion_client.py`), der RX-Logs liest und **ADVERT/PATH** an einen Server überträgt
- einen **Server** (`scripts/meshcore_advert_server.py`), der ADVERT/PATH in SQLite speichert und als Live-Karte darstellt

## Voraussetzungen

- Python 3.10+
- Paket `meshcore` gemäß offizieller PyPI-Dokumentation: https://pypi.org/project/meshcore/
- Für BLE-Modus zusätzlich: `bleak`

Beispielinstallation:

```bash
pip install meshcore bleak
```

## Client starten

### Serielle Verbindung

```bash
python scripts/meshcore_companion_client.py --com-port COM3 --server-url https://mesh.do1ffe.de
```

### BLE-Scan

```bash
python scripts/meshcore_companion_client.py --ble-scan --server-url https://mesh.do1ffe.de
```

Optionen:

- `--ausgabe-datei`: lokale JSONL-Persistierung von ADVERTs
- `--server-url`: Zielserver für ADVERT/PATH (POST auf `/api/events`, Standard: `https://mesh.do1ffe.de`)
- `--config`: JSON-Konfigurationsdatei (Standard: `meshcore_client_config.json`)

## Server starten

```bash
python scripts/meshcore_advert_server.py --host 0.0.0.0 --port 8023 --db data/meshcore_map.db
```

### Endpunkte

- `POST /api/events` akzeptiert **nur** `payload_typename` = `ADVERT` oder `PATH`
- `GET /api/map-data` liefert Nodes + Edges als JSON
- `GET /` liefert die Live-Karte

## Kartenlogik

- Repeater-Identität basiert auf dem **ersten Byte** des Public Keys (`prefix`, 2 Hexzeichen)
- Marker zeigen:
  - Repeater-Name
  - Public Key
  - Prefix (erstes Byte)
- Verbindungen werden aus `path`-Segmenten (weiterhin je 4 Hexzeichen) als Kanten gezeichnet; zur Zuordnung auf Repeater wird daraus jeweils das 2-hexstellige Prefix (erstes Byte) verwendet

## Datenbank

SQLite-Tabellen:

- `repeaters`: aktueller Stand je Repeater (Name, Key, Position, last_seen)
- `adverts`: rohe ADVERT-Eingänge
- `paths`: rohe PATH-Eingänge

## Tests

```bash
python -m unittest -v
```
