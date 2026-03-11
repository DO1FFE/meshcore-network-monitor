# MeshCore Companion Client

Dieses Repository enthält ein CLI-Skript für die Verbindung mit einem MeshCore-Companion-Knoten über **serielle COM-Schnittstelle** oder **BLE-Scan**.

## Voraussetzungen

- Python 3.10+
- Paket `meshcore` (PyPI)
- Für BLE-Modus zusätzlich: `bleak`

Beispielinstallation:

```bash
pip install meshcore bleak
```

## Startbeispiele

### 1) Serielle Verbindung (Windows COM-Port)

```bash
python scripts/meshcore_companion_client.py --com-port COM3
```

Optional mit expliziter Baudrate und PIN:

```bash
python scripts/meshcore_companion_client.py --com-port COM3 --baudrate 115200 --pin 123456
```

### 2) BLE-Scan mit interaktiver Auswahl

```bash
python scripts/meshcore_companion_client.py --ble-scan
```

Ablauf im BLE-Modus:

1. Das Skript scannt nach MeshCore-Geräten.
2. Gefundene Geräte werden nummeriert angezeigt.
3. Ein Gerät wird über Eingabe der Gerätenummer ausgewählt.
4. PIN wird abgefragt (falls nicht per `--pin` übergeben).

## Konfiguration per Datei

Das Skript unterstützt eine optionale JSON-Konfigurationsdatei (Standardpfad: `meshcore_client_config.json`).
CLI-Argumente überschreiben Werte aus der Konfigurationsdatei.

Beispiel:

```bash
python scripts/meshcore_companion_client.py --config meshcore_client_config.example.json
```

Standardverhalten ohne expliziten Modus:

- **BLE-Scan ist standardmäßig aktiv**.
- Für seriellen Betrieb kann `com_port` in der Konfiguration gesetzt oder `--com-port` übergeben werden.

Beispieldatei: `meshcore_client_config.example.json`

## Erwartete Ausgabe

Nach erfolgreicher Verbindung und Authentifizierung (PIN-Login) wird z. B. ausgegeben:

```text
=== Geräteinformationen ===
Name      : MeshCore-Knoten-01
Akkustand : 87%
==========================
```

Anschließend startet der kontinuierliche RX-Log-Modus mit JSON-Zeilen auf der Konsole (inkl. dekodierter Felder, falls vorhanden).

## Persistierung von REPEATER-ADVERT-Daten

Empfangene RX-Logs werden geprüft. Wenn ein ADVERT vom Typ **REPEATER** erkannt wird, wird ein strukturierter Eintrag als JSON-Line gespeichert.

- Standardpfad: `data/repeater_adverts.jsonl`
- Anpassbar über: `--ausgabe-datei`

Gespeicherte Felder umfassen u. a.:

- Name (`adv_name`)
- Public Key (`adv_key`)
- Koordinaten (`adv_lat`, `adv_lon`)
- ADVERT-Metadaten (Flags, Timestamp, Signatur)
- Signalwerte (RSSI, SNR)
- Weitere verfügbare Roh-/Zusatzfelder

## Fehlerbehandlung

Das Skript behandelt robuste Fehlerszenarien mit klaren Fehlermeldungen:

- Timeouts beim Scan, Verbindungsaufbau und Login
- Verbindungsabbrüche/fehlende Antworten
- Ungültige PIN (`LOGIN_FAILED`)
- Nicht verfügbare BLE-Umgebung (fehlendes `bleak`)
- Nicht erreichbare COM-Schnittstelle
