#!/usr/bin/env python3
"""MeshCore-Companion-Client mit COM- und BLE-Modus."""

from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from getpass import getpass
from pathlib import Path
from typing import Any
from urllib import error, request
from urllib.parse import urlsplit, urlunsplit

from meshcore import EventType, MeshCore

try:
    from bleak import BleakScanner
    from bleak.exc import BleakDBusError, BleakDeviceNotFoundError, BleakError
except ImportError:  # BLE ist optional und wird nur für --ble-scan benötigt.
    BleakScanner = None
    BleakError = Exception
    BleakDeviceNotFoundError = Exception
    BleakDBusError = Exception

REPEATER_TYP_NUMMER = 0x02
AUSGABE_PFAD_STANDARD = Path("data/repeater_adverts.jsonl")


class Verbindungsfehler(RuntimeError):
    """Fehlerklasse für Verbindungsprobleme."""


@dataclass(slots=True)
class CliOptionen:
    com_port: str | None
    baudrate: int
    ble_scan: bool
    timeout: float
    ausgabe_pfad: Path
    pin: str | None
    server_url: str | None
    ble_retry_einmal: bool = True


STANDARD_KONFIGURATION = {
    "com_port": None,
    "ble_scan": True,
    "baudrate": 115200,
    "timeout": 10.0,
    "ausgabe_datei": str(AUSGABE_PFAD_STANDARD),
    "pin": None,
    "server_url": "https://mesh.do1ffe.de",
    "ble_retry_einmal": True,
}


async def ble_geraet_interaktiv_auswaehlen(timeout: float) -> Any:
    """Scannt BLE-Geräte und fragt interaktiv nach einer Auswahl."""
    if BleakScanner is None:
        raise Verbindungsfehler(
            "BLE-Scan ist nicht verfügbar: Paket 'bleak' ist nicht installiert."
        )

    print(f"[INFO] Starte BLE-Scan ({timeout:.1f}s) …")
    try:
        geraete = await asyncio.wait_for(BleakScanner.discover(timeout=timeout), timeout=timeout + 2.0)
    except TimeoutError as exc:
        raise Verbindungsfehler("BLE-Scan hat das Zeitlimit überschritten.") from exc
    except Exception as exc:
        raise Verbindungsfehler(f"BLE-Scan fehlgeschlagen: {exc}") from exc

    meshcore_geraete = [g for g in geraete if "meshcore" in (g.name or "").lower()]
    if not meshcore_geraete:
        raise Verbindungsfehler(
            "Keine MeshCore-BLE-Geräte gefunden. Bitte Verfügbarkeit/Adapter prüfen."
        )

    print("\nGefundene MeshCore-Geräte:")
    for index, geraet in enumerate(meshcore_geraete, start=1):
        name = geraet.name or "<ohne Namen>"
        print(f"  [{index}] {name:<24} {geraet.address}")

    while True:
        auswahl = input("Gerätenummer auswählen: ").strip()
        if not auswahl.isdigit():
            print("Ungültige Eingabe. Bitte eine Zahl eingeben.")
            continue
        nummer = int(auswahl)
        if 1 <= nummer <= len(meshcore_geraete):
            return meshcore_geraete[nummer - 1]
        print("Nummer außerhalb der Liste. Bitte erneut versuchen.")


async def meshcore_verbinden(optionen: CliOptionen) -> MeshCore:
    """Stellt je nach Modus eine Verbindung her."""
    try:
        if optionen.com_port:
            print(f"[INFO] Verbinde seriell mit {optionen.com_port} @ {optionen.baudrate} …")
            client = await asyncio.wait_for(
                MeshCore.create_serial(optionen.com_port, baudrate=optionen.baudrate, default_timeout=optionen.timeout),
                timeout=optionen.timeout + 3.0,
            )
        else:
            geraet = await ble_geraet_interaktiv_auswaehlen(optionen.timeout)
            zieladresse = getattr(geraet, "address", None) or str(geraet)
            print(f"[INFO] Verbinde per BLE mit Zieladresse {zieladresse} …")

            hinweis_ursachen = (
                "Mögliche Ursachen: Gerät außer Reichweite, BLE-Adapter exklusiv belegt "
                "oder inkompatibler Verbindungsparameter."
            )
            anzahl_versuche = 2 if optionen.ble_retry_einmal else 1

            async def _ble_verbindungsaufbau() -> MeshCore:
                try:
                    return await MeshCore.create_ble(
                        zieladresse,
                        pin=optionen.pin,
                        default_timeout=optionen.timeout,
                    )
                except TypeError:
                    return await MeshCore.create_ble(
                        address=zieladresse,
                        pin=optionen.pin,
                        default_timeout=optionen.timeout,
                    )

            letzter_fehler: Verbindungsfehler | None = None
            client = None

            urspruenglicher_fehler: Exception | None = None
            for versuch in range(1, anzahl_versuche + 1):
                try:
                    client = await asyncio.wait_for(
                        _ble_verbindungsaufbau(),
                        timeout=optionen.timeout + 5.0,
                    )
                    break
                except TimeoutError as exc:
                    urspruenglicher_fehler = exc
                    letzter_fehler = Verbindungsfehler(
                        "BLE-Verbindung in Timeout gelaufen "
                        f"(Zieladresse={zieladresse}, Timeout={optionen.timeout:.1f}s). "
                        f"{hinweis_ursachen}"
                    )
                except (BleakError, BleakDeviceNotFoundError, BleakDBusError) as exc:
                    urspruenglicher_fehler = exc
                    letzter_fehler = Verbindungsfehler(
                        "BLE-spezifischer Verbindungsfehler "
                        f"(Zieladresse={zieladresse}, Timeout={optionen.timeout:.1f}s): {exc}. "
                        f"{hinweis_ursachen}"
                    )
                except Exception as exc:
                    urspruenglicher_fehler = exc
                    letzter_fehler = Verbindungsfehler(
                        "Allgemeiner Fehler beim BLE-Verbindungsaufbau "
                        f"(Zieladresse={zieladresse}, Timeout={optionen.timeout:.1f}s): {exc}. "
                        f"{hinweis_ursachen}"
                    )

                if versuch < anzahl_versuche:
                    print(
                        "[WARNUNG] Erster BLE-Verbindungsversuch fehlgeschlagen "
                        "– einmaliger Retry in Kürze …"
                    )
                    await asyncio.sleep(1.0)

            if client is None and letzter_fehler is not None:
                raise Verbindungsfehler(
                    "BLE-Verbindung endgültig fehlgeschlagen, auch der einmalige "
                    "Wiederholungsversuch war nicht erfolgreich. "
                    f"{letzter_fehler}"
                ) from urspruenglicher_fehler
            if client is None:
                raise Verbindungsfehler("BLE-Verbindung konnte nicht aufgebaut werden.")
    except Verbindungsfehler:
        raise
    except TimeoutError as exc:
        raise Verbindungsfehler("Verbindungsaufbau hat das Zeitlimit überschritten.") from exc
    except Exception as exc:
        raise Verbindungsfehler(f"Verbindung fehlgeschlagen: {exc}") from exc

    if client is None:
        raise Verbindungsfehler(
            "MeshCore hat die Verbindung abgelehnt oder keine Antwort geliefert."
        )
    return client


async def authentifizieren(client: MeshCore, pin: str) -> None:
    """Authentifiziert gegen den Knoten mit PIN via send_login."""
    self_info = client.self_info or {}
    oeffentlicher_schluessel = self_info.get("public_key")
    if not oeffentlicher_schluessel:
        raise Verbindungsfehler(
            "Authentifizierung nicht möglich: public_key aus self_info fehlt."
        )

    try:
        sende_antwort = await asyncio.wait_for(
            client.commands.send_login(oeffentlicher_schluessel, pin),
            timeout=8.0,
        )
    except TimeoutError as exc:
        raise Verbindungsfehler("Authentifizierung ist in ein Timeout gelaufen.") from exc

    if sende_antwort is None:
        raise Verbindungsfehler("Authentifizierung fehlgeschlagen: Login-Befehl ohne Antwort.")

    if sende_antwort.type == EventType.ERROR:
        payload = sende_antwort.payload if isinstance(sende_antwort.payload, dict) else {}
        if payload.get("code_string") == "ERR_CODE_NOT_FOUND":
            print(
                "[WARNUNG] Login-Befehl wird von diesem Knoten/Firmwarestand nicht "
                "unterstützt (ERR_CODE_NOT_FOUND). Es wird ohne expliziten Login "
                "fortgefahren."
            )
            return
        raise Verbindungsfehler(
            f"Authentifizierung fehlgeschlagen: Login-Befehl wurde abgelehnt ({sende_antwort.payload})."
        )

    if sende_antwort.type != EventType.MSG_SENT:
        raise Verbindungsfehler(
            "Authentifizierung fehlgeschlagen: Unerwartete Antwort auf send_login "
            f"({sende_antwort.type})."
        )

    try:
        antwort = await asyncio.wait_for(
            client.commands.wait_for_events(
                [EventType.LOGIN_SUCCESS, EventType.LOGIN_FAILED, EventType.ERROR],
                timeout=8.0,
            ),
            timeout=9.0,
        )
    except TimeoutError as exc:
        raise Verbindungsfehler(
            "Authentifizierung fehlgeschlagen: Keine Login-Bestätigung erhalten (Timeout)."
        ) from exc

    if antwort is None:
        raise Verbindungsfehler("Authentifizierung fehlgeschlagen: keine Antwort erhalten.")

    if antwort.type == EventType.LOGIN_FAILED:
        raise Verbindungsfehler("Authentifizierung fehlgeschlagen: ungültige PIN.")

    if antwort.type == EventType.ERROR:
        raise Verbindungsfehler(
            f"Authentifizierung fehlgeschlagen: Login-Bestätigung mit Fehler ({antwort.payload})."
        )

    if antwort.type != EventType.LOGIN_SUCCESS:
        raise Verbindungsfehler(f"Unerwartete Antwort bei Login: {antwort.type}")


async def geraeteinformationen_ausgeben(client: MeshCore) -> None:
    """Liest Gerätename und Akkustand aus und formatiert die Ausgabe."""
    self_info = client.self_info or {}
    name = self_info.get("name", "<unbekannt>")

    def _normalisierte_prozentzahl(zahl: float) -> float:
        if 0.0 <= zahl <= 1.0:
            return zahl * 100.0
        if 1.0 < zahl <= 100.0:
            return zahl
        if 1000.0 <= zahl <= 5000.0:
            # Viele Firmwarestände liefern die Batteriespannung in mV (z. B. 4200).
            untergrenze_mv = 3000.0
            obergrenze_mv = 4200.0
            prozent = ((zahl - untergrenze_mv) / (obergrenze_mv - untergrenze_mv)) * 100.0
            return max(0.0, min(100.0, prozent))
        return zahl

    def _zu_prozent_text(wert: Any) -> str | None:
        if wert is None:
            return None
        if isinstance(wert, (int, float)):
            return f"{_normalisierte_prozentzahl(float(wert)):.0f}%"
        if isinstance(wert, str):
            text = wert.strip()
            if not text:
                return None
            if text.endswith("%"):
                return text
            try:
                return f"{_normalisierte_prozentzahl(float(text)):.0f}%"
            except ValueError:
                return f"{text}%"
        return None

    battery_text = None
    try:
        bat_event = await client.commands.get_bat()
    except Exception:
        bat_event = None

    if bat_event is not None and bat_event.type != EventType.ERROR:
        payload = bat_event.payload if isinstance(bat_event.payload, dict) else {}
        for schluessel in ("battery_level", "battery", "percent", "level"):
            battery_text = _zu_prozent_text(payload.get(schluessel))
            if battery_text:
                break

    if not battery_text:
        for schluessel in ("battery_level", "battery", "battery_percent", "percent", "level"):
            battery_text = _zu_prozent_text(self_info.get(schluessel))
            if battery_text:
                break

    if not battery_text:
        battery_text = "<nicht verfügbar>"

    print("\n=== Geräteinformationen ===")
    print(f"Name      : {name}")
    print(f"Akkustand : {battery_text}")
    print("==========================\n")


def ist_advert(log_daten: dict[str, Any]) -> bool:
    """Prüft, ob ein RX-Log-Eintrag ein ADVERT ist."""
    return ermittle_payload_typename(log_daten) == "ADVERT"


def ermittle_payload_typename(log_daten: dict[str, Any]) -> str | None:
    """Liest den Payload-Typ robust aus verschiedenen Feldnamen aus."""
    for schluessel in ("payload_typename", "payloadTypeName", "payload_type"):
        rohwert = log_daten.get(schluessel)
        if rohwert is None:
            continue
        text = str(rohwert).strip()
        if text:
            return text.upper()
    return None


def ist_repeater_advert(log_daten: dict[str, Any]) -> bool:
    """Prüft, ob ein RX-Log-Eintrag ein ADVERT vom Typ REPEATER ist."""
    adv_typ = log_daten.get("adv_type")
    if isinstance(adv_typ, str):
        try:
            adv_typ = int(adv_typ.strip(), 0)
        except ValueError:
            return False

    return ist_advert(log_daten) and adv_typ == REPEATER_TYP_NUMMER


def paket_mehrzeilig_ausgeben(paket: Any, praefix: str = "") -> None:
    """Gibt ein RX-Paket mehrzeilig mit Einrückung aus."""
    if isinstance(paket, dict):
        for schluessel, wert in paket.items():
            if isinstance(wert, (dict, list, tuple)):
                print(f"{praefix}{schluessel}:")
                paket_mehrzeilig_ausgeben(wert, praefix + "  ")
            else:
                print(f"{praefix}{schluessel}: {wert}")
        return

    if isinstance(paket, (list, tuple)):
        for index, wert in enumerate(paket):
            if isinstance(wert, (dict, list, tuple)):
                print(f"{praefix}[{index}]:")
                paket_mehrzeilig_ausgeben(wert, praefix + "  ")
            else:
                print(f"{praefix}[{index}]: {wert}")
        return

    print(f"{praefix}{paket}")


def json_sicherer_wert(wert: Any) -> Any:
    """Normalisiert Werte rekursiv auf JSON-kompatible Datentypen."""
    if isinstance(wert, dict):
        return {str(schluessel): json_sicherer_wert(inhalt) for schluessel, inhalt in wert.items()}

    if isinstance(wert, list):
        return [json_sicherer_wert(eintrag) for eintrag in wert]

    if isinstance(wert, tuple):
        return [json_sicherer_wert(eintrag) for eintrag in wert]

    if isinstance(wert, (bytes, bytearray)):
        return wert.hex()

    if wert is None or isinstance(wert, (str, int, float, bool)):
        return wert

    return str(wert)


def advert_aufbereiten(log_daten: dict[str, Any]) -> dict[str, Any]:
    """Bereitet ADVERT-Felder strukturiert für JSONL auf."""

    def _json_wert_stabilisieren(wert: Any) -> Any:
        if isinstance(wert, bytes):
            return wert.hex()
        if isinstance(wert, dict):
            return {k: _json_wert_stabilisieren(v) for k, v in wert.items()}
        if isinstance(wert, list):
            return [_json_wert_stabilisieren(eintrag) for eintrag in wert]
        return wert

    daten = {
        "zeitstempel_utc": datetime.now(timezone.utc).isoformat(),
        "name": log_daten.get("adv_name"),
        "public_key": log_daten.get("adv_key"),
        "koordinaten": {
            "latitude": log_daten.get("adv_lat"),
            "longitude": log_daten.get("adv_lon"),
        },
        "adv_typ": log_daten.get("adv_type"),
        "adv_timestamp": log_daten.get("adv_timestamp"),
        "adv_flags": log_daten.get("adv_flags"),
        "signature": log_daten.get("signature"),
        "rssi": log_daten.get("rssi"),
        "snr": log_daten.get("snr"),
        "pfad": log_daten.get("path"),
        "weitere_felder": {
            k: _json_wert_stabilisieren(v)
            for k, v in log_daten.items()
            if k
            not in {
                "adv_name",
                "adv_key",
                "adv_lat",
                "adv_lon",
                "adv_type",
                "adv_timestamp",
                "adv_flags",
                "signature",
                "rssi",
                "snr",
                "path",
            }
        },
    }
    return json_sicherer_wert(daten)




def ist_path(log_daten: dict[str, Any]) -> bool:
    """Prüft, ob ein RX-Log-Eintrag ein PATH ist."""
    return ermittle_payload_typename(log_daten) == "PATH"


def extrahiere_path(log_daten: dict[str, Any]) -> Any:
    """Liest PATH-Daten robust aus unterschiedlichen Feldnamen aus."""
    for schluessel in ("path", "PATH"):
        if schluessel in log_daten:
            return log_daten.get(schluessel)
    return None


def soll_an_server_gesendet_werden(log_daten: dict[str, Any]) -> bool:
    """Prüft, ob ein RX-Log-Eintrag gemäß Server-Regel übertragen werden soll."""
    return ist_repeater_advert(log_daten) or ist_path(log_daten)


def _wert_gekuerzt_formatieren(wert: Any, max_laenge: int) -> str:
    """Formatiert einen Wert robust als gekürzten String."""
    if wert is None:
        return "-"

    if isinstance(wert, (list, tuple)):
        text = " -> ".join(str(eintrag) for eintrag in wert)
    else:
        text = str(wert)

    if len(text) <= max_laenge:
        return text
    if max_laenge <= 1:
        return "…"
    return text[: max_laenge - 1] + "…"


def kompakte_server_info(log_daten: dict[str, Any]) -> str:
    """Erzeugt eine kompakte Infozeile zu erfolgreich gesendeten Daten."""
    payload_typ = _wert_gekuerzt_formatieren(ermittle_payload_typename(log_daten), 24)
    schluessel = log_daten.get("adv_key") or log_daten.get("public_key")
    schluessel_text = _wert_gekuerzt_formatieren(schluessel, 20)
    pfad_text = _wert_gekuerzt_formatieren(log_daten.get("path"), 60)

    teile = [f"typ={payload_typ}", f"key={schluessel_text}", f"path={pfad_text}"]
    if log_daten.get("adv_name"):
        teile.append(f"name={_wert_gekuerzt_formatieren(log_daten.get('adv_name'), 24)}")
    return " | ".join(teile)


def event_an_server_senden(server_url: str, log_daten: dict[str, Any]) -> None:
    """Sendet ADVERT/PATH-Ereignisse per HTTP POST an den Server."""
    ziel = server_api_events_url(server_url)
    payload_typename = ermittle_payload_typename(log_daten)
    server_payload = dict(log_daten)
    if payload_typename and "payload_typename" not in server_payload:
        server_payload["payload_typename"] = payload_typename

    path_daten = extrahiere_path(log_daten)
    if path_daten is not None:
        server_payload["path"] = path_daten

    roh = json.dumps(json_sicherer_wert(server_payload), ensure_ascii=False).encode("utf-8")
    req = request.Request(ziel, data=roh, headers={"Content-Type": "application/json"}, method="POST")
    with request.urlopen(req, timeout=5.0) as antwort:
        if antwort.status >= 300:
            raise Verbindungsfehler(
                f"Server meldete HTTP {antwort.status} bei Übertragung an {ziel}."
            )
        print(
            f"[INFO] Serverantwort HTTP {antwort.status} für Event-Typ {payload_typename or "<unbekannt>"}."
        )


def server_beim_start_pruefen(server_url: str) -> None:
    """Prüft beim Programmstart, ob der Server erreichbar ist und POST-Daten verarbeitet."""
    ziel = server_api_events_url(server_url)
    pruef_payload = json.dumps({}, ensure_ascii=False).encode("utf-8")
    print(f"[INFO] Prüfe Server-Verbindung über {ziel} …")
    req = request.Request(
        ziel,
        data=pruef_payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with request.urlopen(req, timeout=5.0) as antwort:
            status = int(getattr(antwort, "status", 0) or 0)
    except error.HTTPError as exc:
        status = int(getattr(exc, "code", 0) or 0)
    except Exception as exc:
        raise Verbindungsfehler(
            f"Serverprüfung fehlgeschlagen: {server_url} ist nicht erreichbar oder nimmt keine Daten an ({exc})."
        ) from exc

    if status not in {200, 201, 202, 400}:
        raise Verbindungsfehler(
            "Serverprüfung fehlgeschlagen: Unerwarteter HTTP-Status "
            f"{status} bei POST auf {ziel}."
        )

    print(f"[INFO] Server erreichbar (HTTP {status}) und POST-Endpunkt antwortet.")


def server_api_events_url(server_url: str) -> str:
    """Normalisiert eine Basis- oder Endpunkt-URL auf den POST-Endpunkt /api/events."""
    teile = urlsplit(server_url.strip())
    if not teile.scheme or not teile.netloc:
        raise Verbindungsfehler(
            "Server-URL ist ungültig. Erwartet wird eine absolute URL wie "
            "https://mesh.do1ffe.de oder https://mesh.do1ffe.de/api/events."
        )

    pfad = (teile.path or "").rstrip("/")
    if pfad.endswith("/api/events"):
        normierter_pfad = pfad
    elif pfad:
        normierter_pfad = pfad + "/api/events"
    else:
        normierter_pfad = "/api/events"

    return urlunsplit((teile.scheme, teile.netloc, normierter_pfad, teile.query, teile.fragment))


def advert_persistieren(pfad: Path, advert_daten: dict[str, Any]) -> None:
    """Speichert einen Datensatz als JSONL-Zeile."""
    pfad.parent.mkdir(parents=True, exist_ok=True)
    with pfad.open("a", encoding="utf-8") as datei:
        datei.write(json.dumps(advert_daten, ensure_ascii=False, default=str) + "\n")


async def rx_log_modus(client: MeshCore, ausgabe_pfad: Path, server_url: str | None = None) -> None:
    """Kontinuierlicher RX-Log-Modus mit Persistierung von REPEATER-ADVERTs."""
    uebertragungs_tasks: set[asyncio.Task[None]] = set()

    async def _event_asynchron_an_server_senden(log_daten: dict[str, Any]) -> None:
        try:
            await asyncio.to_thread(event_an_server_senden, server_url, log_daten)
            print(f"[INFO] An Server übertragen: {kompakte_server_info(log_daten)}")
        except Exception as exc:
            print(f"[WARNUNG] Übertragung an Server fehlgeschlagen: {exc}")

    def _uebertragung_task_registrieren(log_daten: dict[str, Any]) -> None:
        task = asyncio.create_task(_event_asynchron_an_server_senden(log_daten))
        uebertragungs_tasks.add(task)
        task.add_done_callback(uebertragungs_tasks.discard)

    async def bei_rx_log(event) -> None:
        log_daten = event.payload if isinstance(event.payload, dict) else {}

        if server_url and soll_an_server_gesendet_werden(log_daten):
            _uebertragung_task_registrieren(log_daten)

        paket = {
            "zeit": datetime.now(timezone.utc).isoformat(),
            "payload_typ": log_daten.get("payload_typename"),
            "route_typ": log_daten.get("route_typename"),
            "rssi": log_daten.get("rssi"),
            "snr": log_daten.get("snr"),
            "daten": log_daten,
        }
        paket_mehrzeilig_ausgeben(paket)
        print()
        print()

        if ist_advert(log_daten):
            advert = advert_aufbereiten(log_daten)
            advert_persistieren(ausgabe_pfad, advert)
            if ist_repeater_advert(log_daten):
                typtext = "REPEATER-ADVERT"
            else:
                typtext = "ADVERT"
            print(
                f"[INFO] {typtext} gespeichert: "
                f"{advert.get('name') or '<ohne Name>'} / {advert.get('public_key')}"
            )
            print()
            print()

    client.subscribe(EventType.RX_LOG_DATA, bei_rx_log)

    print("[INFO] RX-Log läuft. Mit Strg+C beenden.")
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, asyncio.CancelledError):
        if uebertragungs_tasks:
            await asyncio.gather(*uebertragungs_tasks, return_exceptions=True)
        print("\n[INFO] RX-Log beendet.")


def konfiguration_laden(konfigurations_pfad: Path) -> dict[str, Any]:
    """Lädt optionale Konfiguration aus JSON und kombiniert sie mit Standardwerten."""
    konfiguration = dict(STANDARD_KONFIGURATION)
    if not konfigurations_pfad.exists():
        return konfiguration

    try:
        inhalt = json.loads(konfigurations_pfad.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise Verbindungsfehler(
            f"Konfigurationsdatei ist kein gültiges JSON: {konfigurations_pfad}"
        ) from exc

    if not isinstance(inhalt, dict):
        raise Verbindungsfehler(
            f"Konfigurationsdatei muss ein JSON-Objekt enthalten: {konfigurations_pfad}"
        )

    for schluessel in STANDARD_KONFIGURATION:
        if schluessel in inhalt:
            konfiguration[schluessel] = inhalt[schluessel]
    return konfiguration


def optionen_aus_argumenten_und_konfiguration(
    args: argparse.Namespace, konfiguration: dict[str, Any]
) -> CliOptionen:
    """Priorisiert CLI-Argumente vor Konfiguration und validiert die Startmodi."""
    com_port = args.com_port if args.com_port is not None else konfiguration.get("com_port")
    ble_scan = args.ble_scan if args.ble_scan is not None else konfiguration.get("ble_scan", True)

    if args.com_port is not None and args.ble_scan is None:
        ble_scan = False
    if args.ble_scan is True and args.com_port is None:
        com_port = None

    if com_port and ble_scan:
        raise Verbindungsfehler(
            "Ungültige Konfiguration: --com-port und BLE-Scan dürfen nicht gleichzeitig aktiv sein."
        )

    if not com_port and not ble_scan:
        ble_scan = True

    baudrate = args.baudrate if args.baudrate is not None else konfiguration.get("baudrate", 115200)
    timeout = args.timeout if args.timeout is not None else konfiguration.get("timeout", 10.0)
    ausgabe_datei = args.ausgabe_datei if args.ausgabe_datei is not None else konfiguration.get("ausgabe_datei")
    pin = args.pin if args.pin is not None else konfiguration.get("pin")
    server_url = args.server_url if args.server_url is not None else konfiguration.get("server_url")
    ble_retry_einmal = bool(konfiguration.get("ble_retry_einmal", True))

    return CliOptionen(
        com_port=com_port,
        baudrate=int(baudrate),
        ble_scan=bool(ble_scan),
        timeout=float(timeout),
        ausgabe_pfad=Path(ausgabe_datei),
        pin=pin,
        server_url=server_url,
        ble_retry_einmal=ble_retry_einmal,
    )


def argumente_einlesen(argv: list[str] | None = None) -> CliOptionen:
    """Parst CLI-Argumente und kombiniert sie mit einer optionalen Konfigurationsdatei."""
    parser = argparse.ArgumentParser(
        description="MeshCore Companion Client (COM oder BLE-Scan)"
    )
    parser.add_argument(
        "--com-port",
        help="Serieller COM-Port (z. B. COM3 unter Windows)",
    )
    parser.add_argument(
        "--ble-scan",
        action="store_true",
        default=None,
        help="BLE-Scan starten und Gerät interaktiv auswählen",
    )
    parser.add_argument(
        "--kein-ble-scan",
        dest="ble_scan",
        action="store_false",
        default=None,
        help="BLE-Scan explizit deaktivieren (z. B. bei rein serieller Konfiguration)",
    )
    parser.add_argument("--baudrate", type=int, default=None, help="Baudrate für seriellen Modus")
    parser.add_argument("--timeout", type=float, default=None, help="Timeout in Sekunden")
    parser.add_argument(
        "--ausgabe-datei",
        type=Path,
        default=None,
        help="Pfad zur JSONL-Ausgabedatei für REPEATER-ADVERTs",
    )
    parser.add_argument(
        "--pin",
        default=None,
        help="PIN für Authentifizierung (wenn nicht gesetzt, wird interaktiv abgefragt)",
    )
    parser.add_argument(
        "--server-url",
        default=None,
        help="Basis-URL des ADVERT/PATH-Servers, z. B. https://mesh.do1ffe.de",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("meshcore_client_config.json"),
        help="Pfad zu einer optionalen JSON-Konfigurationsdatei",
    )

    args = parser.parse_args(argv)
    konfiguration = konfiguration_laden(args.config)
    return optionen_aus_argumenten_und_konfiguration(args, konfiguration)


async def async_hauptprogramm() -> int:
    """Asynchroner Programmeinstieg."""
    optionen = argumente_einlesen()

    if optionen.server_url:
        server_beim_start_pruefen(optionen.server_url)

    pin = optionen.pin or getpass("PIN eingeben: ")
    if not pin:
        print("[FEHLER] Es wurde keine PIN angegeben.")
        return 2
    optionen.pin = pin

    client = None
    try:
        client = await meshcore_verbinden(optionen)
        await authentifizieren(client, optionen.pin)
        await geraeteinformationen_ausgeben(client)
        await rx_log_modus(client, optionen.ausgabe_pfad, optionen.server_url)
        return 0
    except Verbindungsfehler as exc:
        print(f"[FEHLER] {exc}")
        return 1
    except Exception as exc:
        print(f"[FEHLER] Unerwarteter Fehler: {exc}")
        return 1
    finally:
        if client is not None:
            try:
                await client.disconnect()
            except Exception:
                pass


def hauptprogramm() -> None:
    """Synchroner CLI-Startpunkt."""
    try:
        raise SystemExit(asyncio.run(async_hauptprogramm()))
    except KeyboardInterrupt:
        print("\n[INFO] Programm durch Benutzer beendet.")
        raise SystemExit(0)


if __name__ == "__main__":
    hauptprogramm()
