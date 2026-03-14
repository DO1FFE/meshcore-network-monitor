import asyncio
import io
import importlib.util
import json
import sys
import tempfile
import types
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


class TestKonfiguration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        meshcore_stub = types.ModuleType("meshcore")

        class _DummyEventType:
            LOGIN_FAILED = "LOGIN_FAILED"
            LOGIN_SUCCESS = "LOGIN_SUCCESS"
            ERROR = "ERROR"
            MSG_SENT = "MSG_SENT"
            RX_LOG_DATA = "RX_LOG_DATA"

        class _DummyMeshCore:
            pass

        meshcore_stub.EventType = _DummyEventType
        meshcore_stub.MeshCore = _DummyMeshCore
        sys.modules["meshcore"] = meshcore_stub

        spec = importlib.util.spec_from_file_location(
            "meshcore_companion_client", "scripts/meshcore_companion_client.py"
        )
        module = importlib.util.module_from_spec(spec)
        sys.modules["meshcore_companion_client"] = module
        assert spec.loader is not None
        spec.loader.exec_module(module)
        cls.modul = module

    def test_standard_ble_scan_aktiv(self):
        optionen = self.modul.argumente_einlesen([])
        self.assertIsNone(optionen.com_port)
        self.assertTrue(optionen.ble_scan)
        self.assertEqual(optionen.server_url, "https://mesh.do1ffe.de")
        self.assertTrue(isinstance(optionen.client_name, str))
        self.assertTrue(len(optionen.client_name) > 0)

    def test_start_header_ausgeben_enthaelt_copyright(self):
        ausgabe = io.StringIO()
        with redirect_stdout(ausgabe):
            self.modul.start_header_ausgeben()

        text = ausgabe.getvalue()
        self.assertIn("MeshCore Companion Client", text)
        self.assertIn("Copyright (c) 2026", text)
        self.assertIn("Erik Schauer", text)
        self.assertIn("do1ffe@darc.de", text)

    def test_konfiguration_com_port_wird_uebernommen(self):
        with tempfile.TemporaryDirectory() as tmp:
            pfad = Path(tmp) / "config.json"
            pfad.write_text(
                json.dumps(
                    {
                        "com_port": "COM7",
                        "ble_scan": False,
                        "baudrate": 9600,
                        "timeout": 3,
                        "ausgabe_datei": "out.jsonl",
                        "server_url": "https://mesh.do1ffe.de",
                        "client_name": "client-a",
                    }
                ),
                encoding="utf-8",
            )
            optionen = self.modul.argumente_einlesen(["--config", str(pfad)])

        self.assertEqual(optionen.com_port, "COM7")
        self.assertFalse(optionen.ble_scan)
        self.assertEqual(optionen.baudrate, 9600)
        self.assertEqual(optionen.timeout, 3.0)
        self.assertEqual(str(optionen.ausgabe_pfad), "out.jsonl")
        self.assertEqual(optionen.server_url, "https://mesh.do1ffe.de")
        self.assertEqual(optionen.client_name, "client-a")

    def test_cli_ueberschreibt_konfiguration(self):
        with tempfile.TemporaryDirectory() as tmp:
            pfad = Path(tmp) / "config.json"
            pfad.write_text(json.dumps({"com_port": "COM7", "ble_scan": False}), encoding="utf-8")
            optionen = self.modul.argumente_einlesen(
                ["--config", str(pfad), "--ble-scan", "--timeout", "5"]
            )

        self.assertIsNone(optionen.com_port)
        self.assertTrue(optionen.ble_scan)
        self.assertEqual(optionen.timeout, 5.0)

    def test_cli_com_port_deaktiviert_ble_aus_konfiguration(self):
        with tempfile.TemporaryDirectory() as tmp:
            pfad = Path(tmp) / "config.json"
            pfad.write_text(json.dumps({"ble_scan": True}), encoding="utf-8")
            optionen = self.modul.argumente_einlesen(
                ["--config", str(pfad), "--com-port", "COM9"]
            )

        self.assertEqual(optionen.com_port, "COM9")
        self.assertFalse(optionen.ble_scan)



    def test_cli_server_url_ueberschreibt_konfiguration(self):
        with tempfile.TemporaryDirectory() as tmp:
            pfad = Path(tmp) / "config.json"
            pfad.write_text(json.dumps({"server_url": "https://alt.example"}), encoding="utf-8")
            optionen = self.modul.argumente_einlesen([
                "--config", str(pfad), "--server-url", "https://mesh.do1ffe.de"
            ])

        self.assertEqual(optionen.server_url, "https://mesh.do1ffe.de")

    def test_cli_client_name_ueberschreibt_konfiguration(self):
        with tempfile.TemporaryDirectory() as tmp:
            pfad = Path(tmp) / "config.json"
            pfad.write_text(json.dumps({"client_name": "alt-client"}), encoding="utf-8")
            optionen = self.modul.argumente_einlesen([
                "--config", str(pfad), "--client-name", "neu-client"
            ])

        self.assertEqual(optionen.client_name, "neu-client")


class TestAdvertSerialisierung(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if "meshcore_companion_client" not in sys.modules:
            TestKonfiguration.setUpClass()
        cls.modul = sys.modules["meshcore_companion_client"]

    def test_json_sicherer_wert_normalisiert_problematische_typen_rekursiv(self):
        klasse_unbekannt = type("KlasseUnbekannt", (), {"__str__": lambda self: "objekt-text"})
        rohwert = {
            "bytes": b"\x00\xff",
            "bytearray": bytearray(b"\x01\x02"),
            "liste": [1, b"\x03"],
            "tuple": (True, b"\x04"),
            "objekt": klasse_unbekannt(),
        }

        normalisiert = self.modul.json_sicherer_wert(rohwert)

        self.assertEqual(normalisiert["bytes"], "00ff")
        self.assertEqual(normalisiert["bytearray"], "0102")
        self.assertEqual(normalisiert["liste"], [1, "03"])
        self.assertEqual(normalisiert["tuple"], [True, "04"])
        self.assertEqual(normalisiert["objekt"], "objekt-text")

    def test_advert_aufbereiten_macht_weitere_felder_json_sicher(self):
        advert = self.modul.advert_aufbereiten(
            {
                "payload_typename": "ADVERT",
                "adv_type": self.modul.REPEATER_TYP_NUMMER,
                "adv_name": "Node",
                "adv_key": "abc",
                "raw_payload": b"\xaa\xbb",
                "nested": {"token": bytearray(b"\x10")},
            }
        )

        self.assertEqual(advert["weitere_felder"]["raw_payload"], "aabb")
        self.assertEqual(advert["weitere_felder"]["nested"]["token"], "10")

    def test_ist_advert_und_ist_repeater_advert_unterscheiden_typen(self):
        advert = {"payload_typename": "ADVERT", "adv_type": self.modul.REPEATER_TYP_NUMMER}
        non_repeater = {"payload_typename": "ADVERT", "adv_type": 1}

        self.assertTrue(self.modul.ist_advert(advert))
        self.assertTrue(self.modul.ist_repeater_advert(advert))
        self.assertTrue(self.modul.ist_advert(non_repeater))
        self.assertFalse(self.modul.ist_repeater_advert(non_repeater))


    def test_ist_repeater_advert_akzeptiert_adv_type_als_string(self):
        self.assertTrue(
            self.modul.ist_repeater_advert(
                {"payload_typename": "ADVERT", "adv_type": str(self.modul.REPEATER_TYP_NUMMER)}
            )
        )

    def test_paket_mehrzeilig_ausgeben_zeigt_parameter_je_zeile(self):
        paket = {
            "zeit": "2026-03-11T00:00:00+00:00",
            "daten": {"payload_typ": "ADVERT", "werte": [1, 2]},
        }

        with patch("builtins.print") as print_mock:
            self.modul.paket_mehrzeilig_ausgeben(paket)

        ausgaben = [aufruf.args[0] for aufruf in print_mock.call_args_list]
        self.assertIn("zeit: 2026-03-11T00:00:00+00:00", ausgaben)
        self.assertIn("daten:", ausgaben)
        self.assertIn("  payload_typ: ADVERT", ausgaben)
        self.assertIn("  werte:", ausgaben)
        self.assertIn("    [0]: 1", ausgaben)
        self.assertIn("    [1]: 2", ausgaben)




    def test_ist_path_erkennt_path_typ(self):
        self.assertTrue(self.modul.ist_path({"payload_typename": "PATH"}))
        self.assertFalse(self.modul.ist_path({"payload_typename": "ADVERT"}))

    def test_ermittle_payload_typename_unterstuetzt_alternative_schluessel(self):
        self.assertEqual(
            self.modul.ermittle_payload_typename({"payloadTypeName": "advert"}),
            "ADVERT",
        )
        self.assertEqual(
            self.modul.ermittle_payload_typename({"payload_type": "path"}),
            "PATH",
        )

    def test_ist_advert_und_ist_path_nutzen_alternative_typfelder(self):
        self.assertTrue(self.modul.ist_advert({"payloadTypeName": "ADVERT"}))
        self.assertTrue(self.modul.ist_path({"payload_type": "PATH"}))

    def test_soll_an_server_gesendet_werden_fuer_repeater_advert_wahr(self):
        self.assertTrue(
            self.modul.soll_an_server_gesendet_werden(
                {
                    "payload_typename": "ADVERT",
                    "adv_type": self.modul.REPEATER_TYP_NUMMER,
                }
            )
        )

    def test_soll_an_server_gesendet_werden_fuer_nicht_repeater_advert_falsch(self):
        self.assertFalse(
            self.modul.soll_an_server_gesendet_werden(
                {
                    "payload_typename": "ADVERT",
                    "adv_type": self.modul.REPEATER_TYP_NUMMER + 1,
                }
            )
        )

    def test_soll_an_server_gesendet_werden_fuer_advert_ohne_adv_type_falsch(self):
        self.assertFalse(
            self.modul.soll_an_server_gesendet_werden(
                {"payload_typename": "ADVERT"}
            )
        )

    def test_soll_an_server_gesendet_werden_fuer_nicht_path_typ_mit_path_falsch(self):
        self.assertFalse(
            self.modul.soll_an_server_gesendet_werden(
                {"payload_typename": "TEXT", "path": ["hop-1"]}
            )
        )

    def test_soll_an_server_gesendet_werden_ohne_advert_und_ohne_path_falsch(self):
        self.assertFalse(
            self.modul.soll_an_server_gesendet_werden(
                {"payload_typename": "TEXT", "ohne_path": True}
            )
        )

    def test_soll_an_server_gesendet_werden_bei_leerem_path_ist_falsch(self):
        self.assertFalse(
            self.modul.soll_an_server_gesendet_werden(
                {"payload_typename": "TEXT", "path": []}
            )
        )

    def test_soll_an_server_gesendet_werden_ignoiert_grosses_path_feld_ohne_path_typ(self):
        self.assertFalse(
            self.modul.soll_an_server_gesendet_werden(
                {"payload_typename": "TEXT", "PATH": "a1b2 c3d4"}
            )
        )

    def test_soll_an_server_gesendet_werden_fuer_path_typ_wahr(self):
        self.assertTrue(
            self.modul.soll_an_server_gesendet_werden(
                {"payload_typename": "PATH", "PATH": "a1b2 c3d4"}
            )
        )

class TestMeshcoreVerbinden(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.modul = sys.modules["meshcore_companion_client"]

    async def test_ble_verbindung_verwendet_adresse_als_positionsargument(self):
        optionen = self.modul.CliOptionen(
            com_port=None,
            baudrate=115200,
            ble_scan=True,
            timeout=5.0,
            ausgabe_pfad=Path("out.jsonl"),
            pin="123456",
            server_url=None,
            client_name="test-client",
        )
        geraet = SimpleNamespace(address="AA:BB:CC:DD:EE:FF")
        erwarteter_client = object()

        with patch.object(self.modul, "ble_geraet_interaktiv_auswaehlen", AsyncMock(return_value=geraet)), patch.object(
            self.modul.MeshCore, "create_ble", AsyncMock(return_value=erwarteter_client), create=True
        ) as create_ble_mock:
            client = await self.modul.meshcore_verbinden(optionen)

        self.assertIs(client, erwarteter_client)
        create_ble_mock.assert_awaited_once_with(
            address="AA:BB:CC:DD:EE:FF",
            pin="123456",
            default_timeout=5.0,
        )

    async def test_ble_fallback_verwendet_address_parameter_bei_typeerror(self):
        optionen = self.modul.CliOptionen(
            com_port=None,
            baudrate=115200,
            ble_scan=True,
            timeout=7.0,
            ausgabe_pfad=Path("out.jsonl"),
            pin=None,
            server_url=None,
            client_name="test-client",
        )
        geraet = SimpleNamespace(address="11:22:33:44:55:66")
        erwarteter_client = object()

        create_ble_mock = AsyncMock(side_effect=[TypeError("bad signature"), erwarteter_client])

        with patch.object(self.modul, "ble_geraet_interaktiv_auswaehlen", AsyncMock(return_value=geraet)), patch.object(
            self.modul.MeshCore, "create_ble", create_ble_mock, create=True
        ):
            client = await self.modul.meshcore_verbinden(optionen)

        self.assertIs(client, erwarteter_client)
        self.assertEqual(create_ble_mock.await_count, 2)
        erster_aufruf = create_ble_mock.await_args_list[0]
        zweiter_aufruf = create_ble_mock.await_args_list[1]

        self.assertEqual(erster_aufruf.args, ())
        self.assertEqual(
            erster_aufruf.kwargs,
            {
                "address": "11:22:33:44:55:66",
                "pin": None,
                "default_timeout": 7.0,
            },
        )
        self.assertEqual(zweiter_aufruf.args, ("11:22:33:44:55:66",))
        self.assertEqual(
            zweiter_aufruf.kwargs,
            {"pin": None, "default_timeout": 7.0},
        )

    async def test_ble_fallback_verwendet_device_parameter_als_dritten_versuch(self):
        optionen = self.modul.CliOptionen(
            com_port=None,
            baudrate=115200,
            ble_scan=True,
            timeout=7.0,
            ausgabe_pfad=Path("out.jsonl"),
            pin="123456",
            server_url=None,
            client_name="test-client",
        )
        geraet = SimpleNamespace(address="12:34:56:78:90:AB")
        erwarteter_client = object()

        create_ble_mock = AsyncMock(
            side_effect=[TypeError("kw-only unsupported"), TypeError("positional unsupported"), erwarteter_client]
        )

        with patch.object(self.modul, "ble_geraet_interaktiv_auswaehlen", AsyncMock(return_value=geraet)), patch.object(
            self.modul.MeshCore, "create_ble", create_ble_mock, create=True
        ):
            client = await self.modul.meshcore_verbinden(optionen)

        self.assertIs(client, erwarteter_client)
        self.assertEqual(create_ble_mock.await_count, 3)
        dritter_aufruf = create_ble_mock.await_args_list[2]
        self.assertEqual(dritter_aufruf.args, ())
        self.assertEqual(
            dritter_aufruf.kwargs,
            {
                "address": "12:34:56:78:90:AB",
                "device": geraet,
                "pin": "123456",
                "default_timeout": 7.0,
            },
        )
    async def test_ble_retry_einmal_erster_versuch_fehlt_zweiter_erfolgreich(self):
        optionen = self.modul.CliOptionen(
            com_port=None,
            baudrate=115200,
            ble_scan=True,
            timeout=4.0,
            ausgabe_pfad=Path("out.jsonl"),
            pin="123456",
            server_url=None,
            client_name="test-client",
            ble_retry_einmal=True,
        )
        geraet = SimpleNamespace(address="AA:00:BB:11:CC:22")
        erwarteter_client = object()
        create_ble_mock = AsyncMock(side_effect=[RuntimeError("temporärer BLE-Fehler"), erwarteter_client])

        with patch.object(self.modul, "ble_geraet_interaktiv_auswaehlen", AsyncMock(return_value=geraet)), patch.object(
            self.modul.MeshCore, "create_ble", create_ble_mock, create=True
        ), patch.object(self.modul.asyncio, "sleep", AsyncMock()) as sleep_mock:
            client = await self.modul.meshcore_verbinden(optionen)

        self.assertIs(client, erwarteter_client)
        self.assertEqual(create_ble_mock.await_count, 2)
        sleep_mock.assert_awaited_once_with(1.0)

    async def test_ble_retry_einmal_beide_versuche_fehlschlag_klare_endmeldung(self):
        optionen = self.modul.CliOptionen(
            com_port=None,
            baudrate=115200,
            ble_scan=True,
            timeout=6.0,
            ausgabe_pfad=Path("out.jsonl"),
            pin="123456",
            server_url=None,
            client_name="test-client",
            ble_retry_einmal=True,
        )
        geraet = SimpleNamespace(address="FF:EE:DD:CC:BB:AA")
        create_ble_mock = AsyncMock(side_effect=[RuntimeError("erster Fehler"), RuntimeError("zweiter Fehler")])

        with patch.object(self.modul, "ble_geraet_interaktiv_auswaehlen", AsyncMock(return_value=geraet)), patch.object(
            self.modul.MeshCore, "create_ble", create_ble_mock, create=True
        ), patch.object(self.modul.asyncio, "sleep", AsyncMock()):
            with self.assertRaises(self.modul.Verbindungsfehler) as context:
                await self.modul.meshcore_verbinden(optionen)

        meldung = str(context.exception)
        self.assertIn("BLE-Verbindung endgültig fehlgeschlagen", meldung)
        self.assertIn("Zieladresse=FF:EE:DD:CC:BB:AA", meldung)
        self.assertIn("Timeout=6.0s", meldung)
        self.assertIn("Mögliche Ursachen", meldung)



class TestAuthentifizieren(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        if "meshcore_companion_client" not in sys.modules:
            TestKonfiguration.setUpClass()
        cls.modul = sys.modules["meshcore_companion_client"]

    async def test_authentifizieren_wartet_auf_login_success_event(self):
        client = SimpleNamespace(
            self_info={"public_key": "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"},
            commands=SimpleNamespace(
                send_login=AsyncMock(return_value=SimpleNamespace(type=self.modul.EventType.MSG_SENT, payload={})),
                wait_for_events=AsyncMock(return_value=SimpleNamespace(type=self.modul.EventType.LOGIN_SUCCESS, payload={})),
            ),
        )

        await self.modul.authentifizieren(client, "123456")

        client.commands.send_login.assert_awaited_once()
        client.commands.wait_for_events.assert_awaited_once()


    async def test_authentifizieren_erlaubt_fallback_bei_err_code_not_found(self):
        client = SimpleNamespace(
            self_info={"public_key": "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"},
            commands=SimpleNamespace(
                send_login=AsyncMock(return_value=SimpleNamespace(
                    type=self.modul.EventType.ERROR,
                    payload={"error_code": 2, "code_string": "ERR_CODE_NOT_FOUND"},
                )),
                wait_for_events=AsyncMock(),
            ),
        )

        await self.modul.authentifizieren(client, "123456")

        client.commands.send_login.assert_awaited_once()
        client.commands.wait_for_events.assert_not_called()
    async def test_authentifizieren_meldet_fehler_wenn_send_login_error_liefert(self):
        client = SimpleNamespace(
            self_info={"public_key": "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"},
            commands=SimpleNamespace(
                send_login=AsyncMock(return_value=SimpleNamespace(type=self.modul.EventType.ERROR, payload={"reason": "foo"})),
                wait_for_events=AsyncMock(),
            ),
        )

        with self.assertRaises(self.modul.Verbindungsfehler) as context:
            await self.modul.authentifizieren(client, "123456")

        self.assertIn("Login-Befehl wurde abgelehnt", str(context.exception))
        client.commands.wait_for_events.assert_not_called()


class TestGeraeteinformationen(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        if "meshcore_companion_client" not in sys.modules:
            TestKonfiguration.setUpClass()
        cls.modul = sys.modules["meshcore_companion_client"]

    async def test_geraeteinformationen_zeigt_akkustand_aus_get_bat(self):
        client = SimpleNamespace(
            self_info={"name": "Node-A"},
            commands=SimpleNamespace(
                get_bat=AsyncMock(
                    return_value=SimpleNamespace(
                        type=self.modul.EventType.MSG_SENT,
                        payload={"battery_level": 73},
                    )
                )
            ),
        )

        with patch("builtins.print") as print_mock:
            await self.modul.geraeteinformationen_ausgeben(client)

        ausgaben = [aufruf.args[0] for aufruf in print_mock.call_args_list if aufruf.args]
        self.assertIn("Name      : Node-A", ausgaben)
        self.assertIn("Akkustand : 73%", ausgaben)

    async def test_geraeteinformationen_fallback_ohne_abbruch_wenn_get_bat_fehlt(self):
        client = SimpleNamespace(
            self_info={"name": "Node-B", "battery_level": 51},
            commands=SimpleNamespace(get_bat=AsyncMock(side_effect=RuntimeError("nicht verfügbar"))),
        )

        with patch("builtins.print") as print_mock:
            await self.modul.geraeteinformationen_ausgeben(client)

        ausgaben = [aufruf.args[0] for aufruf in print_mock.call_args_list if aufruf.args]
        self.assertIn("Name      : Node-B", ausgaben)
        self.assertIn("Akkustand : 51%", ausgaben)

    async def test_geraeteinformationen_normalisiert_vierstelliges_batterie_mv_format(self):
        client = SimpleNamespace(
            self_info={"name": "Node-C"},
            commands=SimpleNamespace(
                get_bat=AsyncMock(
                    return_value=SimpleNamespace(
                        type=self.modul.EventType.MSG_SENT,
                        payload={"battery_level": 4200},
                    )
                )
            ),
        )

        with patch("builtins.print") as print_mock:
            await self.modul.geraeteinformationen_ausgeben(client)

        ausgaben = [aufruf.args[0] for aufruf in print_mock.call_args_list if aufruf.args]
        self.assertIn("Akkustand : 100%", ausgaben)

    def test_client_name_aus_meshcore_geraet_nimmt_geraetename(self):
        client = SimpleNamespace(self_info={"name": "MeshNode-01"})

        name = self.modul.client_name_aus_meshcore_geraet(client, "fallback-client")

        self.assertEqual(name, "MeshNode-01")

    def test_client_name_aus_meshcore_geraet_verwendet_fallback_ohne_name(self):
        client = SimpleNamespace(self_info={})

        name = self.modul.client_name_aus_meshcore_geraet(client, "fallback-client")

        self.assertEqual(name, "fallback-client")



class TestAdvertPersistierung(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if "meshcore_companion_client" not in sys.modules:
            TestKonfiguration.setUpClass()
        cls.modul = sys.modules["meshcore_companion_client"]

    def test_advert_aufbereiten_bytes_wird_stabil_als_hex_aufbereitet(self):
        log_daten = {
            "adv_name": "Node-A",
            "adv_key": "abcd1234",
            "adv_lat": 52.52,
            "adv_lon": 13.405,
            "adv_type": 3,
            "pkt_payload": b"\x01\x02",
        }

        advert = self.modul.advert_aufbereiten(log_daten)

        self.assertEqual(advert["weitere_felder"]["pkt_payload"], "0102")

    def test_advert_persistieren_bytes_serialisierbar(self):
        log_daten = {
            "adv_name": "Node-B",
            "adv_key": "001122",
            "adv_lat": 48.137,
            "adv_lon": 11.575,
            "adv_type": 3,
            "pkt_payload": b"\x01\x02",
        }
        advert = self.modul.advert_aufbereiten(log_daten)

        with tempfile.TemporaryDirectory() as tmp:
            pfad = Path(tmp) / "adverts.jsonl"
            self.modul.advert_persistieren(pfad, advert)

            zeile = pfad.read_text(encoding="utf-8").strip()
            eingelesen = json.loads(zeile)

        self.assertEqual(eingelesen["weitere_felder"]["pkt_payload"], "0102")
        self.assertEqual(eingelesen["name"], "Node-B")
        self.assertEqual(eingelesen["public_key"], "001122")
        self.assertEqual(eingelesen["koordinaten"], {"latitude": 48.137, "longitude": 11.575})
        self.assertEqual(eingelesen["adv_typ"], 3)


class TestRxLogModus(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        if "meshcore_companion_client" not in sys.modules:
            TestKonfiguration.setUpClass()
        cls.modul = sys.modules["meshcore_companion_client"]

    async def test_rx_log_modus_beendet_sauber_bei_cancelled_error(self):
        client = SimpleNamespace(subscribe=lambda *_args, **_kwargs: None)

        with patch.object(
            self.modul.asyncio,
            "sleep",
            AsyncMock(side_effect=asyncio.CancelledError()),
        ), patch("builtins.print") as print_mock:
            await self.modul.rx_log_modus(client, Path("out.jsonl"))

        ausgaben = [aufruf.args[0] for aufruf in print_mock.call_args_list if aufruf.args]
        self.assertIn("[INFO] RX-Log läuft. Mit Strg+C beenden.", ausgaben)
        self.assertIn("\n[INFO] RX-Log beendet.", ausgaben)


class TestServerInfoAusgabe(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        if "meshcore_companion_client" not in sys.modules:
            TestKonfiguration.setUpClass()
        cls.modul = sys.modules["meshcore_companion_client"]

    def test_kompakte_server_info_formatiert_und_kuerzt_felder(self):
        langer_pfad = [f"hop-{index}" for index in range(1, 20)]
        log_daten = {
            "payload_typename": "ADVERT",
            "adv_key": "00112233445566778899aabbccddeeff",
            "path": langer_pfad,
            "adv_name": "MeshNode-Berlin-Mitte",
        }

        text = self.modul.kompakte_server_info(log_daten)

        self.assertIn("typ=ADVERT", text)
        self.assertIn("key=0011223344556677889…", text)
        self.assertIn("name=MeshNode-Berlin-Mitte", text)
        self.assertIn("path=", text)
        self.assertIn("…", text)

    def test_kompakte_server_info_nutzt_public_key_und_ohne_name(self):
        log_daten = {
            "payload_typename": "PATH",
            "public_key": "abcd",
            "path": ["A", "B", "C"],
        }

        text = self.modul.kompakte_server_info(log_daten)

        self.assertEqual(text, "typ=PATH | key=abcd | path=A -> B -> C")

    async def test_rx_log_modus_gibt_info_nach_erfolgreichem_post_aus(self):
        callback_box = {}

        def subscribe(_ereignis_typ, callback):
            callback_box["callback"] = callback

        client = SimpleNamespace(subscribe=subscribe)
        ereignis = SimpleNamespace(
            payload={
                "payload_typename": "ADVERT",
                "adv_type": self.modul.REPEATER_TYP_NUMMER,
                "adv_key": "00112233445566778899aabbccddeeff",
                "adv_name": "Node-A",
                "path": ["hop-1", "hop-2", "hop-3"],
            }
        )

        async def fake_sleep(_sekunden):
            if "callback" in callback_box:
                await callback_box["callback"](ereignis)
            raise asyncio.CancelledError()

        with patch.object(self.modul.asyncio, "sleep", AsyncMock(side_effect=fake_sleep)), patch.object(
            self.modul.asyncio, "to_thread", AsyncMock(return_value=None)
        ), patch.object(self.modul, "advert_persistieren"), patch("builtins.print") as print_mock:
            await self.modul.rx_log_modus(client, Path("out.jsonl"), "https://server.example", "client-a")

        ausgaben = [aufruf.args[0] for aufruf in print_mock.call_args_list if aufruf.args]
        self.assertTrue(
            any(ausgabe.startswith("[INFO] An Server übertragen: typ=ADVERT") for ausgabe in ausgaben)
        )


class TestServerStartPruefung(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if "meshcore_companion_client" not in sys.modules:
            TestKonfiguration.setUpClass()
        cls.modul = sys.modules["meshcore_companion_client"]

    def test_server_beim_start_pruefen_akzeptiert_status_400_als_erreichbar(self):
        with patch.object(self.modul.request, "urlopen") as urlopen_mock, patch("builtins.print") as print_mock:
            urlopen_mock.return_value.__enter__.return_value = SimpleNamespace(status=400)
            self.modul.server_beim_start_pruefen("https://mesh.do1ffe.de")

        ausgaben = [aufruf.args[0] for aufruf in print_mock.call_args_list if aufruf.args]
        self.assertTrue(any(ausgabe.startswith("[INFO] Prüfe Server-Verbindung über https://mesh.do1ffe.de/api/events") for ausgabe in ausgaben))
        self.assertIn("[INFO] Server erreichbar (HTTP 400) und POST-Endpunkt antwortet.", ausgaben)

    def test_server_beim_start_pruefen_akzeptiert_http_error_400_als_erreichbar(self):
        http_fehler = self.modul.error.HTTPError(
            url="https://mesh.do1ffe.de/api/events",
            code=400,
            msg="Bad Request",
            hdrs=None,
            fp=None,
        )

        with patch.object(self.modul.request, "urlopen", side_effect=http_fehler), patch("builtins.print") as print_mock:
            self.modul.server_beim_start_pruefen("https://mesh.do1ffe.de")

        ausgaben = [aufruf.args[0] for aufruf in print_mock.call_args_list if aufruf.args]
        self.assertIn("[INFO] Server erreichbar (HTTP 400) und POST-Endpunkt antwortet.", ausgaben)

    def test_server_beim_start_pruefen_wirft_fehler_bei_netzwerkproblem(self):
        with patch.object(self.modul.request, "urlopen", side_effect=RuntimeError("down")):
            with self.assertRaises(self.modul.Verbindungsfehler):
                self.modul.server_beim_start_pruefen("https://mesh.do1ffe.de")


class TestServerPayloadAufbereitung(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if "meshcore_companion_client" not in sys.modules:
            TestKonfiguration.setUpClass()
        cls.modul = sys.modules["meshcore_companion_client"]

    def test_event_an_server_senden_setzt_payload_typename_nach(self):
        log_daten = {"payloadTypeName": "PATH", "path": ["a1b2", "c3d4"]}

        with patch.object(self.modul.request, "Request") as request_mock, patch.object(
            self.modul.request,
            "urlopen",
        ) as urlopen_mock, patch("builtins.print") as print_mock:
            urlopen_mock.return_value.__enter__.return_value = SimpleNamespace(status=202)
            self.modul.event_an_server_senden("https://server.example", log_daten, "client-a")

        _, kwargs = request_mock.call_args
        payload = json.loads(kwargs["data"].decode("utf-8"))
        self.assertEqual(payload["payload_typename"], "PATH")
        self.assertEqual(payload["client_name"], "client-a")
        ausgaben = [aufruf.args[0] for aufruf in print_mock.call_args_list if aufruf.args]
        self.assertIn("[INFO] Serverantwort HTTP 202 für Event-Typ PATH.", ausgaben)


    def test_event_an_server_senden_uebernimmt_path_aus_grossgeschriebenem_feld(self):
        log_daten = {"payload_typename": "TEXT", "PATH": ["a1b2", "c3d4"]}

        with patch.object(self.modul.request, "Request") as request_mock, patch.object(
            self.modul.request,
            "urlopen",
        ) as urlopen_mock:
            urlopen_mock.return_value.__enter__.return_value = SimpleNamespace(status=202)
            self.modul.event_an_server_senden("https://server.example", log_daten, "client-a")

        _, kwargs = request_mock.call_args
        payload = json.loads(kwargs["data"].decode("utf-8"))
        self.assertEqual(payload["path"], ["a1b2", "c3d4"])


    def test_server_api_events_url_normalisiert_basis_und_endpoint(self):
        self.assertEqual(
            self.modul.server_api_events_url("https://mesh.do1ffe.de"),
            "https://mesh.do1ffe.de/api/events",
        )
        self.assertEqual(
            self.modul.server_api_events_url("https://mesh.do1ffe.de/api/events"),
            "https://mesh.do1ffe.de/api/events",
        )

    def test_server_api_events_url_lehnt_unvollstaendige_url_ab(self):
        with self.assertRaises(self.modul.Verbindungsfehler):
            self.modul.server_api_events_url("mesh.do1ffe.de")

    def test_event_an_server_senden_haengt_api_events_nicht_doppelt_an(self):
        log_daten = {"payload_typename": "ADVERT", "adv_key": "a1b2c3d4"}

        with patch.object(self.modul.request, "Request") as request_mock, patch.object(
            self.modul.request,
            "urlopen",
        ) as urlopen_mock:
            urlopen_mock.return_value.__enter__.return_value = SimpleNamespace(status=202)
            self.modul.event_an_server_senden("https://server.example/api/events", log_daten, "client-a")

        args, _ = request_mock.call_args
        self.assertEqual(args[0], "https://server.example/api/events")


if __name__ == "__main__":
    unittest.main()
