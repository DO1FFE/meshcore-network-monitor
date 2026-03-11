import importlib.util
import json
import sys
import tempfile
import types
import unittest
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
        )
        geraet = SimpleNamespace(address="AA:BB:CC:DD:EE:FF")
        erwarteter_client = object()

        with patch.object(self.modul, "ble_geraet_interaktiv_auswaehlen", AsyncMock(return_value=geraet)), patch.object(
            self.modul.MeshCore, "create_ble", AsyncMock(return_value=erwarteter_client), create=True
        ) as create_ble_mock:
            client = await self.modul.meshcore_verbinden(optionen)

        self.assertIs(client, erwarteter_client)
        create_ble_mock.assert_awaited_once_with(
            "AA:BB:CC:DD:EE:FF", pin="123456", default_timeout=5.0
        )

    async def test_ble_fallback_verwendet_address_parameter_bei_typeerror(self):
        optionen = self.modul.CliOptionen(
            com_port=None,
            baudrate=115200,
            ble_scan=True,
            timeout=7.0,
            ausgabe_pfad=Path("out.jsonl"),
            pin=None,
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

        self.assertEqual(erster_aufruf.args, ("11:22:33:44:55:66",))
        self.assertEqual(erster_aufruf.kwargs, {"pin": None, "default_timeout": 7.0})
        self.assertEqual(zweiter_aufruf.args, ())
        self.assertEqual(
            zweiter_aufruf.kwargs,
            {"address": "11:22:33:44:55:66", "pin": None, "default_timeout": 7.0},
        )


if __name__ == "__main__":
    unittest.main()
