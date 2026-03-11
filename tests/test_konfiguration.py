import importlib.util
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path


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


if __name__ == "__main__":
    unittest.main()
