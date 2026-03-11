import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


class TestAdvertServer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location(
            "meshcore_advert_server", "scripts/meshcore_advert_server.py"
        )
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
        cls.modul = module

    def test_prefix_aus_public_key_liefert_ersten_hex_prefix(self):
        self.assertEqual(self.modul.prefix_aus_public_key("a1b2c3d4"), "a1b2")
        self.assertIsNone(self.modul.prefix_aus_public_key("ab"))

    def test_pfadsegmente_liest_4er_hexsegmente(self):
        segmente = self.modul.pfadsegmente("a1b2->c3d4 / eeff")
        self.assertEqual(segmente, ["a1b2", "c3d4", "eeff"])

    def test_datenbank_speichert_nur_advert_und_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.modul.Datenbank(Path(tmp) / "karte.db")
            with self.assertRaises(ValueError):
                db.speichere_event({"payload_typename": "MSG", "text": "x"})

    def test_map_daten_enthaelt_knoten_und_kanten(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.modul.Datenbank(Path(tmp) / "karte.db")
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "R1",
                    "adv_key": "a1b2ff00",
                    "adv_lat": 51.0,
                    "adv_lon": 10.0,
                    "path": "c3d4eeff",
                }
            )
            db.speichere_event(
                {
                    "payload_typename": "PATH",
                    "public_key": "a1b2ff00",
                    "path": "a1b2 c3d4 eeff",
                }
            )

            daten = db.map_daten()

        self.assertEqual(len(daten["nodes"]), 1)
        self.assertEqual(daten["nodes"][0]["prefix"], "a1b2")
        kanten = {(k["von"], k["nach"]) for k in daten["edges"]}
        self.assertIn(("a1b2", "c3d4"), kanten)

    def test_map_daten_json_serialisierbar(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.modul.Datenbank(Path(tmp) / "karte.db")
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "R2",
                    "adv_key": "ddee1122",
                }
            )
            json.dumps(db.map_daten(), ensure_ascii=False)


if __name__ == "__main__":
    unittest.main()
