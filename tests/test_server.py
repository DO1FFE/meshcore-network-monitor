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
        self.assertEqual(self.modul.prefix_aus_public_key("a1b2c3d4"), "a1")
        self.assertEqual(self.modul.prefix_aus_public_key("ab"), "ab")
        self.assertIsNone(self.modul.prefix_aus_public_key("a"))

    def test_pfadsegmente_liest_4er_hexsegmente(self):
        segmente = self.modul.pfadsegmente("a1b2->c3d4 / eeff")
        self.assertEqual(segmente, ["a1b2", "c3d4", "eeff"])

    def test_parse_argumente_unbenutzte_prefix_datei_konfigurierbar(self):
        import sys
        alte_argv = sys.argv
        try:
            sys.argv = ["prog", "--unused-prefix-file", "foo/bar.txt"]
            optionen = self.modul.parse_argumente()
        finally:
            sys.argv = alte_argv

        self.assertEqual(optionen.unused_prefix_file, Path("foo/bar.txt"))

    def test_initialisierung_unbenutzter_prefixe_erzeugt_256_eintraege(self):
        with tempfile.TemporaryDirectory() as tmp:
            datei = Path(tmp) / "unbenutzte_prefixe.txt"
            self.modul.initialisiere_unbenutzte_prefixe(datei)
            prefixe = self.modul.lese_unbenutzte_prefixe(datei)

        self.assertEqual(len(prefixe), 256)
        self.assertEqual(prefixe[0], "00")
        self.assertEqual(prefixe[-1], "ff")

    def test_markiere_prefix_als_benutzt_entfernt_eintrag_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            datei = Path(tmp) / "unbenutzte_prefixe.txt"
            self.modul.initialisiere_unbenutzte_prefixe(datei)
            self.modul.markiere_prefix_als_benutzt(datei, "A1")
            self.modul.markiere_prefix_als_benutzt(datei, "a1")
            prefixe = self.modul.lese_unbenutzte_prefixe(datei)

        self.assertNotIn("a1", prefixe)
        self.assertEqual(len(prefixe), 255)

    def test_speichere_event_entfernt_prefix_aus_unbenutzter_datei(self):
        with tempfile.TemporaryDirectory() as tmp:
            datei = Path(tmp) / "unbenutzte_prefixe.txt"
            db = self.modul.Datenbank(Path(tmp) / "karte.db", datei)
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "R1",
                    "adv_key": "a1b2ff00",
                }
            )
            prefixe = self.modul.lese_unbenutzte_prefixe(datei)

        self.assertNotIn("a1", prefixe)

    def test_datenbank_speichert_nur_advert_und_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.modul.Datenbank(Path(tmp) / "karte.db", Path(tmp) / "unbenutzte_prefixe.txt")
            with self.assertRaises(ValueError):
                db.speichere_event({"payload_typename": "MSG", "text": "x"})

    def test_map_daten_enthaelt_knoten_und_kanten(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.modul.Datenbank(Path(tmp) / "karte.db", Path(tmp) / "unbenutzte_prefixe.txt")
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
                    "payload_typename": "ADVERT",
                    "adv_name": "R2",
                    "adv_key": "c3d4aa00",
                    "adv_lat": 51.1,
                    "adv_lon": 10.1,
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

        self.assertEqual(len(daten["nodes"]), 2)
        self.assertTrue(all("id" in knoten for knoten in daten["nodes"]))
        self.assertEqual({k["prefix"] for k in daten["nodes"]}, {"a1", "c3"})
        kanten = {(k["von_id"], k["nach_id"]) for k in daten["edges"]}
        self.assertTrue(any(von != nach for von, nach in kanten))

    def test_gleiches_prefix_zwei_repeater_bei_grosser_distanz(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.modul.Datenbank(Path(tmp) / "karte.db", Path(tmp) / "unbenutzte_prefixe.txt")
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "R-Nord",
                    "adv_key": "a1b2ff00",
                    "adv_lat": 53.5511,
                    "adv_lon": 9.9937,
                }
            )
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "R-Sued",
                    "adv_key": "a1b2aa11",
                    "adv_lat": 48.1351,
                    "adv_lon": 11.582,
                }
            )

            daten = db.map_daten()

            knoten_mit_prefix = [n for n in daten["nodes"] if "a1" in (n.get("prefixes") or [])]
            self.assertEqual(len(knoten_mit_prefix), 2)
            ids = {n["id"] for n in knoten_mit_prefix}
            self.assertEqual(len(ids), 2)


    def test_map_daten_waehlt_bei_mehreren_prefix_kandidaten_den_naechsten(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.modul.Datenbank(Path(tmp) / "karte.db", Path(tmp) / "unbenutzte_prefixe.txt")
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "Quelle",
                    "adv_key": "1111aaaa",
                    "adv_lat": 50.0,
                    "adv_lon": 8.0,
                    "path": "a1b2",
                }
            )
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "A1B2-Nah",
                    "adv_key": "a1b2bbbb",
                    "adv_lat": 50.05,
                    "adv_lon": 8.0,
                }
            )
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "A1B2-Fern",
                    "adv_key": "a1b2cccc",
                    "adv_lat": 50.9,
                    "adv_lon": 8.0,
                }
            )

            daten = db.map_daten()

        id_nach_name = {eintrag["name"]: eintrag["id"] for eintrag in daten["nodes"]}
        kanten = {(kante["von_id"], kante["nach_id"]) for kante in daten["edges"]}
        self.assertIn((id_nach_name["Quelle"], id_nach_name["A1B2-Nah"]), kanten)
        self.assertNotIn((id_nach_name["Quelle"], id_nach_name["A1B2-Fern"]), kanten)

    def test_map_daten_kante_unter_20_km_wird_uebernommen(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.modul.Datenbank(Path(tmp) / "karte.db", Path(tmp) / "unbenutzte_prefixe.txt")
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "Start",
                    "adv_key": "1111aaaa",
                    "adv_lat": 50.0,
                    "adv_lon": 8.0,
                    "path": "2222",
                }
            )
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "Ziel-Nah",
                    "adv_key": "2222bbbb",
                    "adv_lat": 50.09,
                    "adv_lon": 8.0,
                }
            )

            daten = db.map_daten()

        id_nach_name = {eintrag["name"]: eintrag["id"] for eintrag in daten["nodes"]}
        kanten = {(kante["von_id"], kante["nach_id"]) for kante in daten["edges"]}
        self.assertIn((id_nach_name["Start"], id_nach_name["Ziel-Nah"]), kanten)

    def test_map_daten_kante_ueber_20_km_wird_verworfen(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.modul.Datenbank(Path(tmp) / "karte.db", Path(tmp) / "unbenutzte_prefixe.txt")
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "Start",
                    "adv_key": "1111aaaa",
                    "adv_lat": 50.0,
                    "adv_lon": 8.0,
                    "path": "2222",
                }
            )
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "Ziel-Fern",
                    "adv_key": "2222bbbb",
                    "adv_lat": 50.4,
                    "adv_lon": 8.0,
                }
            )

            daten = db.map_daten()

        self.assertEqual(daten["edges"], [])

    def test_map_daten_json_serialisierbar(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.modul.Datenbank(Path(tmp) / "karte.db", Path(tmp) / "unbenutzte_prefixe.txt")
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
