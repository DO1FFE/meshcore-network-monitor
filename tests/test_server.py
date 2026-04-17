import importlib.util
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock


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



    def test_max_age_filter_parst_standardwert(self):
        stunden, schalter = self.modul.max_age_filter_aus_parametern({})
        self.assertIsNone(stunden)
        self.assertEqual(schalter, "all")

    def test_html_karte_hat_all_option_als_standard(self):
        html_karte = self.modul.HTML_KARTE
        self.assertIn('<option value=\"all\" selected>ALLE</option>', html_karte)
        self.assertIn('<span id=\"aktiver-filter\">ALLE</span>', html_karte)

    def test_root_antwort_enthaelt_keinen_admin_link(self):
        handler = self.modul.Handler.__new__(self.modul.Handler)
        handler.path = "/"
        handler._html_antwort = Mock()

        self.modul.Handler.do_GET(handler)

        handler._html_antwort.assert_called_once_with(self.modul.HTTPStatus.OK, self.modul.HTML_KARTE)
        ausgeliefertes_html = handler._html_antwort.call_args.args[1]
        self.assertNotIn('href="/admin"', ausgeliefertes_html)
        self.assertNotIn("Administration", ausgeliefertes_html)

    def test_html_admin_enthaelt_formulare_und_button_texte(self):
        html_admin = self.modul.HTML_ADMIN
        self.assertIn('action=\"/admin/reset-prefixes\"', html_admin)
        self.assertIn('action=\"/admin/clear-database\"', html_admin)
        self.assertIn('Prefixe löschen', html_admin)
        self.assertIn('Restliche Datenbank löschen', html_admin)
        self.assertIn('/double-Ansicht', html_admin)

    def test_admin_html_rendering_ersetzt_status_ohne_template_fehler(self):
        handler = self.modul.Handler.__new__(self.modul.Handler)
        html_admin = handler._admin_html("Alles gut")

        self.assertIn("<p class=\"status\">Alles gut</p>", html_admin)
        self.assertNotIn("__STATUS_HINWEIS__", html_admin)

    def test_admin_html_rendering_escaped_status_text_ohne_roh_html(self):
        handler = self.modul.Handler.__new__(self.modul.Handler)
        status_text = '<script>alert("xss")</script><img src=x onerror=alert(1)>'
        html_admin = handler._admin_html(status_text)

        self.assertIn("&lt;script&gt;alert(&quot;xss&quot;)&lt;/script&gt;", html_admin)
        self.assertIn("&lt;img src=x onerror=alert(1)&gt;", html_admin)
        self.assertNotIn("<script>", html_admin)
        self.assertNotIn("<img src=x onerror=alert(1)>", html_admin)

    def test_max_age_filter_parst_all_ueber_max_age(self):
        stunden, schalter = self.modul.max_age_filter_aus_parametern({"max_age": ["all"]})
        self.assertIsNone(stunden)
        self.assertEqual(schalter, "all")

    def test_max_age_filter_lehnt_ungueltige_werte_ab(self):
        with self.assertRaises(ValueError):
            self.modul.max_age_filter_aus_parametern({"max_age_hours": ["5"]})
        with self.assertRaises(ValueError):
            self.modul.max_age_filter_aus_parametern({"max_age": ["1"]})
        with self.assertRaises(ValueError):
            self.modul.max_age_filter_aus_parametern({"max_age_hours": ["all"] , "max_age": ["all"]})

    def test_html_karte_enthaelt_gesamt_repeater_vor_sichtbare_repeater(self):
        html_karte = self.modul.HTML_KARTE
        index_gesamt = html_karte.find('id=\"gesamt-repeater\"')
        index_sichtbar = html_karte.find('id=\"sichtbare-repeater\"')

        self.assertNotEqual(index_gesamt, -1)
        self.assertNotEqual(index_sichtbar, -1)
        self.assertLess(index_gesamt, index_sichtbar)

    def test_html_karte_enthaelt_download_link_fuer_windows_client(self):
        html_karte = self.modul.HTML_KARTE
        self.assertIn('class="download-link"', html_karte)
        self.assertIn(
            'href="https://github.com/DO1FFE/meshcore-network-monitor/releases/download/client-exe-latest/meshcore_companion_client.exe"',
            html_karte,
        )
        self.assertIn('Client für Windows downloaden', html_karte)

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

    def test_setze_unbenutzte_prefixe_zurueck_stellt_alle_prefixe_wieder_her(self):
        with tempfile.TemporaryDirectory() as tmp:
            datei = Path(tmp) / "unbenutzte_prefixe.txt"
            self.modul.initialisiere_unbenutzte_prefixe(datei)
            self.modul.markiere_prefix_als_benutzt(datei, "a1")
            self.modul.setze_unbenutzte_prefixe_zurueck(datei)
            prefixe = self.modul.lese_unbenutzte_prefixe(datei)

        self.assertEqual(len(prefixe), 256)
        self.assertIn("a1", prefixe)

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

    def test_speichere_event_entfernt_genau_prefix_ab(self):
        with tempfile.TemporaryDirectory() as tmp:
            datei = Path(tmp) / "unbenutzte_prefixe.txt"
            db = self.modul.Datenbank(Path(tmp) / "karte.db", datei)
            prefixe_vorher = self.modul.lese_unbenutzte_prefixe(datei)

            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "R-AB",
                    "adv_key": "ab77ff00",
                }
            )

            prefixe_nachher = self.modul.lese_unbenutzte_prefixe(datei)

        self.assertIn("aa", prefixe_nachher)
        self.assertNotIn("ab", prefixe_nachher)
        self.assertIn("ac", prefixe_nachher)
        self.assertEqual(len(prefixe_vorher) - len(prefixe_nachher), 1)

    def test_doppelte_advert_events_bleiben_fuer_unbenutzte_prefixe_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            datei = Path(tmp) / "unbenutzte_prefixe.txt"
            db = self.modul.Datenbank(Path(tmp) / "karte.db", datei)

            ereignis = {
                "payload_typename": "ADVERT",
                "adv_name": "R-AB",
                "adv_key": "ab77ff00",
            }
            db.speichere_event(ereignis)
            prefixe_nach_erstem_event = self.modul.lese_unbenutzte_prefixe(datei)

            db.speichere_event(ereignis)
            prefixe_nach_zweitem_event = self.modul.lese_unbenutzte_prefixe(datei)

        self.assertEqual(prefixe_nach_erstem_event, prefixe_nach_zweitem_event)
        self.assertNotIn("ab", prefixe_nach_zweitem_event)
        self.assertEqual(len(prefixe_nach_zweitem_event), 255)

    def test_doppelte_advert_events_werden_aktualisiert_statt_dupliziert(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.modul.Datenbank(Path(tmp) / "karte.db", Path(tmp) / "unbenutzte_prefixe.txt")
            ereignis_alt = {
                "payload_typename": "ADVERT",
                "adv_name": "Repeater-Alt",
                "adv_key": "ab77ff00",
                "adv_lat": 50.0,
                "adv_lon": 8.0,
            }
            ereignis_neu = {
                "payload_typename": "ADVERT",
                "adv_name": "Repeater-Neu",
                "adv_key": "ab77ff00",
                "adv_lat": 50.0,
                "adv_lon": 8.0,
            }

            db.speichere_event(ereignis_alt)
            db.speichere_event(ereignis_neu)

            anzahl_adverts = db.verbindung.execute("SELECT COUNT(*) FROM adverts").fetchone()[0]
            gespeicherter_name = db.verbindung.execute(
                "SELECT name FROM adverts LIMIT 1"
            ).fetchone()[0]

        self.assertEqual(anzahl_adverts, 1)
        self.assertEqual(gespeicherter_name, "Repeater-Neu")

    def test_doppelte_path_events_werden_aktualisiert_statt_dupliziert(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.modul.Datenbank(Path(tmp) / "karte.db", Path(tmp) / "unbenutzte_prefixe.txt")
            ereignis_alt = {
                "payload_typename": "PATH",
                "public_key": "ab77ff00",
                "path": "a1b2",
            }
            ereignis_neu = {
                "payload_typename": "PATH",
                "public_key": "ab77ff00",
                "path": "a1b2",
            }

            db.speichere_event(ereignis_alt)
            db.speichere_event(ereignis_neu)

            anzahl_paths = db.verbindung.execute("SELECT COUNT(*) FROM paths").fetchone()[0]

        self.assertEqual(anzahl_paths, 1)

    def test_do_post_liefert_bad_request_bei_ungueltigem_content_length(self):
        handler = self.modul.Handler.__new__(self.modul.Handler)
        handler.path = "/api/events"
        handler.headers = {"Content-Length": "ungueltig"}
        handler.rfile = Mock()
        handler._json_antwort = Mock()

        self.modul.Handler.do_POST(handler)

        handler._json_antwort.assert_called_once_with(
            self.modul.HTTPStatus.BAD_REQUEST,
            {"fehler": "Ungültiger Content-Length-Header"},
        )
        handler.rfile.read.assert_not_called()

    def test_datenbank_speichert_nur_advert_und_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.modul.Datenbank(Path(tmp) / "karte.db", Path(tmp) / "unbenutzte_prefixe.txt")
            with self.assertRaises(ValueError):
                db.speichere_event({"payload_typename": "MSG", "text": "x"})

    def test_loesche_prefix_daten_leert_aliase_und_setzt_prefixe_zurueck(self):
        with tempfile.TemporaryDirectory() as tmp:
            datei = Path(tmp) / "unbenutzte_prefixe.txt"
            db = self.modul.Datenbank(Path(tmp) / "karte.db", datei)
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "R1",
                    "adv_key": "a1b2ff00",
                    "path": "c3d4",
                }
            )
            db.speichere_event(
                {
                    "payload_typename": "PATH",
                    "public_key": "a1b2ff00",
                    "path": "a1b2 c3d4",
                }
            )

            db.loesche_prefix_daten()
            anzahl_aliase = db.verbindung.execute("SELECT COUNT(*) FROM repeater_aliases").fetchone()[0]
            adverts_mit_prefix = db.verbindung.execute("SELECT COUNT(*) FROM adverts WHERE prefix IS NOT NULL").fetchone()[0]
            paths_mit_source_prefix = db.verbindung.execute(
                "SELECT COUNT(*) FROM paths WHERE source_prefix IS NOT NULL"
            ).fetchone()[0]
            prefixe = self.modul.lese_unbenutzte_prefixe(datei)

        self.assertEqual(anzahl_aliase, 0)
        self.assertEqual(adverts_mit_prefix, 0)
        self.assertEqual(paths_mit_source_prefix, 0)
        self.assertEqual(prefixe, [f"{wert:02x}" for wert in range(256)])

    def test_loesche_restliche_daten_entfernt_gesamten_inhalt(self):
        with tempfile.TemporaryDirectory() as tmp:
            datei = Path(tmp) / "unbenutzte_prefixe.txt"
            db = self.modul.Datenbank(Path(tmp) / "karte.db", datei)
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "R1",
                    "adv_key": "a1b2ff00",
                    "path": "c3d4",
                }
            )
            db.speichere_event(
                {
                    "payload_typename": "PATH",
                    "public_key": "a1b2ff00",
                    "path": "a1b2 c3d4",
                }
            )

            db.loesche_restliche_daten()
            anzahl_adverts = db.verbindung.execute("SELECT COUNT(*) FROM adverts").fetchone()[0]
            anzahl_paths = db.verbindung.execute("SELECT COUNT(*) FROM paths").fetchone()[0]
            anzahl_aliase = db.verbindung.execute("SELECT COUNT(*) FROM repeater_aliases").fetchone()[0]
            anzahl_repeater = db.verbindung.execute("SELECT COUNT(*) FROM repeaters").fetchone()[0]
            prefixe = self.modul.lese_unbenutzte_prefixe(datei)
            doppelte = db.doppelte_prefixe()

        self.assertEqual(anzahl_adverts, 0)
        self.assertEqual(anzahl_paths, 0)
        self.assertEqual(anzahl_aliase, 0)
        self.assertEqual(anzahl_repeater, 0)
        self.assertEqual(prefixe, [f"{wert:02x}" for wert in range(256)])
        self.assertEqual(doppelte, [])

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
                    "path": "a1b2",
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

    def test_gleiches_prefix_unterschiedlicher_key_bei_kurzer_distanz_erzeugt_zwei_knoten(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.modul.Datenbank(Path(tmp) / "karte.db", Path(tmp) / "unbenutzte_prefixe.txt")
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "R1",
                    "adv_key": "a1b2ff00",
                    "adv_lat": 50.0,
                    "adv_lon": 8.0,
                }
            )
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "R2",
                    "adv_key": "a1c3dd11",
                    "adv_lat": 50.01,
                    "adv_lon": 8.01,
                }
            )

            daten = db.map_daten()

        knoten_mit_prefix = [n for n in daten["nodes"] if "a1" in (n.get("prefixes") or [])]
        self.assertEqual(len(knoten_mit_prefix), 2)

    def test_identischer_vollstaendiger_key_bleibt_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.modul.Datenbank(Path(tmp) / "karte.db", Path(tmp) / "unbenutzte_prefixe.txt")
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "R1",
                    "adv_key": "a1b2ff00",
                    "adv_lat": 50.0,
                    "adv_lon": 8.0,
                }
            )
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "R1-Update",
                    "adv_key": "A1-B2-FF00",
                    "adv_lat": 50.0,
                    "adv_lon": 8.0,
                }
            )

            daten = db.map_daten()

        knoten_mit_prefix = [n for n in daten["nodes"] if "a1" in (n.get("prefixes") or [])]
        self.assertEqual(len(knoten_mit_prefix), 1)
        self.assertEqual(knoten_mit_prefix[0]["name"], "R1-Update")

    def test_name_fallback_verknuepft_adverts_mit_abweichendem_prefix(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.modul.Datenbank(Path(tmp) / "karte.db", Path(tmp) / "unbenutzte_prefixe.txt")
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "R1",
                    "adv_key": "a1b2ff00",
                    "adv_lat": 50.0,
                    "adv_lon": 8.0,
                }
            )
            erste_last_seen = db.verbindung.execute(
                "SELECT last_seen FROM repeaters WHERE name = ?",
                ("R1",),
            ).fetchone()[0]

            time.sleep(0.01)

            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "r1",
                    "adv_key": "b2c3aa11",
                }
            )

            anzahl_repeater = db.verbindung.execute("SELECT COUNT(*) FROM repeaters").fetchone()[0]
            aliases = {
                zeile[0]
                for zeile in db.verbindung.execute(
                    """
                    SELECT a.prefix
                    FROM repeater_aliases a
                    JOIN repeaters r ON r.id = a.repeater_id
                    WHERE LOWER(r.name) = LOWER(?)
                    """,
                    ("R1",),
                )
            }
            zweite_last_seen = db.verbindung.execute(
                "SELECT last_seen FROM repeaters WHERE LOWER(name) = LOWER(?)",
                ("R1",),
            ).fetchone()[0]

        self.assertEqual(anzahl_repeater, 1)
        self.assertEqual(aliases, {"a1", "b2"})
        self.assertGreater(zweite_last_seen, erste_last_seen)

    def test_name_fallback_aktualisiert_public_key_bei_neuem_adv_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.modul.Datenbank(Path(tmp) / "karte.db", Path(tmp) / "unbenutzte_prefixe.txt")
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "R1",
                    "adv_key": "a1b2ff00",
                    "adv_lat": 50.0,
                    "adv_lon": 8.0,
                }
            )

            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "r1",
                    "adv_key": "B2-C3-AA11",
                }
            )

            public_key = db.verbindung.execute(
                "SELECT public_key FROM repeaters WHERE LOWER(name) = LOWER(?)",
                ("R1",),
            ).fetchone()[0]

        self.assertEqual(public_key, "b2c3aa11")

    def test_map_daten_nimmt_prefix_aus_letztem_public_key_statt_alias_reihenfolge(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.modul.Datenbank(Path(tmp) / "karte.db", Path(tmp) / "unbenutzte_prefixe.txt")
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "Repeater-X",
                    "adv_key": "a1b2ff00",
                }
            )
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "repeater-x",
                    "adv_key": "b2c3aa11",
                }
            )

            daten = db.map_daten()

        self.assertEqual(len(daten["nodes"]), 1)
        knoten = daten["nodes"][0]
        self.assertEqual(knoten["public_key"], "b2c3aa11")
        self.assertEqual(knoten["prefix"], "b2")
        self.assertIn("a1", knoten["prefixes"])
        self.assertIn("b2", knoten["prefixes"])

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


    def test_map_daten_prefix_aufloesung_entfernt_keine_knoten(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.modul.Datenbank(Path(tmp) / "karte.db", Path(tmp) / "unbenutzte_prefixe.txt")
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "Start",
                    "adv_key": "1111aaaa",
                    "adv_lat": 50.0,
                    "adv_lon": 8.0,
                    "path": "a1b2",
                }
            )
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "A1-Nah",
                    "adv_key": "a1b2ffff",
                    "adv_lat": 50.05,
                    "adv_lon": 8.0,
                    "path": "1111",
                }
            )
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "A1-Zweit",
                    "adv_key": "a1c3eeee",
                    "adv_lat": 50.06,
                    "adv_lon": 8.01,
                }
            )

            daten = db.map_daten()

        self.assertEqual(len(daten["nodes"]), 3)
        namen = {eintrag["name"] for eintrag in daten["nodes"]}
        self.assertIn("A1-Nah", namen)
        self.assertIn("A1-Zweit", namen)
        kanten = {(kante["von_id"], kante["nach_id"]) for kante in daten["edges"]}
        self.assertTrue(any(von != nach for von, nach in kanten))

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
                    "path": "ab12",
                }
            )
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "AB12-Nah",
                    "adv_key": "ab12bbbb",
                    "adv_lat": 50.05,
                    "adv_lon": 8.0,
                    "path": "1111",
                }
            )
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "AB12-Fern",
                    "adv_key": "ab12cccc",
                    "adv_lat": 50.9,
                    "adv_lon": 8.0,
                }
            )

            daten = db.map_daten()

        id_nach_name = {eintrag["name"]: eintrag["id"] for eintrag in daten["nodes"]}
        kanten = {(kante["von_id"], kante["nach_id"]) for kante in daten["edges"]}
        self.assertIn(tuple(sorted((id_nach_name["Quelle"], id_nach_name["AB12-Nah"]))), kanten)
        self.assertNotIn((id_nach_name["Quelle"], id_nach_name["AB12-Fern"]), kanten)

    def test_map_daten_einseitige_kante_wird_nicht_uebernommen(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.modul.Datenbank(Path(tmp) / "karte.db", Path(tmp) / "unbenutzte_prefixe.txt")
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "A",
                    "adv_key": "1111aaaa",
                    "adv_lat": 50.0,
                    "adv_lon": 8.0,
                    "path": "cd34",
                }
            )
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "B",
                    "adv_key": "cd34bbbb",
                    "adv_lat": 50.09,
                    "adv_lon": 8.0,
                }
            )

            daten = db.map_daten()

        self.assertEqual(daten["edges"], [])


    def test_doppelte_prefixe_liefert_nur_mehrfach_vergebene_sortiert(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.modul.Datenbank(Path(tmp) / "karte.db", Path(tmp) / "unbenutzte_prefixe.txt")
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "R-AB-1",
                    "adv_key": "ab11aaaa",
                    "adv_lat": 50.0,
                    "adv_lon": 8.0,
                }
            )
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "R-AB-2",
                    "adv_key": "ab22bbbb",
                    "adv_lat": 50.8,
                    "adv_lon": 8.0,
                }
            )
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "R-0A-1",
                    "adv_key": "0a11cccc",
                    "adv_lat": 51.0,
                    "adv_lon": 8.0,
                }
            )
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "R-0A-2",
                    "adv_key": "0a22dddd",
                    "adv_lat": 52.0,
                    "adv_lon": 8.0,
                }
            )
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "R-7F",
                    "adv_key": "7f11eeee",
                    "adv_lat": 53.0,
                    "adv_lon": 8.0,
                }
            )

            doppelte = db.doppelte_prefixe()

        self.assertEqual(doppelte, [{"prefix": "0a", "anzahl": 2}, {"prefix": "ab", "anzahl": 2}])

    def test_veraltete_daten_werden_nach_sieben_tagen_entfernt(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.modul.Datenbank(Path(tmp) / "karte.db", Path(tmp) / "unbenutzte_prefixe.txt")
            zeitgrenze = self.modul.datetime.now(self.modul.timezone.utc) - self.modul.AUFBEWAHRUNGSDAUER
            alte_zeit = (zeitgrenze - self.modul.timedelta(minutes=1)).isoformat()
            frische_zeit = (zeitgrenze + self.modul.timedelta(minutes=1)).isoformat()

            repeater_alt = db.verbindung.execute(
                """
                INSERT INTO repeaters (name, public_key, latitude, longitude, last_seen)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("Alt", "aa11", 50.0, 8.0, alte_zeit),
            ).lastrowid
            repeater_neu = db.verbindung.execute(
                """
                INSERT INTO repeaters (name, public_key, latitude, longitude, last_seen)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("Neu", "bb22", 50.1, 8.1, frische_zeit),
            ).lastrowid
            db.verbindung.execute(
                "INSERT INTO repeater_aliases (repeater_id, prefix) VALUES (?, ?)",
                (repeater_alt, "aa"),
            )
            db.verbindung.execute(
                "INSERT INTO repeater_aliases (repeater_id, prefix) VALUES (?, ?)",
                (repeater_neu, "bb"),
            )
            db.verbindung.execute(
                """
                INSERT INTO adverts (event_schluessel, received_at, repeater_id, prefix, payload_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("alt-advert", alte_zeit, repeater_alt, "aa", "{}"),
            )
            db.verbindung.execute(
                """
                INSERT INTO adverts (event_schluessel, received_at, repeater_id, prefix, payload_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("neu-advert", frische_zeit, repeater_neu, "bb", "{}"),
            )
            db.verbindung.execute(
                """
                INSERT INTO paths (event_schluessel, received_at, source_prefix, path, payload_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("alt-path", alte_zeit, "aa", "aa11", "{}"),
            )
            db.verbindung.execute(
                """
                INSERT INTO paths (event_schluessel, received_at, source_prefix, path, payload_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("neu-path", frische_zeit, "bb", "bb22", "{}"),
            )
            db.verbindung.commit()

            _ = db.map_daten()

            anzahl_repeater = db.verbindung.execute("SELECT COUNT(*) FROM repeaters").fetchone()[0]
            anzahl_aliases = db.verbindung.execute("SELECT COUNT(*) FROM repeater_aliases").fetchone()[0]
            anzahl_adverts = db.verbindung.execute("SELECT COUNT(*) FROM adverts").fetchone()[0]
            anzahl_paths = db.verbindung.execute("SELECT COUNT(*) FROM paths").fetchone()[0]

        self.assertEqual(anzahl_repeater, 1)
        self.assertEqual(anzahl_aliases, 1)
        self.assertEqual(anzahl_adverts, 1)
        self.assertEqual(anzahl_paths, 1)

    def test_double_ignoriert_veraltete_repeater_aliases(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.modul.Datenbank(Path(tmp) / "karte.db", Path(tmp) / "unbenutzte_prefixe.txt")
            zeitgrenze = self.modul.datetime.now(self.modul.timezone.utc) - self.modul.AUFBEWAHRUNGSDAUER
            alte_zeit = (zeitgrenze - self.modul.timedelta(minutes=1)).isoformat()
            frische_zeit = (zeitgrenze + self.modul.timedelta(minutes=1)).isoformat()

            repeater_alt = db.verbindung.execute(
                """
                INSERT INTO repeaters (name, public_key, latitude, longitude, last_seen)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("Alt", "ab11", 50.0, 8.0, alte_zeit),
            ).lastrowid
            repeater_neu = db.verbindung.execute(
                """
                INSERT INTO repeaters (name, public_key, latitude, longitude, last_seen)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("Neu", "ab22", 50.1, 8.1, frische_zeit),
            ).lastrowid
            db.verbindung.execute(
                "INSERT INTO repeater_aliases (repeater_id, prefix) VALUES (?, ?)",
                (repeater_alt, "ab"),
            )
            db.verbindung.execute(
                "INSERT INTO repeater_aliases (repeater_id, prefix) VALUES (?, ?)",
                (repeater_neu, "ab"),
            )
            db.verbindung.commit()

            doppelte = db.doppelte_prefixe()
            anzahl_aliases = db.verbindung.execute(
                "SELECT COUNT(*) FROM repeater_aliases WHERE prefix = 'ab'"
            ).fetchone()[0]

        self.assertEqual(doppelte, [])
        self.assertEqual(anzahl_aliases, 1)

    def test_baue_doppelte_prefix_listeneintraege_erganzt_unbenutzte_prefixe(self):
        doppelte_prefixe = [{"prefix": "0a", "anzahl": 2}, {"prefix": "ab", "anzahl": 3}]
        unbenutzte_prefixe = ["00", "ab", "ff"]

        eintraege = self.modul.baue_doppelte_prefix_listeneintraege(doppelte_prefixe, unbenutzte_prefixe)

        self.assertEqual(len(eintraege), 256)
        self.assertEqual(eintraege[0], ("00", "*** BISHER UNBENUTZT ***"))
        self.assertEqual(eintraege[10], ("0a", "2"))
        self.assertEqual(eintraege[171], ("ab", "3"))
        self.assertEqual(eintraege[255], ("ff", "*** BISHER UNBENUTZT ***"))
        self.assertIn(("7e", "1"), eintraege)

    def test_baue_doppelte_prefix_hinweis_erscheint_nur_ohne_doppelte(self):
        mit_doppelten = self.modul.baue_doppelte_prefix_hinweis([{"prefix": "ab", "anzahl": 2}])
        ohne_doppelte = self.modul.baue_doppelte_prefix_hinweis([])

        self.assertEqual(mit_doppelten, "")
        self.assertIn("keine doppelten Prefixe", ohne_doppelte)

    def test_map_daten_bidirektionale_kante_wird_uebernommen(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.modul.Datenbank(Path(tmp) / "karte.db", Path(tmp) / "unbenutzte_prefixe.txt")
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "A",
                    "adv_key": "1111aaaa",
                    "adv_lat": 50.0,
                    "adv_lon": 8.0,
                    "path": "cd34",
                }
            )
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "B",
                    "adv_key": "cd34bbbb",
                    "adv_lat": 50.09,
                    "adv_lon": 8.0,
                    "path": "1111",
                }
            )

            daten = db.map_daten()

        id_nach_name = {eintrag["name"]: eintrag["id"] for eintrag in daten["nodes"]}
        kanten = {(kante["von_id"], kante["nach_id"]) for kante in daten["edges"]}
        self.assertIn(tuple(sorted((id_nach_name["A"], id_nach_name["B"]))), kanten)

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
                    "path": "cd34",
                }
            )
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "Ziel-Nah",
                    "adv_key": "cd34bbbb",
                    "adv_lat": 50.09,
                    "adv_lon": 8.0,
                    "path": "1111",
                }
            )

            daten = db.map_daten()

        id_nach_name = {eintrag["name"]: eintrag["id"] for eintrag in daten["nodes"]}
        kanten = {(kante["von_id"], kante["nach_id"]) for kante in daten["edges"]}
        self.assertIn(tuple(sorted((id_nach_name["Start"], id_nach_name["Ziel-Nah"]))), kanten)

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
                    "path": "cd34",
                }
            )
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "Ziel-Fern",
                    "adv_key": "cd34bbbb",
                    "adv_lat": 50.4,
                    "adv_lon": 8.0,
                    "path": "1111",
                }
            )

            daten = db.map_daten()

        self.assertEqual(daten["edges"], [])


    def test_map_daten_liefert_applied_filter_hours(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.modul.Datenbank(Path(tmp) / "karte.db", Path(tmp) / "unbenutzte_prefixe.txt")
            db.speichere_event({"payload_typename": "ADVERT", "adv_name": "R", "adv_key": "a1b2ff00"})
            daten = db.map_daten(max_age_stunden=6)

        self.assertEqual(daten["applied_filter_hours"], 6)

    def test_map_daten_enthaelt_zeitpunkt_letztes_datenpaket(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.modul.Datenbank(Path(tmp) / "karte.db", Path(tmp) / "unbenutzte_prefixe.txt")
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "R2",
                    "adv_key": "ddee1122",
                }
            )

            daten = db.map_daten()

        self.assertIn("last_packet_received", daten)
        self.assertIsNotNone(daten["last_packet_received"])

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


    def test_map_daten_zeitfilter_schliesst_ungueltige_last_seen_aus(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.modul.Datenbank(Path(tmp) / "karte.db", Path(tmp) / "unbenutzte_prefixe.txt")
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "Aktiv",
                    "adv_key": "a1b2ff00",
                    "adv_lat": 50.0,
                    "adv_lon": 8.0,
                }
            )
            db.verbindung.execute("UPDATE repeaters SET last_seen = ? WHERE name = ?", ("ungueltig", "Aktiv"))
            db.verbindung.commit()

            daten_gefiltert = db.map_daten(max_age_stunden=6)
            daten_alle = db.map_daten(max_age_stunden=None)

        self.assertEqual(daten_gefiltert["nodes"], [])
        self.assertEqual(len(daten_alle["nodes"]), 1)

    def test_map_daten_zeitfilter_verwirft_kanten_zu_alten_knoten(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.modul.Datenbank(Path(tmp) / "karte.db", Path(tmp) / "unbenutzte_prefixe.txt")
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "Neu-A",
                    "adv_key": "1111aaaa",
                    "adv_lat": 50.0,
                    "adv_lon": 8.0,
                    "path": "2222",
                }
            )
            db.speichere_event(
                {
                    "payload_typename": "ADVERT",
                    "adv_name": "Alt-B",
                    "adv_key": "2222bbbb",
                    "adv_lat": 50.09,
                    "adv_lon": 8.0,
                    "path": "1111",
                }
            )
            db.verbindung.execute(
                "UPDATE repeaters SET last_seen = datetime('now', '-2 days') WHERE name = ?",
                ("Alt-B",),
            )
            db.verbindung.commit()

            daten = db.map_daten(max_age_stunden=6)

        namen = {eintrag["name"] for eintrag in daten["nodes"]}
        self.assertEqual(namen, {"Neu-A"})
        self.assertEqual(daten["edges"], [])


class TestVerbundeneClients(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if "meshcore_advert_server" not in globals():
            spec = importlib.util.spec_from_file_location(
                "meshcore_advert_server", "scripts/meshcore_advert_server.py"
            )
            module = importlib.util.module_from_spec(spec)
            assert spec.loader is not None
            spec.loader.exec_module(module)
            cls.modul = module
        else:
            cls.modul = meshcore_advert_server

    def setUp(self):
        self.modul.Handler.verbundene_clients = {}

    def test_client_aktivitaet_markieren_uebernimmt_namen(self):
        jetzt = self.modul.datetime(2026, 1, 1, tzinfo=self.modul.timezone.utc)
        self.modul.Handler.client_aktivitaet_markieren("client-a", jetzt=jetzt)

        clients = self.modul.Handler._bereinige_und_liste_verbundene_clients(jetzt=jetzt)
        self.assertEqual(clients, ["client-a"])

    def test_bereinigung_entfernt_clients_nach_mehr_als_10_minuten(self):
        start = self.modul.datetime(2026, 1, 1, tzinfo=self.modul.timezone.utc)
        self.modul.Handler.client_aktivitaet_markieren("client-alt", jetzt=start)

        spaeter = start + self.modul.timedelta(minutes=11)
        clients = self.modul.Handler._bereinige_und_liste_verbundene_clients(jetzt=spaeter)

        self.assertEqual(clients, [])



if __name__ == "__main__":
    unittest.main()
