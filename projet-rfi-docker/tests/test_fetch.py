"""Tests de rfi_ingest.fetch (sans réseau)."""

import csv
import time

import feedparser
import pytest

from rfi_ingest import fetch as fetch_mod

# Vraie fonction d'analyse, conservée avant tout monkeypatch.
_vrai_parse = feedparser.parse

RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
  <title>RFI test</title><link>https://www.rfi.fr</link><description>test</description>
  <item>
    <title>  Premier article  </title>
    <link>https://www.rfi.fr/fr/a1</link>
    <description>  Résumé 1  </description>
    <pubDate>Thu, 29 Oct 2026 10:00:00 +0000</pubDate>
    <category>Afrique</category>
  </item>
  <item>
    <title>Sans lien</title>
    <description>À ignorer</description>
  </item>
</channel></rss>"""

URL = "https://www.rfi.fr/fr/afrique/rss"


@pytest.fixture
def flux_factice(monkeypatch):
    """Remplace feedparser.parse par l'analyse du flux RSS de test (aucun réseau)."""
    monkeypatch.setattr(fetch_mod.feedparser, "parse", lambda url: _vrai_parse(RSS))


def test_safe_tag_garde_lettres_chiffres_tirets():
    assert fetch_mod.safe_tag("run__2026-10-29T11:00:00+00:00") == "run-2026-10-29T11-00-00-00-00"
    assert fetch_mod.safe_tag("///") == ""


def test_slugify():
    assert fetch_mod.slugify(URL) == "www-rfi-fr-fr-afrique-rss"
    assert fetch_mod.slugify("") == "flux"


def test_csv_path_deterministe(tmp_path):
    a = fetch_mod.csv_path(tmp_path, URL, "run1")
    assert a == fetch_mod.csv_path(tmp_path, URL, "run1")
    assert a != fetch_mod.csv_path(tmp_path, URL, "run2")
    assert fetch_mod.csv_path(tmp_path, URL, "").name.endswith("__manuel.csv")


def test_parse_entries_ignore_sans_lien_et_nettoie():
    parsed = _vrai_parse(RSS)
    lignes = fetch_mod.parse_entries(parsed, URL, "2026-10-29T11:00:00+00:00")
    assert len(lignes) == 1
    ligne = lignes[0]
    assert ligne["title"] == "Premier article"
    assert ligne["summary"] == "Résumé 1"
    assert ligne["category"] == "Afrique"
    assert ligne["published"] == "2026-10-29T10:00:00+00:00"
    assert ligne["feed_url"] == URL
    assert list(ligne) == fetch_mod.COLUMNS


def test_published_en_utc():
    date = time.strptime("2026-10-29 10:00:00", "%Y-%m-%d %H:%M:%S")
    assert fetch_mod._published({"published_parsed": date}) == "2026-10-29T10:00:00+00:00"
    assert fetch_mod._published({"updated_parsed": date}) == "2026-10-29T10:00:00+00:00"
    assert fetch_mod._published({}) == ""


def test_write_csv_atomique(tmp_path):
    cible = tmp_path / "sous" / "flux.csv"
    lignes = fetch_mod.parse_entries(_vrai_parse(RSS), URL, "t")
    resultat = fetch_mod.write_csv(lignes, cible)
    assert resultat == cible
    assert not list(tmp_path.rglob("*.tmp"))
    with cible.open(encoding="utf-8") as f:
        assert csv.DictReader(f).fieldnames == fetch_mod.COLUMNS


def test_write_csv_ecrase_l_existant(tmp_path):
    cible = tmp_path / "flux.csv"
    fetch_mod.write_csv([], cible)
    fetch_mod.write_csv(fetch_mod.parse_entries(_vrai_parse(RSS), URL, "t"), cible)
    with cible.open(encoding="utf-8") as f:
        assert len(list(csv.DictReader(f))) == 1


def test_fetch_idempotent(tmp_path, flux_factice):
    p1 = fetch_mod.fetch(URL, tmp_path, "run1")
    p2 = fetch_mod.fetch(URL, tmp_path, "run1")
    assert p1 == p2
    assert len(list(tmp_path.glob("*.csv"))) == 1
    fetch_mod.fetch(URL, tmp_path, "run2")
    assert len(list(tmp_path.glob("*.csv"))) == 2


def test_main_affiche_le_chemin_en_derniere_ligne(tmp_path, flux_factice, capsys):
    code = fetch_mod.main(["--url", URL, "--out", str(tmp_path), "--run-tag", "r1"])
    derniere = capsys.readouterr().out.strip().splitlines()[-1]
    assert code == 0
    assert derniere == str(fetch_mod.csv_path(tmp_path, URL, "r1"))


def test_main_run_tag_par_defaut(tmp_path, flux_factice, capsys):
    fetch_mod.main(["--url", URL, "--out", str(tmp_path)])
    assert capsys.readouterr().out.strip().endswith(".csv")


def test_flux_illisible_leve_runtime_error(tmp_path, monkeypatch):
    illisible = feedparser.FeedParserDict(bozo=True, bozo_exception=OSError("réseau"), entries=[])
    monkeypatch.setattr(fetch_mod.feedparser, "parse", lambda url: illisible)
    with pytest.raises(RuntimeError):
        fetch_mod.fetch(URL, tmp_path, "run1")
    assert not list(tmp_path.glob("*.csv"))


def test_statut_http_en_erreur_sans_entree(tmp_path, monkeypatch):
    erreur = feedparser.FeedParserDict(status=404, entries=[])
    monkeypatch.setattr(fetch_mod.feedparser, "parse", lambda url: erreur)
    with pytest.raises(RuntimeError):
        fetch_mod.fetch(URL, tmp_path, "run1")
