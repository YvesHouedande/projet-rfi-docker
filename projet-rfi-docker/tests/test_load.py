"""Tests de rfi_ingest.load (sans base de données)."""

import csv

import pytest

from rfi_ingest import fetch as fetch_mod
from rfi_ingest import load as load_mod


def ecrire_csv(chemin, lignes):
    """Écrit un CSV au format produit par fetch."""
    with chemin.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fetch_mod.COLUMNS)
        writer.writeheader()
        writer.writerows(lignes)
    return chemin


def ligne(lien, publie="2026-10-29T10:00:00+00:00"):
    return {
        "link": lien,
        "title": "t",
        "summary": "s",
        "published": publie,
        "category": "c",
        "feed_url": "u",
        "fetched_at": "2026-10-29T11:00:00+00:00",
    }


class FauxCurseur:
    def __init__(self, journal):
        self.journal = journal

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql):
        self.journal.append(("execute", sql))


class FausseConnexion:
    """Compte les transactions ouvertes (« with conn ») et les curseurs créés."""

    def __init__(self):
        self.transactions = 0
        self.curseurs = 0
        self.journal = []

    def __enter__(self):
        self.transactions += 1
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self):
        self.curseurs += 1
        return FauxCurseur(self.journal)


def test_read_rows_dates_vides_deviennent_none(tmp_path):
    chemin = ecrire_csv(tmp_path / "a.csv", [ligne("https://x/1", publie="")])
    lignes = load_mod.read_rows(chemin)
    assert lignes == [("https://x/1", "t", "s", None, "c", "u", "2026-10-29T11:00:00+00:00")]


def test_load_files_une_seule_transaction(tmp_path, monkeypatch):
    appels = []
    monkeypatch.setattr(load_mod, "execute_values", lambda cur, sql, rows: appels.append(rows))
    f1 = ecrire_csv(tmp_path / "a.csv", [ligne("https://x/1"), ligne("https://x/2")])
    f2 = ecrire_csv(tmp_path / "b.csv", [ligne("https://x/3")])
    conn = FausseConnexion()

    total = load_mod.load_files(conn, [str(f1), str(f2)])

    assert total == 3
    assert conn.transactions == 1
    assert conn.curseurs == 1
    assert len(appels) == 1 and len(appels[0]) == 3
    assert "CREATE TABLE IF NOT EXISTS" in conn.journal[0][1]
    assert "ON CONFLICT (link) DO NOTHING" in load_mod.INSERT


def test_load_files_sans_ligne_ne_fait_pas_d_insert(tmp_path, monkeypatch):
    appels = []
    monkeypatch.setattr(load_mod, "execute_values", lambda *a: appels.append(a))
    vide = ecrire_csv(tmp_path / "vide.csv", [])
    conn = FausseConnexion()
    assert load_mod.load_files(conn, [str(vide)]) == 0
    assert appels == []
    assert conn.transactions == 1


def test_fichier_absent_n_ecrit_rien(tmp_path, monkeypatch):
    appels = []
    monkeypatch.setattr(load_mod, "execute_values", lambda *a: appels.append(a))
    bon = ecrire_csv(tmp_path / "a.csv", [ligne("https://x/1")])
    conn = FausseConnexion()

    with pytest.raises(FileNotFoundError):
        load_mod.load_files(conn, [str(bon), str(tmp_path / "absent.csv")])

    assert conn.transactions == 0
    assert conn.curseurs == 0
    assert appels == []


def test_main_sans_fichier(capsys):
    assert load_mod.main(["--files"]) == 0
    assert "Aucun fichier à charger." in capsys.readouterr().out


def test_main_avec_fichiers(tmp_path, monkeypatch, capsys):
    f1 = ecrire_csv(tmp_path / "a.csv", [ligne("https://x/1")])
    conn = FausseConnexion()
    conn.close = lambda: setattr(conn, "ferme", True)
    monkeypatch.setattr(load_mod, "connect", lambda: conn)
    monkeypatch.setattr(load_mod, "execute_values", lambda *a: None)

    assert load_mod.main(["--files", str(f1)]) == 0
    assert conn.ferme is True
    assert "1 ligne(s)" in capsys.readouterr().out


def test_connect_lit_l_environnement(monkeypatch):
    recu = {}
    monkeypatch.setattr(load_mod.psycopg2, "connect", lambda **kw: recu.update(kw) or "conn")
    for cle, val in {
        "POSTGRES_HOST": "h",
        "POSTGRES_DB": "d",
        "POSTGRES_USER": "u",
        "POSTGRES_PASSWORD": "p",
    }.items():
        monkeypatch.setenv(cle, val)
    monkeypatch.delenv("POSTGRES_PORT", raising=False)

    assert load_mod.connect() == "conn"
    assert recu == {"host": "h", "port": 5432, "dbname": "d", "user": "u", "password": "p"}
