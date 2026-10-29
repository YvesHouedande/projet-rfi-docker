"""Chargement de fichiers CSV dans PostgreSQL.

Usage :
    python -m rfi_ingest.load --files f1.csv f2.csv ...

Le chargement est piloté par une liste explicite de fichiers (jamais par un balayage
de dossier) et exécuté en une seule transaction : tout ou rien.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS rfi_articles (
    link       TEXT PRIMARY KEY,
    title      TEXT,
    summary    TEXT,
    published  TIMESTAMPTZ,
    category   TEXT,
    feed_url   TEXT,
    fetched_at TIMESTAMPTZ
)
"""

INSERT = """
INSERT INTO rfi_articles (link, title, summary, published, category, feed_url, fetched_at)
VALUES %s
ON CONFLICT (link) DO NOTHING
"""


def read_rows(path: str | Path) -> list[tuple]:
    """Lit un CSV et renvoie des tuples prêts à insérer (dates vides → None)."""
    with Path(path).open(newline="", encoding="utf-8") as fichier:
        return [
            (
                ligne["link"],
                ligne["title"],
                ligne["summary"],
                ligne["published"] or None,
                ligne["category"],
                ligne["feed_url"],
                ligne["fetched_at"] or None,
            )
            for ligne in csv.DictReader(fichier)
        ]


def connect():
    """Ouvre la connexion PostgreSQL à partir des variables d'environnement."""
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def load_files(conn, fichiers: list[str]) -> int:
    """Charge les fichiers en une seule transaction ; renvoie le nombre de lignes lues.

    Tous les fichiers sont lus avant d'ouvrir la transaction : un fichier absent
    lève FileNotFoundError sans rien écrire en base.
    """
    lignes: list[tuple] = []
    for fichier in fichiers:
        lignes.extend(read_rows(fichier))

    # « with conn » : commit si tout se passe bien, rollback en cas d'exception.
    with conn:
        with conn.cursor() as curseur:
            curseur.execute(CREATE_TABLE)
            if lignes:
                execute_values(curseur, INSERT, lignes)
    return len(lignes)


def main(argv: list[str] | None = None) -> int:
    """Point d'entrée en ligne de commande."""
    parser = argparse.ArgumentParser(description="CSV → PostgreSQL")
    parser.add_argument("--files", nargs="*", default=[], help="fichiers CSV à charger")
    args = parser.parse_args(argv)

    if not args.files:
        print("Aucun fichier à charger.")
        return 0

    conn = connect()
    try:
        total = load_files(conn, args.files)
    finally:
        conn.close()
    print(f"{total} ligne(s) lue(s) dans {len(args.files)} fichier(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
