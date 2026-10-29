"""Récupération d'un flux RSS et écriture dans un fichier CSV.

Usage :
    python -m rfi_ingest.fetch --url URL --out DOSSIER --run-tag TAG

La dernière ligne affichée sur stdout est le chemin du CSV : le DockerOperator
la remonte en XCom (dernière ligne de la sortie du conteneur).
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

import feedparser

COLUMNS = ["link", "title", "summary", "published", "category", "feed_url", "fetched_at"]


def safe_tag(texte: str) -> str:
    """Ne garde que les lettres, chiffres et tirets.

    Toute autre suite de caractères est remplacée par un seul tiret, ce qui évite
    les collisions (« /fr/monde » et « /frmonde » restent distincts).
    """
    return re.sub(r"[^A-Za-z0-9-]+", "-", texte).strip("-")


def slugify(url: str) -> str:
    """Transforme l'URL d'un flux en un nom court utilisable dans un nom de fichier."""
    parties = urlsplit(url)
    return safe_tag(parties.netloc + parties.path).lower() or "flux"


def csv_path(out_dir: str | Path, feed_url: str, run_tag: str) -> Path:
    """Chemin déterministe du CSV : même flux + même run → même fichier."""
    return Path(out_dir) / f"{slugify(feed_url)}__{safe_tag(run_tag) or 'manuel'}.csv"


def _published(entry) -> str:
    """Date de publication ISO 8601 en UTC, ou chaîne vide si absente."""
    date = entry.get("published_parsed") or entry.get("updated_parsed")
    if not date:
        return ""
    return datetime(*date[:6], tzinfo=timezone.utc).isoformat()


def parse_entries(parsed_feed, feed_url: str, fetched_at: str) -> list[dict]:
    """Convertit les entrées du flux en lignes de CSV (entrées sans lien ignorées)."""
    lignes = []
    for entry in parsed_feed.get("entries", []):
        lien = (entry.get("link") or "").strip()
        if not lien:
            continue
        tags = entry.get("tags") or []
        lignes.append(
            {
                "link": lien,
                "title": (entry.get("title") or "").strip(),
                "summary": (entry.get("summary") or "").strip(),
                "published": _published(entry),
                "category": (tags[0].get("term") or "") if tags else "",
                "feed_url": feed_url,
                "fetched_at": fetched_at,
            }
        )
    return lignes


def write_csv(rows: list[dict], cible: str | Path) -> Path:
    """Écrit le CSV de façon atomique : fichier .tmp puis renommage (écrase l'existant)."""
    cible = Path(cible)
    cible.parent.mkdir(parents=True, exist_ok=True)
    temporaire = cible.with_suffix(".csv.tmp")
    with temporaire.open("w", newline="", encoding="utf-8") as fichier:
        writer = csv.DictWriter(fichier, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    temporaire.replace(cible)
    return cible


def fetch(url: str, out_dir: str | Path, run_tag: str) -> Path:
    """Télécharge le flux et écrit son CSV. Lève RuntimeError si le flux est illisible."""
    parsed = feedparser.parse(url)
    entrees = parsed.get("entries", [])
    statut = parsed.get("status", 200)
    if not entrees and (parsed.get("bozo") or statut >= 400):
        raison = parsed.get("bozo_exception") or f"statut HTTP {statut}"
        raise RuntimeError(f"Flux illisible : {url} ({raison})")
    fetched_at = datetime.now(timezone.utc).isoformat()
    lignes = parse_entries(parsed, url, fetched_at)
    return write_csv(lignes, csv_path(out_dir, url, run_tag))


def main(argv: list[str] | None = None) -> int:
    """Point d'entrée en ligne de commande."""
    parser = argparse.ArgumentParser(description="Flux RSS → CSV")
    parser.add_argument("--url", required=True, help="URL du flux RSS")
    parser.add_argument("--out", required=True, help="dossier de sortie")
    parser.add_argument(
        "--run-tag",
        default=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
        help="identifiant du run (défaut : horodatage UTC)",
    )
    args = parser.parse_args(argv)
    chemin = fetch(args.url, args.out, args.run_tag)
    # Dernière ligne de stdout = chemin du CSV (lue par le DockerOperator).
    print(chemin)
    return 0


if __name__ == "__main__":
    sys.exit(main())
