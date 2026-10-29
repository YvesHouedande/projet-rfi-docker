"""DAG principal : flux RSS RFI → CSV (en parallèle) → PostgreSQL → purge.

get_feeds ─► fetch_feed[0..n] ─► build_load_command ─► load_to_postgres ─► purge_old_csv
"""

from __future__ import annotations

import json

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG, Variable, task
from rfi_common import (
    DATA_DIR,
    DEFAULT_ARGS,
    DEFAULT_FEEDS,
    POSTGRES_ENV,
    START_DATE,
    docker_defaults,
)

# Plafond de fetch simultanés, pour ne pas surcharger RFI (Variable rfi_fetch_concurrency).
FETCH_CONCURRENCY = int(Variable.get("rfi_fetch_concurrency", default="3"))

with DAG(
    dag_id="rfi_pipeline",
    description="Ingestion horaire des flux RSS de RFI vers PostgreSQL",
    schedule="@hourly",
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["rfi"],
) as dag:

    @task
    def get_feeds() -> list[list[str]]:
        """Renvoie une commande `fetch` par flux (Variable rfi_feeds, sinon flux par défaut)."""
        flux = Variable.get("rfi_feeds", default=DEFAULT_FEEDS, deserialize_json=True)
        if isinstance(flux, str):
            flux = json.loads(flux)
        return [
            [
                "python", "-m", "rfi_ingest.fetch",
                "--url", url,
                "--out", DATA_DIR,
                # Rendu par Jinja au moment de l'exécution de la tâche mappée.
                "--run-tag", "{{ run_id }}",
            ]
            for url in flux
        ]

    feeds = get_feeds()

    # Un conteneur éphémère par flux. do_xcom_push (vrai par défaut) pousse la dernière
    # ligne de stdout, c'est-à-dire le chemin du CSV.
    fetch_feed = DockerOperator.partial(
        task_id="fetch_feed",
        do_xcom_push=True,
        max_active_tis_per_dag=FETCH_CONCURRENCY,
        **docker_defaults(),
    ).expand(command=feeds)

    @task(trigger_rule="all_done")
    def build_load_command(chemins) -> list[str]:
        """Construit la commande de chargement à partir des CSV effectivement produits.

        Les instances de fetch en échec ne poussent aucun XCom : on filtre les valeurs
        vides ou None, puis on supprime les doublons en conservant l'ordre.
        """
        uniques = list(dict.fromkeys(c.strip() for c in chemins if c and str(c).strip()))
        return ["python", "-m", "rfi_ingest.load", "--files", *uniques]

    load_command = build_load_command(fetch_feed.output)

    load_to_postgres = DockerOperator(
        task_id="load_to_postgres",
        command=load_command,
        environment=POSTGRES_ENV,
        **docker_defaults(),
    )

    # Supprime les CSV plus vieux que rfi_retention_days jours (défaut 7). Idempotent.
    purge_old_csv = DockerOperator(
        task_id="purge_old_csv",
        command=[
            "find", DATA_DIR, "-maxdepth", "1", "-type", "f",
            "(", "-name", "*.csv", "-o", "-name", "*.csv.tmp", ")",
            "-mtime", "+{{ var.value.get('rfi_retention_days', 7) }}",
            "-print", "-delete",
        ],
        trigger_rule="all_done",
        do_xcom_push=False,
        **docker_defaults(),
    )

    # build_load_command (all_done) s'exécute même si certains fetch ont échoué ; le
    # chargement n'a alors que les fichiers réussis (ou aucun : « Aucun fichier à charger. »).
    # Les dépendances sont déjà déduites des XComArgs ; la chaîne reste explicite.
    feeds >> fetch_feed >> load_command >> load_to_postgres >> purge_old_csv
