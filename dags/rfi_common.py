"""Configuration partagée des DAGs RFI.

Deux façons de lire une Variable Airflow :
- champs **templatés** du DockerOperator (``image``, ``command``, ``environment``) :
  expressions Jinja ``{{ var.value.<nom> }}`` évaluées à l'exécution ;
- valeurs lues au **parsing** du DAG (ex. ``network_mode``, ``max_active_tis_per_dag``) :
  ``Variable.get("nom", default=...)``.

Remarque : avec le provider Docker utilisé ici (4.5.x), ``mounts`` figure aussi dans
``template_fields`` ; on le construit néanmoins au parsing, car un ``Mount`` est un
objet et non une chaîne à templater.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.sdk import Variable
from docker.types import Mount

DEFAULT_FEEDS = [
    "https://www.rfi.fr/fr/afrique/rss",
    "https://www.rfi.fr/fr/monde/rss",
    "https://www.rfi.fr/fr/france/rss",
    "https://www.rfi.fr/fr/economie/rss",
    "https://www.rfi.fr/fr/culture/rss",
]

DATA_DIR = "/data"
START_DATE = datetime(2026, 1, 1, tzinfo=timezone.utc)
DEFAULT_ARGS = {"retries": 2, "retry_delay": timedelta(minutes=2)}

# Image d'ingestion, lue à l'exécution (champ templaté).
INGEST_IMAGE = "{{ var.value.get('rfi_ingest_image', 'rfi-ingest:latest') }}"

# Environnement PostgreSQL pour la tâche de chargement (champ templaté).
# Le mot de passe n'a volontairement pas de valeur par défaut.
POSTGRES_ENV = {
    "POSTGRES_HOST": "{{ var.value.get('rfi_postgres_host', 'rfi-postgres') }}",
    "POSTGRES_DB": "{{ var.value.get('rfi_postgres_db', 'rfi') }}",
    "POSTGRES_USER": "{{ var.value.get('rfi_postgres_user', 'rfi') }}",
    "POSTGRES_PASSWORD": "{{ var.value.rfi_postgres_password }}",
}




def docker_defaults() -> dict:
    """Arguments communs à tous les DockerOperator d'ingestion.

    Les montages sont résolus par le démon Docker de l'hôte : on utilise donc un
    volume nommé, jamais un chemin interne au conteneur Airflow.
    """
    return {
        "docker_url": "unix://var/run/docker.sock",
        "mounts": [
            Mount(
                source=Variable.get("rfi_data_volume", default="rfi-data"),
                target=DATA_DIR,
                type="volume",
            )
        ],
        # Sinon un répertoire temporaire inexistant côté démon serait monté.
        "mount_tmp_dir": False,
        "auto_remove": "success",
        "network_mode": Variable.get("rfi_docker_network", default="rfi-net"),
        "image": INGEST_IMAGE,
    }
