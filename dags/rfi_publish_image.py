"""DAG manuel : construit l'image d'ingestion et la publie sur Docker Hub.

Le conteneur `docker:cli` pilote le démon Docker de l'hôte via le socket monté ; le dossier
du projet **côté hôte** (Variable rfi_project_dir) est monté en lecture seule sur /src.
"""

from __future__ import annotations

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG, Variable
from docker.types import Mount
from rfi_common import DEFAULT_ARGS, START_DATE

SCRIPT = """
set -eu
trap 'docker logout >/dev/null 2>&1 || true' EXIT
printf '%s' "$DOCKERHUB_TOKEN" | docker login -u "$DOCKERHUB_USER" --password-stdin
docker build -t rfi-ingest:latest -t "$DOCKERHUB_USER/rfi-ingest:$IMAGE_TAG" /src
docker push "$DOCKERHUB_USER/rfi-ingest:$IMAGE_TAG"
docker logout
"""

with DAG(
    dag_id="rfi_publish_image",
    description="Build et push de l'image rfi-ingest sur Docker Hub (déclenchement manuel)",
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    default_args={**DEFAULT_ARGS, "retries": 0},
    tags=["rfi", "docker"],
) as dag:
    DockerOperator(
        task_id="build_and_push",
        image="docker:cli",
        user="root",
        docker_url="unix://var/run/docker.sock",
        command=["sh", "-c", SCRIPT],
        environment={
            "DOCKERHUB_USER": "{{ var.value.dockerhub_user }}",
            "DOCKERHUB_TOKEN": "{{ var.value.dockerhub_token }}",
            "IMAGE_TAG": "{{ var.value.get('rfi_image_tag', 'latest') }}",
        },
        mounts=[
            Mount(source="/var/run/docker.sock", target="/var/run/docker.sock", type="bind"),
            Mount(
                source=Variable.get("rfi_project_dir", default="/CHEMIN/A/RENSEIGNER"),
                target="/src",
                type="bind",
                read_only=True,
            ),
        ],
        mount_tmp_dir=False,
        auto_remove="success",
        do_xcom_push=False,
    )
