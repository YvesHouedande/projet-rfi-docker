"""Test d'intégrité des DAGs (DagBag). Ignoré si Airflow n'est pas installé."""

from pathlib import Path

import pytest

pytest.importorskip("airflow")

DAGS = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def dagbag(tmp_path_factory):
    import os

    # Les Variables lues au parsing viennent de l'environnement : pas de base nécessaire.
    os.environ.setdefault("AIRFLOW_VAR_RFI_DATA_VOLUME", "rfi-data")
    os.environ.setdefault("AIRFLOW_VAR_RFI_DOCKER_NETWORK", "rfi-net")
    os.environ.setdefault("AIRFLOW_VAR_RFI_FETCH_CONCURRENCY", "3")
    os.environ.setdefault("AIRFLOW_VAR_RFI_PROJECT_DIR", str(DAGS.parent))
    from airflow.dag_processing.dagbag import DagBag

    return DagBag(dag_folder=str(DAGS), bundle_path=DAGS, bundle_name="rfi-test")


def test_aucune_erreur_d_import(dagbag):
    assert dagbag.import_errors == {}


def test_dags_attendus(dagbag):
    assert {"rfi_pipeline", "rfi_publish_image"} <= set(dagbag.dags)


def test_chaine_du_pipeline(dagbag):
    dag = dagbag.dags["rfi_pipeline"]
    assert dag.get_task("fetch_feed").downstream_task_ids == {"build_load_command"}
    assert dag.get_task("build_load_command").downstream_task_ids == {"load_to_postgres"}
    assert dag.get_task("load_to_postgres").downstream_task_ids == {"purge_old_csv"}
    for nom in ("build_load_command", "purge_old_csv"):
        regle = dag.get_task(nom).trigger_rule
        assert getattr(regle, "value", regle) == "all_done"


def test_fetch_feed_mappe_et_plafonne(dagbag):
    fetch = dagbag.dags["rfi_pipeline"].get_task("fetch_feed")
    assert fetch.max_active_tis_per_dag == 3
    assert fetch.partial_kwargs["mount_tmp_dir"] is False
