from pathlib import Path

import pytest
from airflow.dag_processing.dagbag import DagBag

DAGS_DIR = Path(__file__).resolve().parent.parent / "dags"

DAG_IDS = [
    "ingest_zones",
    "ingest_calendar",
    "ingest_weather",
    "ingest_nyc311",
    "ingest_nyc",
]


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_DIR))


@pytest.mark.parametrize("dag_id", DAG_IDS)
def test_dag_loads_without_import_errors(dagbag: DagBag, dag_id: str):
    assert dagbag.import_errors == {}
    assert dag_id in dagbag.dag_ids
