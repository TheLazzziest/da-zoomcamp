from pathlib import Path

from airflow.dag_processing.dagbag import DagBag

DAGS_DIR = Path(__file__).resolve().parent.parent / "dags"

EXPECTED_DAG_IDS = {
    "ingest_zones",
    "ingest_calendar",
    "ingest_weather",
    "ingest_nyc311",
    "ingest_nyc",
}


def test_dags_load_without_import_errors():
    bag = DagBag(dag_folder=str(DAGS_DIR))
    assert bag.import_errors == {}
    assert set(bag.dag_ids) >= EXPECTED_DAG_IDS
