from __future__ import annotations

from airflow.sdk import dag, task
from common import DEFAULT_ARGS, START_DATE, current_period, get_destination

from dlt_sources import pipelines
from dlt_sources.core.containers import Container


@dag(
    dag_id="ingest_nyc311",
    schedule="0 7 * * *",
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "nyc311"],
)
def ingest_nyc311():
    @task
    def run() -> None:
        pipelines.run_nyc311(
            current_period(),
            container=Container(),
            destination=get_destination(),
            pipeline_name="nyc311_ingestion",
            dataset_name="nyc311",
        )

    run()


ingest_nyc311()
