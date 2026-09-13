from __future__ import annotations

from airflow.sdk import dag, task
from common import DEFAULT_ARGS, START_DATE, get_destination

from dlt_sources import pipelines
from dlt_sources.core.containers import Container


@dag(
    dag_id="ingest_zones",
    schedule="0 6 * * 1",
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "tlc_lookup"],
)
def ingest_zones():
    @task
    def run() -> None:
        pipelines.run_zones(
            container=Container(),
            destination=get_destination(),
            pipeline_name="tlc_lookup_ingestion",
            dataset_name="tlc_lookup",
        )

    run()


ingest_zones()
