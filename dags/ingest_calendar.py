from __future__ import annotations

from airflow.sdk import dag, task
from common import DEFAULT_ARGS, START_DATE, current_period, get_destination

from dlt_sources import pipelines
from dlt_sources.core.containers import Container


@dag(
    dag_id="ingest_calendar",
    schedule="0 6 * * *",
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "calendar"],
)
def ingest_calendar():
    @task
    def run() -> None:
        pipelines.run_calendar(
            current_period(),
            container=Container(),
            calendars=["US-NY"],
            destination=get_destination(),
            pipeline_name="calendar_ingestion",
            dataset_name="calendar",
        )

    run()


ingest_calendar()
