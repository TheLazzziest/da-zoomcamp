from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import Variable, get_current_context

from dlt_sources.core.enums import Destination

DEFAULT_ARGS: dict = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
}

START_DATE = pendulum.datetime(2024, 1, 1, tz="UTC")


def get_destination() -> Destination:
    """Destination for every bronze load, overridable via the ``destination`` Airflow Variable."""
    return Destination(
        Variable.get("destination", default_var=Destination.DUCKDB.value)
    )


def current_period() -> pendulum.Interval:
    """The DAG run's data interval, as the pipeline period (backfill-aware)."""
    context = get_current_context()
    return pendulum.interval(
        context["data_interval_start"], context["data_interval_end"]
    )
