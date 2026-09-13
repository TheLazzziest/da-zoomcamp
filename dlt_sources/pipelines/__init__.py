from collections.abc import Sequence
from typing import cast

import dlt
import pendulum
from dlt.pipeline import LoadInfo
from dlt.sources import DltSource

from dlt_sources.core.containers import Container
from dlt_sources.core.enums import Destination
from dlt_sources.sources.calendar.calendar import Granularity as CalendarGranularity
from dlt_sources.sources.nyc.enums import NYCTripCategory
from dlt_sources.sources.weather.weather import Granularity as WeatherGranularity
from dlt_sources.transformers.clickhouse import TableEngine, adapt_clickhouse
from dlt_sources.transformers.gcs import adapt_gcs


def _adapt(source: DltSource, destination: Destination) -> DltSource:
    """Apply the destination-specific transformations to ``source``."""
    if destination is Destination.S3:
        return adapt_gcs(source, table_format="iceberg")
    if destination is Destination.CH:
        return adapt_gcs(source)
    return source


def ingest(
    source: DltSource,
    *,
    destination: Destination,
    pipeline_name: str,
    dataset_name: str,
    table_engine: TableEngine | None = None,
    dev_mode: bool = False,
    max_items: int | None = None,
) -> LoadInfo:
    """Adapt ``source`` for the destination and load it with dlt."""
    source = _adapt(source, destination)
    if table_engine and destination is Destination.CH:
        source = adapt_clickhouse(source, table_engine=table_engine)
    if max_items:
        source = source.add_limit(max_items=max_items)

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination.value,
        staging=("filesystem" if destination is Destination.CH else None),
        dataset_name=dataset_name,
        dev_mode=dev_mode,
    )
    return pipeline.run(source)


def run_nyc(
    categories: Sequence[NYCTripCategory],
    period: pendulum.Interval,
    *,
    container: Container,
    destination: Destination = Destination.DUCKDB,
    pipeline_name: str = "nyc_trip_data_ingestion",
    dataset_name: str = "nyc",
    table_engine: TableEngine | None = None,
    dev_mode: bool = False,
    max_items: int | None = None,
) -> LoadInfo:
    """Run the NYC taxi trips pipeline."""
    source = container.nyc_source(categories=categories, period=period)
    return ingest(
        source,
        destination=destination,
        pipeline_name=pipeline_name,
        dataset_name=dataset_name,
        table_engine=table_engine,
        dev_mode=dev_mode,
        max_items=max_items,
    )


def run_weather(
    period: pendulum.Interval,
    *,
    container: Container,
    granularities: Sequence[str] | None = None,
    destination: Destination = Destination.DUCKDB,
    pipeline_name: str = "weather_ingestion",
    dataset_name: str = "weather",
    dev_mode: bool = False,
    max_items: int | None = None,
) -> LoadInfo:
    """Run the Open-Meteo weather pipeline."""
    source = container.weather_source(
        period=period,
        granularities=cast(Sequence[WeatherGranularity] | None, granularities),
    )
    return ingest(
        source,
        destination=destination,
        pipeline_name=pipeline_name,
        dataset_name=dataset_name,
        dev_mode=dev_mode,
        max_items=max_items,
    )


def run_calendar(
    period: pendulum.Interval,
    *,
    container: Container,
    calendars: Sequence[str] | None = None,
    granularities: Sequence[str] | None = None,
    destination: Destination = Destination.DUCKDB,
    pipeline_name: str = "calendar_ingestion",
    dataset_name: str = "calendar",
    dev_mode: bool = False,
    max_items: int | None = None,
) -> LoadInfo:
    """Run the workalendar calendar reference pipeline."""
    source = container.calendar_source(
        period=period,
        calendars=calendars,
        granularities=cast(Sequence[CalendarGranularity] | None, granularities),
    )
    return ingest(
        source,
        destination=destination,
        pipeline_name=pipeline_name,
        dataset_name=dataset_name,
        dev_mode=dev_mode,
        max_items=max_items,
    )


def run_nyc311(
    period: pendulum.Interval,
    *,
    container: Container,
    destination: Destination = Destination.DUCKDB,
    pipeline_name: str = "nyc311_ingestion",
    dataset_name: str = "nyc311",
    dev_mode: bool = False,
    max_items: int | None = None,
) -> LoadInfo:
    """Run the NYC 311 service requests pipeline."""
    source = container.nyc311_source(period=period)
    return ingest(
        source,
        destination=destination,
        pipeline_name=pipeline_name,
        dataset_name=dataset_name,
        dev_mode=dev_mode,
        max_items=max_items,
    )


def run_zones(
    *,
    container: Container,
    destination: Destination = Destination.DUCKDB,
    pipeline_name: str = "tlc_lookup_ingestion",
    dataset_name: str = "tlc_lookup",
    dev_mode: bool = False,
    max_items: int | None = None,
) -> LoadInfo:
    """Run the TLC taxi zone lookup pipeline."""
    source = container.tlc_lookup_source()
    return ingest(
        source,
        destination=destination,
        pipeline_name=pipeline_name,
        dataset_name=dataset_name,
        dev_mode=dev_mode,
        max_items=max_items,
    )
