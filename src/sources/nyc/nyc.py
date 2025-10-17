import itertools
import os
from concurrent import futures
from typing import AsyncGenerator, Generator, Iterator, Literal, Sequence, cast

import dlt
import duckdb
import httpx
import pendulum
from dlt.destinations.adapters import clickhouse_adapter
from dlt.sources import DltResource
from pyarrow import RecordBatch
from loguru import logger

from .enums import NYCTripCategory
from .schemas import RecordYellowTrip, RecordFHVHVTrip, RecordFHVTrip, RecordGreenTrip

MAPPING = {
    NYCTripCategory.YELLOW: {
        "primary_key": ("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime"),
        "schema": RecordYellowTrip,
    },
    NYCTripCategory.GREEN: {
        "primary_key": ("VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime"),
        "schema": RecordGreenTrip,
    },
    NYCTripCategory.FHV: {
        "primary_key": tuple(),
        "schema": RecordFHVTrip
    },
    NYCTripCategory.FHVHV: {
        "primary_key": tuple(),
        "schema": RecordFHVHVTrip
    }
}

@dlt.source(name="nyc", max_table_nesting=3, parallelized=True)
def factory(
    categories: Sequence[NYCTripCategory],
    period: pendulum.Interval[pendulum.Date],
    *,
    base_url: str = dlt.config.value,
    chunk_size: int = 100000,
    write_disposition: Literal["append", "replace", "merge"] = "merge",
    table_engine: Literal["merge_tree", "replicated_merge_tree"] = "merge_tree"
) -> Generator[DltResource, None, None]:
    """
    A source for NYC taxi trips data collected from the NYC Taxi and Limousine Commission (TLC).
    Args:
        categories (Sequence[NYCTripCategory]): The categories of taxi trips to include in the source.
        period (pendulum.Interval[pendulum.Date]): The time period for which to collect taxi trip data.
        base_url (str): The base URL for the NYC Taxi and Limousine Commission API.
        write_disposition (Literal["append", "replace", "merge"]): The write disposition for the source.
    Returns:
        Generator[SourceFactory, None, None]:
                A generator of source factories which provides resources
                for the specified taxi categories over the specified period.
    """

    async def get_resource(
        response: httpx.Response, batch_size: int
    ) -> AsyncGenerator[Iterator]:
        
        relation = duckdb.read_parquet(str(response.url), filename=True)
        for batch in relation.fetch_arrow_reader(batch_size=batch_size):
            yield batch


    with httpx.Client(base_url=base_url) as client:
        tasks: list[httpx.Request] = []

        for category, current_date in itertools.product(
            categories, period.range("months")
        ):
            tasks.append(
                client.build_request(
                    "HEAD",
                    f"{category.value}_tripdata_{current_date.strftime('%Y-%m')}.parquet",
                    headers={
                        "x-report-category": category.value,
                        "x-report-date": current_date.isoformat(),
                    },
                )
            )

        with futures.ThreadPoolExecutor(os.cpu_count()) as executor:
            responses: Iterator[httpx.Response] = executor.map(client.send, tasks)

    for r in responses:
        category = r.request.headers["x-report-category"]
        current_date = cast(
            pendulum.DateTime, pendulum.parse(r.request.headers["x-report-date"])
        ).date()
        lookup_key = f"{category}:{current_date.strftime('%Y-%m')}"

        try:
            r.raise_for_status()  # pyright: ignore[reportUnusedCallResult]
            resource = dlt.resource(
                get_resource(r, batch_size=chunk_size),
                name=lookup_key,
                table_name=category,
                primary_key=MAPPING[NYCTripCategory[category.upper()]]["primary_key"],
                # columns=MAPPING[NYCTripCategory[category.upper()]]["schema"],
                write_disposition=write_disposition,
                file_format="parquet",
            )
            yield clickhouse_adapter(resource, table_engine_type=table_engine)
        except httpx.HTTPError as e:
            logger.warning(
                f"Resource {lookup_key} failed to be fetched: {e}",
                extra={
                    "source": "nyc",
                    "resource": lookup_key,
                },
            )
