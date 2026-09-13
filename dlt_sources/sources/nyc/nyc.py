import itertools
import os
from collections.abc import AsyncGenerator, Generator, Iterator, Sequence
from concurrent import futures
from typing import Literal, cast

import dlt
import duckdb
import httpx
import pendulum
from dlt.sources import DltResource
from loguru import logger

from dlt_sources.core.http import build_client
from dlt_sources.core.settings import get_settings

from .enums import NYCTripCategory
from .schemas import NYCRecordSchema


def discover_categories() -> dict[str, tuple[tuple[str, ...], type[NYCRecordSchema]]]:
    return {
        cls.category: (cls.primary_key(), cls)
        for cls in NYCRecordSchema.__subclasses__()
        if cls.category
    }


MAPPING = discover_categories()


@dlt.source(name="nyc", max_table_nesting=3, parallelized=True)
def factory(
    categories: Sequence[NYCTripCategory] | None = None,
    *,
    period: pendulum.Interval[pendulum.Date],
    base_url: str = dlt.config.value,
    chunk_size: int = 100000,
    write_disposition: Literal["append", "replace", "merge"] = "merge",
) -> Generator[DltResource]:
    """
    A source for NYC taxi trips data collected from the NYC Taxi and Limousine Commission (TLC).
    Args:
        categories (Sequence[NYCTripCategory]): The categories of taxi trips to include in the source.
            Defaults to all categories registered in the schema discovery registry.
        period (pendulum.Interval[pendulum.Date]): The time period for which to collect taxi trip data.
        base_url (str): The base URL for the NYC Taxi and Limousine Commission API.
        write_disposition (Literal["append", "replace", "merge"]): The write disposition for the source.
            Target-specific adaptation (e.g. ClickHouse table engine) is applied downstream,
            so this source stays target-agnostic.
    Returns:
        Generator[SourceFactory, None, None]:
                A generator of source factories which provides resources
                for the specified taxi categories over the specified period.
    """

    categories = categories or [
        NYCTripCategory[name.upper()] for name in discover_categories()
    ]

    async def get_resource(
        response: httpx.Response, batch_size: int
    ) -> AsyncGenerator[Iterator]:

        relation = duckdb.read_parquet(str(response.url), filename=True)
        for batch in relation.fetch_arrow_reader(batch_size=batch_size):
            yield batch

    with build_client(base_url=base_url, timeout=get_settings().http.timeout) as client:
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
        category_name = r.request.headers["x-report-category"]
        current_date = cast(
            pendulum.DateTime, pendulum.parse(r.request.headers["x-report-date"])
        ).date()
        lookup_key = f"{category_name}:{current_date.strftime('%Y-%m')}"

        try:
            r.raise_for_status()  # pyright: ignore[reportUnusedCallResult]
            primary_key, schema = MAPPING[category_name]
            resource = dlt.resource(  # type: ignore[call-overload]  # dlt: async-generator resource typing
                get_resource(r, batch_size=chunk_size),
                name=lookup_key,
                table_name=category_name,
                primary_key=primary_key,
                columns=schema,
                write_disposition=write_disposition,
            )
            yield resource
        except httpx.HTTPError as e:
            logger.warning(
                f"Resource {lookup_key} failed to be fetched: {e}",
                extra={
                    "source": "nyc",
                    "resource": lookup_key,
                },
            )
