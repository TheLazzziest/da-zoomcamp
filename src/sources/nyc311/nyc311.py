from collections.abc import Generator, Iterator
from typing import Literal

import dlt
import httpx
import pendulum
import pyarrow as pa
from dlt.sources import DltResource

from .schemas import ServiceRequestRecord

DATE_FIELDS = (
    "created_date",
    "closed_date",
    "due_date",
    "resolution_action_updated_date",
)
SELECT_FIELDS = tuple(ServiceRequestRecord.model_fields)
DEFAULT_DATASET_ID = "erm2-nwe9"  # 311 Service Requests from 2020 to present
DEFAULT_PAGE_SIZE = 1000  # Socrata anonymous request cap
DEFAULT_CHUNK_SIZE = 10000


@dlt.source(name="nyc311", max_table_nesting=1)
def factory(
    *,
    period: pendulum.Interval[pendulum.DateTime],
    base_url: str = dlt.config.value,
    dataset_id: str = DEFAULT_DATASET_ID,
    app_token: str | None = None,
    page_size: int = DEFAULT_PAGE_SIZE,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    write_disposition: Literal["append", "replace", "merge"] = "merge",
) -> Generator[DltResource]:
    """
    A source for NYC 311 service requests collected from the Socrata Open Data API.
    Args:
        period (pendulum.Interval[pendulum.DateTime]): The ``created_date`` range to collect requests for (inclusive start, exclusive end).
        base_url (str): The Socrata resource base URL.
        dataset_id (str): The Socrata dataset id (defaults to the 2020-present 311 dataset).
        app_token (str): Optional Socrata app token to raise rate limits.
        page_size (int): Number of rows fetched per API page (keyset paginated on ``unique_key``).
        chunk_size (int): Arrow record-batch size (rows).
        write_disposition (Literal["append", "replace", "merge"]): The write disposition for the source.
    Returns:
        Generator[SourceFactory, None, None]: A generator of source factories for the given period.
    """

    headers = {"X-App-Token": app_token} if app_token else None
    where = (
        f"created_date >= '{period.start.format('YYYY-MM-DDTHH:mm:ss')}' "
        f"AND created_date < '{period.end.format('YYYY-MM-DDTHH:mm:ss')}'"
    )

    def extract() -> Iterator[pa.RecordBatch]:
        rows: list[dict] = []
        last_key: str | None = None

        with httpx.Client(base_url=base_url, headers=headers, timeout=60) as client:

            def fetch_page() -> list[dict]:
                nonlocal last_key
                response = client.get(
                    f"/{dataset_id}.json",
                    params={
                        "$select": ",".join(SELECT_FIELDS),
                        "$where": (
                            where
                            if last_key is None
                            else f"{where} AND unique_key > '{last_key}'"
                        ),
                        "$order": "unique_key",
                        "$limit": page_size,
                    },
                )
                response.raise_for_status()
                records = response.json()
                if records:
                    last_key = records[-1]["unique_key"]
                return records

            previous_key: str | None = None
            for records in iter(fetch_page, []):
                rows.extend(
                    {
                        field: (
                            pendulum.parse(record[field])
                            if field in DATE_FIELDS and record.get(field)
                            else record.get(field)
                        )
                        for field in SELECT_FIELDS
                    }
                    for record in records
                )

                if len(rows) >= chunk_size:
                    yield pa.RecordBatch.from_pylist(rows)
                    rows.clear()

                if (
                    len(records) < page_size
                    or records[-1]["unique_key"] == previous_key
                ):
                    break
                previous_key = records[-1]["unique_key"]

        if rows:
            yield pa.RecordBatch.from_pylist(rows)

    yield dlt.resource(
        extract(),
        name="requests",
        table_name="nyc311",
        primary_key=ServiceRequestRecord.primary_key(),
        columns=ServiceRequestRecord,
        write_disposition=write_disposition,
    )
