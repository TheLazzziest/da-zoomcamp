import csv
import io
from collections.abc import Generator, Iterator
from typing import Literal

import dlt
import httpx
import pyarrow as pa
from dlt.sources import DltResource

from .schemas import ZoneLookupRecord

CSV_FIELDS = {
    "LocationID": "location_id",
    "Borough": "borough",
    "Zone": "zone",
    "service_zone": "service_zone",
}
DEFAULT_CHUNK_SIZE = 1000


@dlt.source(name="tlc_lookup", max_table_nesting=1)
def factory(
    *,
    base_url: str = dlt.config.value,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    write_disposition: Literal["append", "replace", "merge"] = "replace",
) -> Generator[DltResource]:
    """
    A source for the TLC taxi zone lookup (a static seed-style dimension).
    Args:
        base_url (str): The URL of the taxi zone lookup CSV.
        chunk_size (int): Arrow record-batch size (rows).
        write_disposition (Literal["append", "replace", "merge"]): The write disposition for the source.
    Returns:
        Generator[SourceFactory, None, None]: A generator of source factories.
    """

    def extract() -> Iterator[pa.RecordBatch]:
        response = httpx.get(base_url, timeout=60)
        response.raise_for_status()
        reader = csv.DictReader(io.StringIO(response.text))
        rows: list[dict] = []
        for record in reader:
            rows.append(
                {
                    "location_id": int(record["LocationID"]),
                    "borough": record["Borough"],
                    "zone": record["Zone"],
                    "service_zone": record["service_zone"],
                }
            )
            if len(rows) >= chunk_size:
                yield pa.RecordBatch.from_pylist(rows)
                rows.clear()
        if rows:
            yield pa.RecordBatch.from_pylist(rows)

    yield dlt.resource(
        extract(),
        name="zones",
        table_name="zones",
        primary_key=ZoneLookupRecord.primary_key(),
        columns=ZoneLookupRecord,
        write_disposition=write_disposition,
    )
