from collections.abc import Iterable
from typing import Literal

from dlt.sources import DltResource, DltSource

StagingFileFormat = Literal["parquet", "jsonl", "csv"]


def adapt_gcs(
    source: DltSource,
    file_format: StagingFileFormat = "parquet",
) -> DltSource:
    """Stage ``source`` to an object store through dlt's filesystem destination.

    Ensures every resource is materialised in the given staging format so the
    downstream warehouse destination (e.g. ClickHouse) loads it in a second,
    separated step — the ELT read from the object store.
    """
    resources = [
        resource.apply_hints(file_format=file_format)
        for resource in _iter_resources(source)
    ]
    return DltSource(source.schema, source.section, resources)


def _iter_resources(source: DltSource) -> Iterable[DltResource]:
    return source.resources.values()
