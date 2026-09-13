from collections.abc import Iterable
from typing import Literal

from dlt.sources import DltResource, DltSource

StagingFileFormat = Literal["parquet", "jsonl", "csv"]
TableFormat = Literal["iceberg", "delta", "hive", "native"]


def adapt_gcs(
    source: DltSource,
    file_format: StagingFileFormat = "parquet",
    table_format: TableFormat | None = None,
) -> DltSource:
    """Stage ``source`` to an object store through dlt's filesystem destination.

    Ensures every resource is materialised in the given staging format so the
    downstream warehouse destination (e.g. ClickHouse) loads it in a second,
    separated step — the ELT read from the object store.
    """
    resources = [
        _adapt_resource(resource, file_format=file_format, table_format=table_format)
        for resource in _iter_resources(source)
    ]
    return DltSource(source.schema, source.section, resources)


def _adapt_resource(
    resource: DltResource,
    *,
    file_format: StagingFileFormat,
    table_format: TableFormat | None,
) -> DltResource:
    if table_format is not None:
        resource = resource.apply_hints(table_format=table_format)
    return resource.apply_hints(file_format=file_format)


def _iter_resources(source: DltSource) -> Iterable[DltResource]:
    return source.resources.values()
