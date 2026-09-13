from collections.abc import Iterable
from typing import Literal

from dlt.destinations.adapters import clickhouse_adapter
from dlt.sources import DltResource, DltSource

TableEngine = Literal["merge_tree", "replicated_merge_tree"]


def adapt_clickhouse(
    source: DltSource,
    table_engine: TableEngine,
) -> DltSource:
    """Wrap every resource of ``source`` with the ClickHouse adapter.

    Target-specific concern, decoupled from the source which is destination-agnostic.
    """
    resources = [
        clickhouse_adapter(resource, table_engine_type=table_engine)
        for resource in _iter_resources(source)
    ]
    return DltSource(source.schema, source.section, resources)


def _iter_resources(source: DltSource) -> Iterable[DltResource]:
    return source.resources.values()
