from enum import StrEnum


class Destination(StrEnum):
    DUCKDB = "duckdb"
    CH = "clickhouse"
    S3 = "filesystem"
    BIGQUERY = "bigquery"
