"""
BigQuery connector — fetches schema metadata and content samples.
No profiling logic here; returns raw data for profiler.py to process.
"""

from dataclasses import dataclass, field
from typing import Dict, Iterator, List, Optional

from google.cloud import bigquery

_TEXT_TYPES = {
    "STRING", "BYTES", "DATE", "DATETIME", "TIMESTAMP", "TIME", "BOOL", "BOOLEAN"
}

_DATE_TYPES = {"DATE"}
_TIMESTAMP_TYPES = {"TIMESTAMP", "DATETIME"}


def is_text_type(bq_data_type: str) -> bool:
    return bq_data_type.upper() in _TEXT_TYPES


def _safe_column(name: str) -> str:
    """Escape backticks in column/table names for use in BQ SQL identifiers."""
    return name.replace("`", "\\`")


def _partition_filter(col: str, data_type: str, days: int = 7) -> str:
    """
    Build a partition elimination filter for a given column and type.
    Returns an empty string for unsupported types (e.g. INTEGER range partitions).
    """
    t = data_type.upper()
    if t in _DATE_TYPES:
        return f"`{col}` >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)"
    if t in _TIMESTAMP_TYPES:
        return f"`{col}` >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)"
    return ""  # INTEGER range partition — skip filter, let caller handle


@dataclass
class ColumnMeta:
    project: str
    dataset: str
    table: str
    column: str
    data_type: str                      # BQ data type, e.g. "STRING", "INT64"
    is_partitioning_column: bool = False


@dataclass
class NumericStats:
    min_value: float
    max_value: float
    avg_value: float
    median: float           # 50th percentile
    iqr: float              # 75th - 25th percentile


@dataclass
class ColumnSample:
    meta: ColumnMeta
    values: List[str]       # sampled non-null values, cast to str
    approx_distinct: int = 0
    total_count: int = 0
    numeric_stats: Optional[NumericStats] = None


def get_columns(
    client: bigquery.Client,
    data_project: str,
    dataset: str,
    tables: List[str],
) -> Iterator[ColumnMeta]:
    """
    Fetch column metadata from INFORMATION_SCHEMA.COLUMNS.
    Free — no table scan. client must be initialised with the billing project.
    """
    query = f"""
    SELECT
        table_name,
        column_name,
        data_type,
        is_partitioning_column
    FROM `{data_project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name IN UNNEST(@tables)
    ORDER BY table_name, ordinal_position
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("tables", "STRING", tables)
        ]
    )
    for row in client.query(query, job_config=job_config).result():
        yield ColumnMeta(
            project=data_project,
            dataset=dataset,
            table=row.table_name,
            column=row.column_name,
            data_type=row.data_type,
            is_partitioning_column=(row.is_partitioning_column == "YES"),
        )


def build_partition_map(columns: List[ColumnMeta]) -> Dict[str, ColumnMeta]:
    """
    Returns a map of table_name -> partition ColumnMeta for tables that have one.
    Used by callers to inject partition filters into sample queries.
    """
    return {
        col.table: col
        for col in columns
        if col.is_partitioning_column
    }


def sample_column(
    client: bigquery.Client,
    meta: ColumnMeta,
    limit: int = 100_000,
    partition_col: Optional[ColumnMeta] = None,
    partition_days: int = 7,
) -> ColumnSample:
    """
    Fetch up to `limit` non-null values from a text column for MinHash.
    If partition_col is provided, adds a partition filter to avoid full-scan errors
    on required-partition tables. Uses 7-day window by default.
    """
    col = _safe_column(meta.column)
    fqtn = f"`{meta.project}.{meta.dataset}.{meta.table}`"
    partition_clause = ""
    if partition_col is not None:
        pf = _partition_filter(_safe_column(partition_col.column), partition_col.data_type, partition_days)
        if pf:
            partition_clause = f"AND {pf}"

    sample_query = f"""
    SELECT CAST(`{col}` AS STRING) AS val
    FROM {fqtn}
    WHERE `{col}` IS NOT NULL {partition_clause}
    LIMIT {limit}
    """
    values = [row.val for row in client.query(sample_query).result()]

    card_query = f"""
    SELECT
        APPROX_COUNT_DISTINCT(`{col}`) AS approx_distinct,
        COUNT(*) AS total_count
    FROM {fqtn}
    WHERE `{col}` IS NOT NULL {partition_clause}
    """
    row = next(iter(client.query(card_query).result()))

    return ColumnSample(
        meta=meta,
        values=values,
        approx_distinct=row.approx_distinct,
        total_count=row.total_count,
    )


def sample_numeric_column(
    client: bigquery.Client,
    meta: ColumnMeta,
    partition_col: Optional[ColumnMeta] = None,
    partition_days: int = 7,
) -> ColumnSample:
    """
    Compute numeric statistics for a numeric column.
    Single query: cardinality + min/max/avg/quantiles.
    Partition filter applied if partition_col is provided.
    """
    col = _safe_column(meta.column)
    fqtn = f"`{meta.project}.{meta.dataset}.{meta.table}`"
    partition_clause = ""
    if partition_col is not None:
        pf = _partition_filter(_safe_column(partition_col.column), partition_col.data_type, partition_days)
        if pf:
            partition_clause = f"AND {pf}"

    # APPROX_QUANTILES(x, 4) returns [0%, 25%, 50%, 75%, 100%]
    query = f"""
    SELECT
        APPROX_COUNT_DISTINCT(`{col}`)               AS approx_distinct,
        COUNT(*)                                      AS total_count,
        MIN(CAST(`{col}` AS FLOAT64))                 AS min_value,
        MAX(CAST(`{col}` AS FLOAT64))                 AS max_value,
        AVG(CAST(`{col}` AS FLOAT64))                 AS avg_value,
        APPROX_QUANTILES(CAST(`{col}` AS FLOAT64), 4) AS quantiles
    FROM {fqtn}
    WHERE `{col}` IS NOT NULL {partition_clause}
    """
    row = next(iter(client.query(query).result()))
    q = list(row.quantiles)  # [min, q25, median, q75, max]

    return ColumnSample(
        meta=meta,
        values=[],
        approx_distinct=row.approx_distinct,
        total_count=row.total_count,
        numeric_stats=NumericStats(
            min_value=row.min_value,
            max_value=row.max_value,
            avg_value=row.avg_value,
            median=q[2],
            iqr=q[3] - q[1],
        ),
    )
