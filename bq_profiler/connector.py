"""
BigQuery connector — fetches schema metadata and content samples.
No profiling logic here; returns raw data for profiler.py to process.
"""

from dataclasses import dataclass
from typing import Iterator, List, Optional

from google.cloud import bigquery

_TEXT_TYPES = {
    "STRING", "BYTES", "DATE", "DATETIME", "TIMESTAMP", "TIME", "BOOL", "BOOLEAN"
}


def is_text_type(bq_data_type: str) -> bool:
    return bq_data_type.upper() in _TEXT_TYPES


def _safe_column(name: str) -> str:
    """Escape backticks in column/table names for use in BQ SQL identifiers."""
    return name.replace("`", "\\`")


@dataclass
class ColumnMeta:
    project: str
    dataset: str
    table: str
    column: str
    data_type: str          # BQ data type string, e.g. "STRING", "INT64"
    description: Optional[str] = None


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
        description
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
            description=row.description,
        )


def sample_column(
    client: bigquery.Client,
    meta: ColumnMeta,
    sample_pct: float = 1.0,
    limit: int = 100_000,
) -> ColumnSample:
    """
    Sample text column values and compute cardinality.
    Costs BQ scan proportional to sample_pct.
    """
    col = _safe_column(meta.column)
    fqtn = f"`{meta.project}.{meta.dataset}.{meta.table}`"

    sample_query = f"""
    SELECT CAST(`{col}` AS STRING) AS val
    FROM {fqtn}
    TABLESAMPLE SYSTEM ({sample_pct} PERCENT)
    WHERE `{col}` IS NOT NULL
    LIMIT {limit}
    """
    values = [row.val for row in client.query(sample_query).result()]

    # HyperLogLog cardinality — scans full table but BQ makes this cheap
    card_query = f"""
    SELECT
        APPROX_COUNT_DISTINCT(`{col}`) AS approx_distinct,
        COUNT(*) AS total_count
    FROM {fqtn}
    WHERE `{col}` IS NOT NULL
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
) -> ColumnSample:
    """
    Compute numeric statistics for a numeric column.
    Single query: cardinality + min/max/avg/quantiles.
    Scans full column — no TABLESAMPLE (stats need full distribution).
    """
    col = _safe_column(meta.column)
    fqtn = f"`{meta.project}.{meta.dataset}.{meta.table}`"

    # APPROX_QUANTILES(x, 4) returns [0%, 25%, 50%, 75%, 100%]
    query = f"""
    SELECT
        APPROX_COUNT_DISTINCT(`{col}`)          AS approx_distinct,
        COUNT(*)                                 AS total_count,
        MIN(CAST(`{col}` AS FLOAT64))            AS min_value,
        MAX(CAST(`{col}` AS FLOAT64))            AS max_value,
        AVG(CAST(`{col}` AS FLOAT64))            AS avg_value,
        APPROX_QUANTILES(CAST(`{col}` AS FLOAT64), 4) AS quantiles
    FROM {fqtn}
    WHERE `{col}` IS NOT NULL
    """
    row = next(iter(client.query(query).result()))
    q = list(row.quantiles)  # [min, q25, median, q75, max]

    stats = NumericStats(
        min_value=row.min_value,
        max_value=row.max_value,
        avg_value=row.avg_value,
        median=q[2],
        iqr=q[3] - q[1],
    )

    return ColumnSample(
        meta=meta,
        values=[],              # no MinHash for numeric columns
        approx_distinct=row.approx_distinct,
        total_count=row.total_count,
        numeric_stats=stats,
    )
