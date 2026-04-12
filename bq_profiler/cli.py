"""
Entry point: reads a YAML config and runs the full profiling pipeline.

Usage:
    python -m bq_profiler.cli --config configs/my_dataset.yaml
    python -m bq_profiler.cli --config configs/my_dataset.yaml --sample_pct 0.5 --dry_run
"""

import argparse
import re
import yaml

from google.api_core.exceptions import BadRequest
from google.cloud import bigquery
from elasticsearch import Elasticsearch

from bq_profiler.connector import (
    ColumnMeta, get_columns, build_partition_map,
    sample_column, sample_numeric_column, is_text_type,
)
from bq_profiler.profiler import profile
from bq_profiler.push_to_es import push_profiles, push_text_index

_PARTITION_REQUIRED_RE = re.compile(r"without a filter over column\(s\) '([^']+)'")


def _fallback_partition_col(error: BadRequest, col_meta: ColumnMeta, columns: list) -> ColumnMeta | None:
    """
    When BQ rejects a query for a missing partition filter, extract the required
    column name from the error message and find its ColumnMeta in the already-fetched
    columns list — matched by (table == col_meta.table) to avoid cross-table collisions.

    Returns None if the column cannot be resolved (logged by caller).
    """
    m = _PARTITION_REQUIRED_RE.search(str(error))
    if not m:
        return None
    required_col = m.group(1)
    match = next(
        (c for c in columns if c.table == col_meta.table and c.column == required_col),
        None,
    )
    if match is None:
        # Column not in view's INFORMATION_SCHEMA (rare). Fall back to DATE as safe default.
        return ColumnMeta(
            project=col_meta.project, dataset=col_meta.dataset,
            table=col_meta.table, column=required_col,
            data_type="DATE", is_partitioning_column=True,
        )
    return ColumnMeta(
        project=match.project, dataset=match.dataset,
        table=match.table, column=match.column,
        data_type=match.data_type, is_partitioning_column=True,
    )


def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


_LOCALHOST_ALIASES = {"localhost", "127.0.0.1", "::1"}


def run(config_path: str, dry_run: bool = False):
    cfg = load_config(config_path)

    billing_project = cfg["bigquery"]["project"]
    data_project = cfg["bigquery"].get("data_project", billing_project)
    dataset = cfg["bigquery"]["dataset"]
    tables = cfg["bigquery"]["tables"]
    db_name = cfg.get("db_name", dataset)
    sample_limit = cfg.get("sample_limit", 100_000)

    es_host = cfg.get("elasticsearch", {}).get("host", "localhost")
    es_port = cfg.get("elasticsearch", {}).get("port", 9200)

    if es_host not in _LOCALHOST_ALIASES:
        raise ValueError(
            f"ES host '{es_host}' is not localhost. Pushing sampled production data to a "
            f"remote Elasticsearch instance is not allowed. Use localhost only."
        )

    # billing_project = where jobs run (needs bigquery.jobs.create)
    # data_project = where tables live (needs bigquery.dataViewer)
    bq_client = bigquery.Client(project=billing_project)
    es = Elasticsearch([{"host": es_host, "port": es_port}])

    columns = list(get_columns(bq_client, data_project, dataset, tables))
    partition_map = build_partition_map(columns)
    print(f"Found {len(columns)} columns across {len(tables)} tables.")
    if partition_map:
        print(f"Partition columns detected: { {t: c.column for t, c in partition_map.items()} }")

    profiles = []
    samples_by_id = {}  # profile.id -> List[str] values, for text index

    for col_meta in columns:
        is_text = is_text_type(col_meta.data_type)
        print(f"  {'T' if is_text else 'N'} {col_meta.table}.{col_meta.column} ...", end=" ")

        partition_col = partition_map.get(col_meta.table)
        try:
            if is_text:
                sample = sample_column(bq_client, col_meta, limit=sample_limit, partition_col=partition_col)
            else:
                sample = sample_numeric_column(bq_client, col_meta, partition_col=partition_col)
        except BadRequest as e:
            if "without a filter over column" not in str(e):
                raise
            fallback = _fallback_partition_col(e, col_meta, columns)
            if fallback is None:
                print(f"SKIP (partition filter required but column unresolvable: {e})")
                continue
            print(f"retrying with partition filter on '{fallback.column}' (view metadata gap) ... ", end="")
            partition_map[col_meta.table] = fallback  # cache: remaining columns skip the error path
            if is_text:
                sample = sample_column(bq_client, col_meta, limit=sample_limit, partition_col=fallback)
            else:
                sample = sample_numeric_column(bq_client, col_meta, partition_col=fallback)

        p = profile(sample, db_name=db_name)
        profiles.append(p)

        if is_text and sample.values:
            samples_by_id[p.id] = sample.values

        stats_str = (
            f"unique_ratio={p.uniquenessRatio:.3f}, minhash_len={len(p.minhash)}"
            if is_text else
            f"min={p.minValue:.2f}, max={p.maxValue:.2f}, median={p.median:.2f}, iqr={p.iqr:.2f}"
        )
        print(f"done ({stats_str})")

    if dry_run:
        print(f"\nDry run: {len(profiles)} profiles computed, not pushed to ES.")
        return profiles

    push_profiles(es, profiles)
    push_text_index(es, profiles, samples_by_id)
    print(f"\nPushed {len(profiles)} profiles to 'profile' index.")
    print(f"Pushed {len(samples_by_id)} text documents to 'text' index.")
    return profiles


def main():
    parser = argparse.ArgumentParser(description="BQ Profiler — Aurum Python profiler for BigQuery")
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    parser.add_argument("--dry_run", action="store_true", help="Compute profiles but do not push to ES")
    args = parser.parse_args()
    run(args.config, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
