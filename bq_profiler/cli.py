"""
Entry point: reads a YAML config and runs the full profiling pipeline.

Usage:
    python -m bq_profiler.cli --config configs/my_dataset.yaml
    python -m bq_profiler.cli --config configs/my_dataset.yaml --sample_pct 0.5 --dry_run
"""

import argparse
import yaml

from google.cloud import bigquery
from elasticsearch import Elasticsearch

from bq_profiler.connector import get_columns, sample_column, sample_numeric_column, is_text_type
from bq_profiler.profiler import profile
from bq_profiler.push_to_es import push_profiles, push_text_index


def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


_LOCALHOST_ALIASES = {"localhost", "127.0.0.1", "::1"}


def run(config_path: str, sample_pct: float = None, dry_run: bool = False):
    cfg = load_config(config_path)

    billing_project = cfg["bigquery"]["project"]
    data_project = cfg["bigquery"].get("data_project", billing_project)
    dataset = cfg["bigquery"]["dataset"]
    tables = cfg["bigquery"]["tables"]
    db_name = cfg.get("db_name", dataset)
    effective_sample_pct = sample_pct if sample_pct is not None else cfg.get("sample_pct", 1.0)

    if not (0 < effective_sample_pct <= 100):
        raise ValueError(f"sample_pct must be between 0 (exclusive) and 100 (inclusive), got {effective_sample_pct}")

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
    print(f"Found {len(columns)} columns across {len(tables)} tables.")

    profiles = []
    samples_by_id = {}  # profile.id -> List[str] values, for text index

    for col_meta in columns:
        is_text = is_text_type(col_meta.data_type)
        print(f"  {'T' if is_text else 'N'} {col_meta.table}.{col_meta.column} ...", end=" ")

        if is_text:
            sample = sample_column(bq_client, col_meta, sample_pct=effective_sample_pct)
        else:
            sample = sample_numeric_column(bq_client, col_meta)

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
    parser.add_argument("--sample_pct", type=float, default=None, help="Override sample percentage (e.g. 1.0)")
    parser.add_argument("--dry_run", action="store_true", help="Compute profiles but do not push to ES")
    args = parser.parse_args()
    run(args.config, sample_pct=args.sample_pct, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
