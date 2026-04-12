"""
bq_profiler — Python replacement for Aurum's Java ddprofiler, targeting BigQuery.

Components:
  connector.py  — fetch schema and content samples from BigQuery
  profiler.py   — compute MinHash, uniqueness ratio, and column name tokens
  push_to_es.py — push ColumnProfile documents to Elasticsearch
  cli.py        — entry point: bq-profile --config <path>
"""
