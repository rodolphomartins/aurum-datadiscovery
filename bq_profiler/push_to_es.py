"""
Elasticsearch writer — pushes ColumnProfile documents to Aurum's indexes.

Two indexes (matching NativeElasticStore.java exactly):
  profile — one doc per column, holds MinHash + numeric stats + metadata
  text    — one doc per column, holds raw sampled values for TF-IDF schema similarity
"""

from typing import List

from elasticsearch import Elasticsearch

from bq_profiler.profiler import ColumnProfile


def push_profiles(
    es: Elasticsearch,
    profiles: List[ColumnProfile],
    index: str = "profile",
) -> None:
    """Push to 'profile' index — used by networkbuildercoordinator.py."""
    for p in profiles:
        doc = {
            "id": p.id,
            "dbName": p.dbName,
            "path": p.path,
            "sourceName": p.sourceName,
            "sourceNameNA": p.sourceName,       # exact-match field
            "columnName": p.columnName,
            "columnNameNA": p.columnName,       # exact-match field
            "dataType": p.dataType,
            "totalValues": p.totalValues,
            "uniqueValues": p.uniqueValues,
            "entities": "",                     # entity recognition not implemented
            "minhash": p.minhash if p.minhash else [-1],
            "minValue": p.minValue,
            "maxValue": p.maxValue,
            "avgValue": p.avgValue,
            "median": p.median,
            "iqr": p.iqr,
        }


def push_text_index(
    es: Elasticsearch,
    profiles: List[ColumnProfile],
    samples: dict,          # {profile.id: List[str]} — raw values per column
    index: str = "text",
) -> None:
    """
    Push to 'text' index — used by get_all_fields_text_signatures() for TF-IDF.
    One document per sampled value batch; field 'text' holds the array of values.
    Field names match NativeElasticStore.indexData() exactly.
    """
    for p in profiles:
        values = samples.get(p.id, [])
        if not values:
            continue
        doc = {
            "id": p.id,
            "dbName": p.dbName,
            "path": p.path,
            "sourceName": p.sourceName,
            "columnName": p.columnName,
            "columnNameSuggest": p.columnName,
            "text": values,
        }
        es.index(index=index, body=doc)
