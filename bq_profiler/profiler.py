"""
Profiler — pure logic, no I/O.
Takes ColumnSample and produces ColumnProfile.
ColumnProfile field names match Aurum's Elasticsearch profile index exactly
(see ddprofiler/src/main/java/core/WorkerTaskResult.java and
 ddprofiler/src/main/java/store/NativeElasticStore.java).
"""

import binascii
import re
from dataclasses import dataclass
from typing import List, Optional

from datasketch import MinHash

from bq_profiler.connector import ColumnSample

from bq_profiler.connector import is_text_type  # single definition, imported here


@dataclass
class ColumnProfile:
    # Exact field names from WorkerTaskResult / NativeElasticStore — do not rename
    id: str               # CRC32(dbName + sourceName + columnName) as string
    dbName: str
    path: str             # BQ: "project.dataset.table"
    sourceName: str       # table name
    columnName: str
    dataType: str         # "T" (text) or "N" (numeric)
    totalValues: int
    uniqueValues: int
    uniquenessRatio: float
    minhash: List[int]    # empty list for numeric columns
    tokens: List[str]     # tokenized column name for schema similarity
    # Numeric stats (zero for text columns, matches Java default)
    minValue: float = 0.0
    maxValue: float = 0.0
    avgValue: float = 0.0
    median: float = 0.0
    iqr: float = 0.0
    # Optional enrichment
    description: Optional[str] = None


def compute_nid(db_name: str, source_name: str, column_name: str) -> str:
    raw = db_name + source_name + column_name
    return str(binascii.crc32(raw.encode("utf-8")))


def compute_minhash(values: List[str], num_perm: int = 128) -> List[int]:
    m = MinHash(num_perm=num_perm)
    for v in values:
        m.update(v.encode("utf-8"))
    return [int(x) for x in m.hashvalues]


def tokenize_column_name(column_name: str) -> List[str]:
    """
    Split column name into tokens for schema similarity (TF-IDF).
    'hurrier_order_id' -> ['hurrier', 'order', 'id']
    'gmvEurF30d'       -> ['gmv', 'eur', 'f30d']
    """
    parts = re.split(r"[_\s]+", column_name)
    tokens = []
    for part in parts:
        sub = re.sub(r"([a-z])([A-Z])", r"\1_\2", part).split("_")
        tokens.extend(sub)
    return [t.lower() for t in tokens if len(t) > 1]


def profile(sample: ColumnSample, db_name: str, num_perm: int = 128) -> ColumnProfile:
    meta = sample.meta
    nid = compute_nid(db_name, meta.table, meta.column)
    data_type = "T" if is_text_type(meta.data_type) else "N"
    uniqueness = (
        sample.approx_distinct / sample.total_count
        if sample.total_count > 0 else 0.0
    )
    minhash = (
        compute_minhash(sample.values, num_perm)
        if data_type == "T" and sample.values else []
    )
    tokens = tokenize_column_name(meta.column)
    path = f"{meta.project}.{meta.dataset}.{meta.table}"

    p = ColumnProfile(
        id=nid,
        dbName=db_name,
        path=path,
        sourceName=meta.table,
        columnName=meta.column,
        dataType=data_type,
        totalValues=sample.total_count,
        uniqueValues=sample.approx_distinct,
        uniquenessRatio=uniqueness,
        minhash=minhash,
        tokens=tokens,
        description=meta.description,
    )

    if sample.numeric_stats is not None:
        ns = sample.numeric_stats
        p.minValue = ns.min_value
        p.maxValue = ns.max_value
        p.avgValue = ns.avg_value
        p.median = ns.median
        p.iqr = ns.iqr

    return p
