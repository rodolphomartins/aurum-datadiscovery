# Aurum with BigQuery — Quick Start Guide

This guide covers running Aurum end-to-end against a BigQuery dataset using the Python `bq_profiler` module. The original Java `ddprofiler` is not required.

---

## What Aurum does

Aurum builds an **Enterprise Knowledge Graph (EKG)** from your data sources. The EKG is a network of columns connected by automatically discovered relationships:

| Relationship | How it is discovered | What it means |
|---|---|---|
| **Content similarity** | MinHash on sampled column values | Two columns contain overlapping values (e.g., `vendor_code` in table A and B) |
| **Schema similarity** | TF-IDF on column name tokens | Two columns share naming patterns (e.g., `order_id` and `order_code`) |
| **PK/FK** | Uniqueness ratio threshold | One column likely references another (e.g., a foreign key) |
| **Numeric similarity** | Distribution overlap (median, IQR) | Two numeric columns have similar value distributions |

Once built, the EKG can be queried with Aurum's Python API to answer questions like:
- "Find all columns joinable to `vendor_code` in this table"
- "Find tables that contain columns similar in name and content to these columns"
- "Find columns with high uniqueness ratio that could be primary keys"

---

## Architecture

The pipeline has three stages:

```
[BigQuery]
    │
    ▼
[bq_profiler]  ← Python, runs on your machine
    │  Reads schema from INFORMATION_SCHEMA (free)
    │  Samples column values via TABLESAMPLE (costs BQ scan)
    │  Computes MinHash, numeric stats, column name tokens
    │
    ▼
[Elasticsearch]  ← Docker, runs locally
    │  Stores column profiles (profile index)
    │  Stores sampled values for TF-IDF (text index)
    │
    ▼
[networkbuildercoordinator.py]  ← Python, runs on your machine
    │  Reads profiles from ES
    │  Builds EKG using LSH (MinHash), TF-IDF, numeric distribution overlap
    │
    ▼
[EKG pickles]  ← serialized NetworkX graph + similarity indexes
    │
    ▼
[Aurum API / Jupyter notebook]
```

---

## What you need to install

### Colima (for Elasticsearch only)

Colima is a free, open-source container runtime for macOS. It replaces Docker Desktop with no licensing restrictions and uses the same `docker` and `docker-compose` commands.

```bash
brew install colima docker docker-compose
```

Start Colima before any `docker-compose` commands:
```bash
colima start
```

Verify:
```bash
docker --version
```

Colima must be running whenever you use Elasticsearch. It does not start automatically on login — run `colima start` at the beginning of each work session, or add it to your shell profile.

### Python environment (venv required)

The Python components (`bq_profiler`, `networkbuildercoordinator.py`, Aurum API) run on your machine — not inside Docker. A virtual environment is required to avoid dependency conflicts with Aurum's pinned packages.

Python 3.10 is required. If you use `pyenv`:

```bash
pyenv install 3.10
pyenv local 3.10      # writes .python-version — already present in this repo
```

Create and activate a virtual environment:
```bash
python3.10 -m venv .venv
source .venv/bin/activate          # macOS / Linux
# .venv\Scripts\activate           # Windows
```

Install dependencies using the lean requirements file (excludes web server, databases, RDF tools):
```bash
pip install -r requirements-lite.txt
pip install -e .
```

The `-e .` installs `bq_profiler` as an editable package so it can be imported from anywhere.

> **Note:** The original `requirements.txt` is kept for reference but contains many dependencies not needed for BigQuery usage (Flask, Django, uWSGI, PostgreSQL, MongoDB, Neo4j, etc.). Use `requirements-lite.txt` for all new setups.

### Google Cloud SDK (for BigQuery access)

Install the gcloud CLI: https://cloud.google.com/sdk/docs/install

Authenticate with application default credentials:
```bash
gcloud auth application-default login
```

This writes credentials to `~/.config/gcloud/application_default_credentials.json`, which the BigQuery Python client picks up automatically. No service account JSON file needed for local development.

Verify BQ access:
```bash
python -c "from google.cloud import bigquery; print(bigquery.Client(project='YOUR_PROJECT').project)"
```

---

## Directory structure

```
aurum-datadiscovery/          ← this repo (your fork)
  bq_profiler/                ← Python profiler for BigQuery
  data/
    elasticsearch/            ← ES data volume (auto-created by Docker)
    models/                   ← EKG output (pickles written by networkbuildercoordinator.py)
  .venv/                      ← Python virtual environment (gitignored)

your-experiments-repo/        ← separate repo (can be private)
  configs/
    my_dataset.yaml           ← BQ dataset config (contains project/dataset/table names)
  run_profiler.py             ← thin runner (calls bq_profiler.cli)
  notebooks/                  ← Jupyter notebooks for EKG queries
```

Keep your config files (which reference your dataset names) in a separate repository if the dataset is proprietary. The `bq_profiler` code in this repo contains no dataset-specific information.

---

## Step-by-step: running Aurum on your BigQuery dataset

### Step 1 — Start Elasticsearch

```bash
colima start   # if not already running
docker-compose up -d elasticsearch
```

Elasticsearch starts at `http://localhost:9200`. Verify:
```bash
curl http://localhost:9200
```

You should see a JSON response with `"tagline": "You Know, for Search"`.

The `data/elasticsearch/` directory persists your indexed profiles across restarts. To start fresh, stop ES and delete that directory:
```bash
docker-compose down
rm -rf data/elasticsearch
docker-compose up -d elasticsearch
```

### Step 2 — Create your dataset config

Copy the example template:
```bash
cp bq_profiler/configs/example.yaml my_config.yaml
```

Edit `my_config.yaml`:
```yaml
bigquery:
  project: "your-gcp-project-id"
  dataset: "your_dataset"
  tables:
    - "table_one"
    - "table_two"
    - "table_three"

db_name: "your_dataset"   # used as a namespace in the EKG
sample_pct: 1.0           # percentage of rows to sample for MinHash (1% = 0.01 ... 100% = 100.0)

elasticsearch:
  host: "localhost"
  port: 9200
```

**On `sample_pct`:** This controls how much of each table is scanned for MinHash content signatures. BigQuery charges per bytes scanned. At `1.0` (1%), a 10 GB table costs ~$0.05. At `100.0` (full scan), $5.00. For initial exploration, `1.0` is sufficient. For the final experiment run, use `5.0` or `10.0` for better MinHash recall on rare values.

Start with a small set of tables (5–10) to validate the pipeline before profiling the full dataset.

### Step 3 — Run the profiler

With your virtual environment active:

```bash
python -m bq_profiler.cli --config my_config.yaml --dry_run
```

`--dry_run` computes profiles and prints them without writing to Elasticsearch. Use this to verify BQ connectivity and check uniqueness ratios before committing to a full run.

When satisfied:
```bash
python -m bq_profiler.cli --config my_config.yaml
```

Expected output:
```
Found 87 columns across 5 tables.
  T pandora__agg_orders.vendor_code ... done (unique_ratio=0.002, minhash_len=128)
  T pandora__agg_orders.order_code  ... done (unique_ratio=0.821, minhash_len=128)
  N pandora__agg_orders.gmv_local   ... done (min=0.00, max=9842.50, median=12.40, iqr=18.20)
  ...
Pushed 87 profiles to 'profile' index.
Pushed 61 text documents to 'text' index.
```

### Step 4 — Build the EKG

```bash
mkdir -p data/models
python networkbuildercoordinator.py --opath data/models
```

This reads all profiles from Elasticsearch and builds the EKG. It runs the following steps:
1. Schema skeleton — connects tables to their columns
2. Schema similarity — TF-IDF on column name tokens
3. Content similarity (MinHash) — finds columns with overlapping values
4. Numeric similarity — matches columns with similar distributions
5. PK/FK detection — uniqueness ratio threshold

Output:
```
Total skeleton: 0.3s
Total schema-sim: 1.2s
Total text-sig-sim (minhash): 4.7s
Total num-sig-sim: 2.1s
Total PKFK: 0.8s
Total time: 9.1s
DONE!
```

Pickled files are written to `data/models/`:
```
data/models/
  graph.pkl                 ← the EKG (NetworkX graph)
  schema_sim_index.pkl      ← LSH index for schema similarity queries
  content_sim_index.pkl     ← LSH index for content similarity queries
```

### Step 5 — Query the EKG

Start a Jupyter notebook:
```bash
jupyter notebook
```

Basic usage:
```python
from inputoutput import inputoutput as io
from knowledgerepr.fieldnetwork import FieldNetwork
import algebra

# Load the EKG
network = io.deserialize_network("data/models/")
schema_sim_index = io.deserialize_object("data/models/schema_sim_index.pkl")
content_sim_index = io.deserialize_object("data/models/content_sim_index.pkl")

# Find columns with content similar to a given column
drs = algebra.similar_content(network, content_sim_index, "your_table", "your_column", 0.5)
for hit in drs:
    print(hit.source_name, hit.field_name, hit.score)

# Find columns with similar names
drs = algebra.similar_schema(network, schema_sim_index, "vendor_code", 10)
for hit in drs:
    print(hit.source_name, hit.field_name, hit.score)

# Find joinable tables (PKFK edges)
drs = algebra.pkfk(network, "your_table", "your_column")
for hit in drs:
    print(hit.source_name, hit.field_name)
```

---

## Do I need Docker, a venv, or both?

| Component | Colima + docker-compose | venv |
|---|---|---|
| Elasticsearch | Required | Not applicable |
| `bq_profiler` | Not needed | Required |
| `networkbuildercoordinator.py` | Not needed | Required |
| Aurum API / Jupyter | Not needed | Required |

**Short answer:** Run Elasticsearch via Colima, run everything else in a Python venv on your machine. Docker Desktop is not required — Colima is a free drop-in replacement with no licensing restrictions.

The original `docker-compose.yml` includes containers for the Java profiler and networkbuilder. Ignore those — they are replaced by the Python profiler and the host-based Python networkbuilder.

---

## Troubleshooting

**ES connection refused**
```
ConnectionError: Connection refused localhost:9200
```
Either Colima is not running (`colima start`) or Elasticsearch is not running (`docker-compose up -d elasticsearch`). Wait 10–15 seconds after starting ES before retrying.

**BQ permission denied**
```
google.api_core.exceptions.Forbidden: 403 Access Denied
```
Your GCP credentials do not have BigQuery read access on the target project. Run `gcloud auth application-default login` again, or check IAM permissions for your account.

**MinHash produces no content similarity edges**
The `sample_pct` may be too low for sparse columns, or the similarity threshold in `config.py` (`join_overlap_th = 0.4`) is too high. Try increasing `sample_pct` to `5.0` or lowering `join_overlap_th` to `0.2`.

**`networkbuildercoordinator.py` fails with ES scroll error**
Elasticsearch version mismatch. This repo requires ES 6.x (the version pinned in `docker-compose.yml`). Do not upgrade the ES Docker image.

---

## Re-profiling and incremental updates

Each call to `bq_profiler.cli` overwrites existing profiles for the same columns (ES `index` operation is idempotent by document ID). To add new tables, add them to your config and re-run the profiler — existing profiles are preserved.

After any profiling change, re-run `networkbuildercoordinator.py` to rebuild the EKG. The pickles are overwritten in place.
