# Stargazers Pipeline

An ELT pipeline that tracks GitHub stars across major open-source data tooling repositories. Ingests raw stargazer data from the GitHub API, transforms it through a layered dbt model, and orchestrates everything with Dagster on a daily schedule.

---

## What it does

Tracks who stars each of these repos and when:

| Repository | Notes |
|---|---|
| `dbt-labs/dbt-core` | |
| `apache/airflow` | GitHub API caps pagination at 400 pages (~40k records) |
| `dagster-io/dagster` | |
| `duckdb/duckdb` | |
| `dlt-hub/dlt` | |

Each run fetches only new stars since the last load (incremental by `starred_at`), loads them into DuckDB via dlt, then runs dbt to rebuild the transformation layers.

---

## Architecture

```
GitHub API
    │
    ▼
ingestion/extract_github_stargazers.py   (dlt → DuckDB)
    │
    ▼
DuckDB: github_raw.stargazers            (raw layer)
    │
    ▼
transform/models/base/                   (rename, cast, load_dt)
    │
    ▼
transform/models/intermediate/           (filter bots, add temporal dims, incremental)
    │
    ▼
transform/models/mart/                   (user-level aggregations)
    │
    ▼
Dagster UI: stargazers_daily @ 6 AM UTC
```

---

## Stack

- **Ingestion:** [dlt](https://dlthub.com/) — handles schema inference, staging, and loading into DuckDB
- **Warehouse:** [DuckDB](https://duckdb.org/) — local, file-based (`stargazers.duckdb`)
- **Transformation:** [dbt-core](https://www.getdbt.com/) with the `dbt-duckdb` adapter
- **Orchestration:** [Dagster](https://dagster.io/) — sequential job with a daily schedule

---

## Data model

### `github_raw.stargazers`
Raw records loaded by dlt. One row per (user, repo) star event.

| Column | Type | Description |
|---|---|---|
| `login` | varchar | GitHub username |
| `user_id` | integer | GitHub permanent numeric ID |
| `user_type` | varchar | `'User'` or `'Bot'` |
| `repo_name` | varchar | `owner/repo` format |
| `starred_at` | timestamp | When the star was created |

### `base_stargazers.base_stargazers`
Minimal cleaning pass. Renames `login` → `username`, adds `load_dt`.

### `int_stargazers.int_stargazers`
Filters out bots, adds a surrogate key (`MD5(user_id || repo_name)`), and adds `starred_month` / `starred_year` for time-series analysis. Incremental materialization.

### `mart_stargazers.mart_stargazers`
User-level aggregation: how many repos each person starred, their first and last star timestamps, and activity by month.

---

## Setup

### Prerequisites
- Python 3.12
- `GITHUB_TOKEN` environment variable (needs `public_repo` scope)

### Install

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Run locally

```bash
source .venv/bin/activate
dagster dev -f orchestration/definitions.py
```

Open [http://localhost:3000](http://localhost:3000) and launch the `stargazers_daily` job.

---

## Project structure

```
stargazers-pipeline/
├── ingestion/
│   └── extract_github_stargazers.py   # GitHub API fetch + dlt pipeline
├── transform/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── base/
│       ├── intermediate/
│       └── mart/
├── orchestration/
│   └── definitions.py                 # Dagster job + schedule
├── validation/
│   └── test_fetch_results.py          # Quick row count checks
└── stargazers.duckdb                  # Local DuckDB file (gitignored)
```

---

## Future improvements

### 1. Handle the apache/airflow pagination cap
GitHub's API hard-caps pagination at 400 pages (~40,000 records). `apache/airflow` has 44k+ stars, so roughly 4,000 records are permanently unreachable via the REST API.

**Future improvement:** Use the [GitHub Archive](https://www.gharchive.org/) dataset (available on BigQuery) for a full historical backfill of large repos. The incremental API pipeline can then pick up from the point where the archive ends. Alternatively, use the GitHub GraphQL API which has better pagination support for large result sets.

### 2. Separate full-refresh and incremental pipelines
There is no way to do a clean full reload without manually dropping schemas. Any schema change or data corruption requires manual intervention.

**Future improvement:** Add a second Dagster job (`stargazers_full_refresh`) that drops and reloads all raw data from scratch — useful for backfills, schema migrations, or recovering from a corrupted state. The daily job stays incremental; the full-refresh job is triggered manually or on a slow cadence (e.g. monthly).

### 3. Move off a local DuckDB file to a shared warehouse
A `.duckdb` file on disk does not work across multiple machines or CI environments and cannot be queried concurrently.

**Future improvement:** Migrate to [MotherDuck](https://motherduck.com/) (managed DuckDB) or a cloud warehouse (BigQuery, Snowflake, Redshift). Update the dbt profile and dlt destination accordingly.

### 4. Secrets management
`GITHUB_TOKEN` is set via environment variable with no enforcement or rotation strategy.

**Future improvement:** Store secrets in a proper secrets manager (AWS Secrets Manager, GCP Secret Manager, or Dagster's built-in secret resources). In dev, load from a `.env` file via `python-dotenv`. Add pre-commit hooks to catch accidental token exposure.

### 5. Idempotent ingestion
The `merge` write disposition in dlt fails if the destination table doesn't exist (a known partial-load edge case). The manual `last_loaded_at` cursor already prevents duplicates on a plain append.

**Future improvement:** Switch to `write_disposition="append"` — the cursor-based incremental logic already ensures no duplicates are loaded. Add a deduplication step in the dbt intermediate model as a safety net.

### 6. dbt incremental model robustness
The intermediate model uses `MAX(starred_at)` as its cursor. Any late-arriving record with an old timestamp will be silently dropped.

**Future improvement:** Use a lookback window in the incremental filter (e.g. re-process the last 7 days) rather than a strict `>` comparison. Schedule a monthly `dbt build --full-refresh` run via the full-refresh Dagster job.

### 7. Observability and alerting
There are no alerts when the pipeline fails or when source data goes stale beyond the dbt freshness SLAs defined in `sources.yml`.

**Future improvement:** Configure Dagster alerts (Slack or email) on job failure. Expose dbt test results as Dagster asset checks so failures surface in the UI. Add a post-transformation step that runs `dbt test` and fails the Dagster run if any test does not pass.
