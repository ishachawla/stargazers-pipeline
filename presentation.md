# Stargazers Pipeline
### Tracking GitHub Star Growth Across the OSS Data Stack

An ELT pipeline that ingests GitHub stargazer data from 5 major open-source repositories, transforms it into a layered dbt data model, and orchestrates everything on a daily schedule.

**Stack:** `dlt` + `DuckDB` + `dbt-core` + `Dagster`

---

## 1. Project Structure

```
stargazers-pipeline/
├── ingestion/
│   └── extract_github_stargazers.py   # GitHub API → DuckDB via dlt
├── transform/
│   ├── dbt_project.yml                # dbt config + materialization settings
│   ├── profiles.yml                   # DuckDB connection
│   └── models/
│       ├── sources.yml                # Source definition + freshness SLAs
│       ├── base/                      # Minimal renaming, add load_dt
│       ├── intermediate/              # Filter bots, surrogate key, incremental
│       └── mart/                      # User-level aggregations
├── orchestration/
│   └── definitions.py                 # Dagster job + daily schedule
└── validation/
    └── test_fetch_results.py          # Quick DuckDB row-count checks
```

**ELT flow:**

```
GitHub API
    │
    ▼
dlt (schema inference + staging)
    │
    ▼
DuckDB: github_raw.stargazers          ← raw, untouched
    │
    ▼
base_stargazers                        ← rename, cast, load_dt
    │
    ▼
int_stargazers                         ← filter bots, surrogate key, temporal dims
    │
    ▼
mart_stargazers                        ← user-level aggregations
    │
    ▼
Dagster: runs daily at 6 AM UTC
```

**Repos tracked:**

| Repository | Notes |
|---|---|
| `dbt-labs/dbt-core` | |
| `apache/airflow` | GitHub API caps at 400 pages (~40k records); repo has 44k+ stars |
| `dagster-io/dagster` | |
| `duckdb/duckdb` | |
| `dlt-hub/dlt` | |

**Talking points:**
- This is a classic ELT pattern — land raw first, transform in layers inside the warehouse, orchestrate end-to-end
- The five repos were chosen deliberately: all are major tools in the modern data stack, so tracking who stars them tells you something about where the analytics engineering community is paying attention
- Everything lives in four Python files and three SQL models — the surface area is intentionally small
- The Dagster UI lets you see every run, every log line, and every step in the sequence without SSH-ing into a server

---

## 2. Data Model

### `github_raw.stargazers` — raw dlt output
One row per (user, repo) star event. Exactly as received from the GitHub API.

| Column | Type | Description |
|---|---|---|
| `login` | varchar | GitHub username |
| `user_id` | integer | GitHub permanent numeric ID |
| `user_type` | varchar | `'User'` or `'Bot'` |
| `repo_name` | varchar | `owner/repo` format |
| `starred_at` | timestamp | When the star was created |

### `base_stargazers` — minimal cleaning
Renames `login` → `username`, adds `load_dt`. No filtering, no business logic. Preserves the full raw record for reuse.

### `int_stargazers` — enrichment + deduplication

```sql
{{
    config(
        materialized='incremental',
        unique_key='stargazer_pk'
    )
}}

{% set max_ts = '1970-01-01' %}
{% if is_incremental() %}
    {% set max_ts = run_query("SELECT COALESCE(MAX(starred_at), '1970-01-01') FROM " ~ this).columns[0].values()[0] %}
{% endif %}

SELECT
    {{ dbt_utils.generate_surrogate_key(['user_id', 'repo_name']) }} AS stargazer_pk,
    s.user_id,
    s.username,
    s.repo_name,
    s.starred_at,
    DATE_TRUNC('month', s.starred_at) AS starred_month,
    DATE_TRUNC('year', s.starred_at)  AS starred_year
FROM {{ ref('base_stargazers') }} AS s
WHERE s.user_type = 'User'
AND s.starred_at > '{{ max_ts }}'
```

### `mart_stargazers` — user-level analytics
Groups by user, computes `repos_starred`, `first_starred_at`, `last_starred_at`, `first_starred_month`, `last_starred_month`. One row per stargazer.

**Talking points:**
- The raw layer is intentionally untouched — no filtering, no renaming. If a downstream consumer wants raw bot activity or wants to apply a different business rule, they can go back to the raw layer without losing anything
- The base layer is just housekeeping — rename `login` to `username` to be more descriptive, add `load_dt` for lineage. One SQL file, very boring, very important
- The surrogate key in intermediate uses `user_id` not `login` — GitHub usernames are mutable, someone can rename themselves and suddenly your primary key breaks. Numeric IDs are permanent
- The intermediate model is where the interesting decisions happen: bot filter, temporal dimensions, incremental materialization
- The mart is the answer to the question "who are our stargazers and how engaged are they?" — one row per person, their first star, their last star, how many repos they've starred across our tracked set

---

## 3. How Did You Decide Which Tools to Use?

| Tool | Role | Why | Alternative Considered |
|---|---|---|---|
| **dlt** | Ingestion | Schema inference means no DDL to write; `merge` disposition handles upserts; has a native DuckDB destination | Raw `requests` + manual schema management — more control, much more boilerplate |
| **DuckDB** | Warehouse | Zero infrastructure — just a file; full SQL analytical functions; integrates natively with dlt and dbt | SQLite (no window functions, no analytical SQL), Postgres (over-engineered for a local project) |
| **dbt-core** | Transformation | Layered DAG with lineage, schema tests, source freshness, incremental materialization — all built-in | Raw SQL scripts — no lineage, no tests, no incremental logic out of the box |
| **Dagster** | Orchestration | Python-native, explicit sequential op dependencies, built-in UI, runs locally without infrastructure | Cron + shell script (no observability, no retry logic), Airflow (too heavy for a single-machine project) |

**Talking points:**
- The common theme across every tool choice: minimize infrastructure overhead while keeping engineering discipline — tests, lineage, observability
- dlt was new to me. The selling point is that you hand it a Python list of records and it handles schema inference, creates the table, and manages staging — no DDL, no MERGE statement to write yourself
- DuckDB was the obvious choice for a local-first project. It's a file, it runs in-process, it speaks full analytical SQL, and both dlt and dbt have native adapters for it. No Postgres server to spin up, no credentials to manage
- I considered Airflow for orchestration because it's the industry default, but it's genuinely overkill here — it needs a metadata database, a scheduler process, a web server. Dagster runs with a single command and gives you the same visual DAG and run history
- Worth noting: some of these choices would change in production — DuckDB specifically doesn't support concurrent writes or multiple machines. That's a known tradeoff made consciously for this scope

---

## 4. Architecture Tradeoffs

### 1. Incremental ingestion via `MAX(starred_at)` cursor

```python
def get_last_loaded_at():
    conn = duckdb.connect('stargazers.duckdb')
    result = conn.execute(
        "SELECT MAX(starred_at) FROM github_raw.stargazers"
    ).fetchone()
    return result[0]
```

**Why:** Simple, API-friendly, and avoids re-fetching thousands of historical records on every run. GitHub returns stars newest-first, so we can stop paginating the moment we hit a record older than the cursor.

**Tradeoff:** If a star is backdated — e.g., a record arrives with an old `starred_at` timestamp — the cursor will skip it permanently. A lookback window (re-process the last 7 days) would address this at the cost of some extra API calls.

**Talking points:**
- GitHub returns stars in reverse chronological order — newest first. That's what makes this cursor approach efficient: the moment we see a `starred_at` older than our last load, we stop paginating entirely instead of scanning to the end
- On the first run, `get_last_loaded_at()` returns None because the table doesn't exist yet, and we fetch everything. On every subsequent run, we only fetch what's new — typically a handful of records per repo per day
- The risk I didn't fully solve: if GitHub ever backdates a star record (e.g., a data correction on their side), our cursor would skip it forever. A 7-day lookback window would fix this but add API calls

---

### 2. dlt `merge` disposition with composite primary key

```python
pipeline.run(
    records,
    table_name="stargazers",
    write_disposition="merge",
    primary_key=["login", "repo_name"]
)
```

**Why:** Prevents duplicate (user, repo) pairs if the same record appears across pipeline runs.

**Tradeoff:** dlt's merge requires the destination table to already exist. On the very first run, the table is being created for the first time, which can cause the merge to fail. Since the cursor already prevents duplicates at the extraction layer, switching to `write_disposition="append"` would be simpler and more robust.

**Talking points:**
- The composite primary key `[login, repo_name]` means: one row per person per repo. If the same (user, repo) pair appears across two runs, dlt deduplicates it
- In practice, the cursor already prevents this — we only fetch records newer than the last run, so duplicates shouldn't appear in the first place. The merge is a belt-and-suspenders safety net
- The painful discovery: `merge` in dlt requires the destination table to already exist before it can do the DELETE + INSERT. On a fresh database, the table doesn't exist yet, and the pipeline errors out. I had to drop the schema and restart to recover
- The fix I'd make: switch to `append` and let dbt handle deduplication in the intermediate model via the `unique_key` config. Simpler and avoids the first-run edge case entirely

---

### 3. Bot filtering at intermediate, not raw

**Why:** The raw layer (`github_raw.stargazers`) is preserved exactly as received from the source. No filtering, no transformations. This means it can be reused for other purposes — auditing, bot behavior analysis, or a different business definition of what counts as a "star."

The filter (`WHERE user_type = 'User'`) is a business rule that lives explicitly in the intermediate model where it is visible, documented, and testable.

**Talking points:**
- This is a principle I'd apply to any pipeline: the raw layer is a historical record of what the source sent us. We should never destroy information at the raw layer because we might need it later for something we haven't thought of yet
- "Bots don't count as stargazers" is a business decision, not a data quality issue. It belongs in the transformation layer where it's explicit, documented, and could be changed or overridden without touching the raw data
- In practice, there aren't many bots in the dataset — but having the filter in the intermediate schema.yml with an `accepted_values` test means if a new `user_type` value ever appears from the GitHub API, we'll catch it

---

### 4. Dagster `In(Nothing)` for sequential op dependency

```python
@op(ins={"after_extract": In(Nothing)})
def run_dbt(context, dbt: DbtCliResource) -> Nothing:
```

**Why:** Both ops write to DuckDB as a side effect — they don't pass data between each other. `In(Nothing)` is Dagster's idiom for "this op must wait for that op to finish, even though no data is passed." It enforces the run order without fabricating a return value.

**Tradeoff:** Dagster has no visibility into what was actually loaded in step 1. If `extract_stargazers` runs but loads zero records (nothing new), `run_dbt` still runs and rebuilds all models unnecessarily. A future improvement would be to return a row count and skip dbt if nothing was loaded.

**Talking points:**
- The typical Dagster mental model is: op A returns a dataframe, op B consumes it. That works for transformation pipelines. It doesn't work here because both ops just write to DuckDB — there's no data being passed between them
- `In(Nothing)` is Dagster's way of saying "I care about the order, not the output." The `Nothing` type is just a signal: op B cannot start until op A completes successfully
- The current limitation: if the ingestion step runs and finds zero new records — because nothing was starred since yesterday — dbt still runs a full build. It's wasted compute. A smarter version would return a row count from extraction and skip dbt if it's zero

---

## 5. Testing

### dbt schema tests
Defined in `schema.yml` files. Run automatically as part of `dbt build`.

| Test | Column | Model |
|---|---|---|
| `not_null` | `user_id`, `repo_name` | `base_stargazers` |
| `not_null` | `user_id`, `starred_at`, `stargazer_pk` | `int_stargazers` |
| `accepted_values: ['User', 'Bot']` | `user_type` | `base_stargazers` |
| `unique` + `not_null` | `stargazer_pk` | `int_stargazers` |

### dbt source freshness
Defined in `sources.yml`. Checks that new data is actually flowing through the pipeline.

```yaml
freshness:
  warn_after: {count: 25, period: hour}   # warn if older than 25hrs
  error_after: {count: 49, period: hour}  # error if older than 49hrs
loaded_at_field: starred_at
```

25hr/49hr thresholds allow for one missed daily run before escalating to an error — so a weekend or holiday doesn't immediately page someone.

### Validation script (`validation/test_fetch_results.py`)
Manual row-count check against the raw table after a run. Not automated — used as a quick sanity check after the first load or after a schema change.

**Honest gaps:** There are no automated tests for the ingestion layer. There is no mocked GitHub API for unit testing `fetch_page()` or `get_stargazers()`. These are the two weakest points in the testing surface.

**Talking points:**
- dbt schema tests run as part of `dbt build` — they're not a separate step. If `user_type` suddenly returns a value we haven't seen before, the `accepted_values` test catches it before it propagates to the mart
- The `unique` test on `stargazer_pk` is the most important one. It tells us the surrogate key is doing its job — if we ever see a duplicate (user, repo) pair get through, the test will fail and the run will fail
- The freshness thresholds are deliberately generous: 25 hours for a warning, 49 hours for an error. The pipeline runs daily so anything older than 25 hours means we missed at least one run. 49 hours gives a one-run buffer before we escalate to an actual error — useful for weekends or holidays
- The honest gap: I have no automated tests for the Python ingestion code. The rate limit handling and pagination cap logic are only tested by running the actual pipeline. Adding mocked HTTP tests for `fetch_page()` would be the highest-value improvement to the test coverage

---

## 6. How Would You Productionize This?

### 1. Replace local DuckDB with a shared warehouse

**Before:** `stargazers.duckdb` — a file on a single machine, no concurrent reads, not accessible from CI or a second developer's machine.

**After:** [MotherDuck](https://motherduck.com/) (managed DuckDB, requires minimal config change) or a cloud warehouse like BigQuery or Snowflake. Update `profiles.yml` and the dlt destination — the dbt models and Dagster job are unchanged.

### 2. Add a full-refresh Dagster job

**Before:** No way to do a clean reload without manually dropping schemas from a DuckDB shell.

**After:** A second job `stargazers_full_refresh` that drops and reloads all raw data. Triggered manually or on a slow cadence (monthly). The daily `stargazers_daily` job stays incremental. This also enables safe schema migrations.

### 3. Secrets management

**Before:** `GITHUB_TOKEN` is a bare environment variable with no rotation or enforcement strategy.

**After:** Store in AWS Secrets Manager, GCP Secret Manager, or Dagster's built-in secret resources. In development, use `python-dotenv` with a `.env` file. Add a pre-commit hook to catch accidental token commits.

### 4. Switch dlt to `write_disposition="append"`

**Before:** `merge` fails on the first run if the destination table does not exist yet.

**After:** `append` + rely on the cursor-based incremental logic that already exists. Add a deduplication step in the dbt intermediate model as a safety net. Simpler and more robust.

**Talking points:**
- The biggest single change for production is the warehouse. DuckDB as a local file doesn't support concurrent writes — if two processes try to write at the same time, one will block. MotherDuck is the path of least resistance because the dbt and dlt configs barely need to change
- The full-refresh job is something I'd want on day one in production. Right now if someone adds a column to the raw schema, there's no clean way to backfill it other than manually dropping tables. A dedicated full-refresh job makes schema migrations a one-button operation
- Secrets management is a day-one requirement for any shared environment. The current `GITHUB_TOKEN` env var is fine locally but it's one copy-paste away from being committed to the repo. A pre-commit hook that scans for tokens is cheap insurance
- The `append` change is also a day-one fix — the `merge` first-run failure is a known bug in the current setup that I had to work around manually during development

---

## 7. Monitoring and Alerting

**Current state:** dbt source freshness SLAs are configured. That is the only active monitoring. There are no Slack or email alerts, and dbt test failures do not automatically fail the Dagster run.

### Three concrete improvements

**1. Dagster failure alerts**
Configure a `RunStatusSensorDefinition` in `definitions.py` to send a Slack or email message when any run of `stargazers_daily` fails. This is a config change, not a code change.

**2. Surface dbt test failures to Dagster**
The current `run_dbt` op streams dbt output and checks `process.returncode`. This already raises on failure — but it should be verified that `dbt build` returns a non-zero exit code when schema tests fail (it does by default). Confirm this is wired correctly so test failures show up as Dagster run failures, not silent warnings.

**3. Make freshness a Dagster step**
Add a third op `run_dbt_freshness` after `run_dbt` that runs `dbt source freshness --select source:github_raw`. If freshness fails, the Dagster run fails, which triggers the alert from improvement #1. This makes the freshness SLA a first-class pipeline gate, not just a passive check.

**Talking points:**
- Right now the pipeline fails silently. If the Dagster job fails at 6 AM, nobody finds out until someone opens the UI and looks. That's not acceptable in production
- The freshness SLAs in `sources.yml` are good, but they're passive — they only fire if someone runs `dbt source freshness` manually or as part of a build. Making freshness a dedicated Dagster op turns it into an active gate that blocks the run and can trigger an alert
- The Dagster sensor approach is the right abstraction: one sensor definition catches failures from any run of the job, regardless of which step failed. You don't have to instrument each op individually
- The monitoring gap I'd prioritize first: Slack alerts on job failure. It's a 20-line config change and it means you find out about failures before your stakeholders do

---

## 8. What Did I Add Beyond the Core Requirements?

### Bot filtering at intermediate (not raw)
The requirement was to load and transform stargazer data. Filtering at raw would have been simpler. Choosing to filter at intermediate — and document why — reflects the principle that raw data should be preserved exactly as received. The business rule belongs in the transformation layer where it is visible, versioned, and testable.

### Surrogate key using `user_id`, not `login`

```sql
{{ dbt_utils.generate_surrogate_key(['user_id', 'repo_name']) }} AS stargazer_pk
```

GitHub usernames (`login`) are mutable — users can rename themselves. `user_id` is GitHub's permanent numeric identifier and never changes. Using `user_id` in the surrogate key makes the primary key stable across renames. This is explicitly noted in the intermediate schema.yml: *"Github usernames - these can change over time."*

### dbt source freshness SLAs
Adding freshness monitoring to `sources.yml` was not required but makes the pipeline self-monitoring. A pipeline that silently produces stale data is worse than one that fails loudly. The 25hr/49hr thresholds are deliberate: tight enough to catch a missed run, loose enough to not alert on weekends.

### Incremental materialization at intermediate
The intermediate model could have been a table that rebuilds fully on every run. Making it incremental (`materialized='incremental'`) means it only processes records newer than `MAX(starred_at)`. As the dataset grows to hundreds of thousands of records, this avoids an increasingly expensive full scan on every daily run.

### Explicit rate limit + pagination cap handling
The GitHub API has two distinct failure modes in this pipeline:
1. **Rate limiting** — recoverable; wait until the reset window and retry
2. **Pagination cap on large repos** — unrecoverable; GitHub will not return more than 400 pages regardless of retry

Both are handled explicitly in `fetch_page()` with clear log messages. The alternative — a single generic error handler — would mask which failure mode occurred and make debugging harder.

**Talking points:**
- None of these were in the requirements — I added them because they're the difference between a pipeline that works once and a pipeline you can trust
- The `user_id` vs `login` decision came from thinking about what happens six months from now when a GitHub user renames themselves. If you built the key on `login`, every historical record for that person suddenly has the wrong key. Using `user_id` makes the surrogate key immune to that
- The two-mode error handling in `fetch_page()` is something I'm glad I added early. Rate limiting and pagination caps are completely different problems — one you retry, one you log and move on. A generic `except Exception` would have hidden which one we hit
- The freshness SLAs are cheap to add and give you a safety net that runs without any extra work. If the pipeline silently stops loading data, freshness will catch it within one cycle

---

## 9. What I Learned or Explored for the First Time

*(Pick whichever is genuinely true for you)*

### Option A — dlt as an ingestion tool
Before this project I had written ingestion pipelines by hand: raw HTTP requests, manual schema definitions, custom MERGE statements. dlt abstracts all of that. Schema inference means you don't write a `CREATE TABLE` statement. The `merge` write disposition handles upsert logic without writing SQL.

What I didn't expect: the abstraction introduces edge cases that aren't obvious. The `merge` disposition assumes the destination table already exists. On the first run it does not, and the pipeline fails at the load step with a catalog error. Understanding *why* required reading dlt's load execution flow, not just its documentation.

### Option B — Dagster's `In(Nothing)` pattern
My mental model of Dagster coming in was: ops return data, downstream ops consume it. That works well for data transformation pipelines. It does not work for pipelines where ops write to an external system as a side effect and don't return anything meaningful.

`In(Nothing)` is Dagster's solution: declare that an op depends on another op's *completion*, not its *output*. The type `Nothing` signals that the dependency is about ordering, not data flow. This is the correct pattern for orchestrating ELT steps against a shared database, but it is not the first thing you find in the Dagster documentation.

**Talking points:**
- The dlt edge case taught me that abstraction layers have failure modes that only appear in specific execution sequences — in this case, "table doesn't exist yet." The tool's happy path works beautifully; the first-run edge case required actually reading the source
- The `In(Nothing)` pattern was a conceptual shift. I kept trying to figure out how to pass data between the extraction and transformation steps, and the answer was: you don't. You pass a signal that says "I'm done." That's a cleaner separation of concerns once you see it
- Both of these are lessons about reading tools deeply, not just using their documented APIs

---

## 10. Known Limitations

Being direct about limitations is part of the work.

| Limitation | Impact | Path Forward |
|---|---|---|
| `apache/airflow` API pagination cap | ~4,000 of 44,000+ stars are permanently unreachable via REST | Use GitHub GraphQL API or ingest from [GitHub Archive](https://www.gharchive.org/) on BigQuery for historical backfill |
| No full-refresh mechanism | Schema changes or corrupted state require manual `DROP SCHEMA` | Add a `stargazers_full_refresh` Dagster job |
| Local DuckDB | Single machine, no concurrent reads, not CI-friendly | Migrate to MotherDuck or a cloud warehouse |
| dlt `merge` fails on first run | Pipeline errors on initial load unless schemas are pre-created | Switch to `append` disposition |
| `MAX(starred_at)` cursor drops late-arriving records | Any backdated star is permanently missed | Add a lookback window to reprocess the last N days |
| No ingestion-layer unit tests | API error handling is tested only by running the real pipeline | Add mocked HTTP tests for `fetch_page()` |

**Talking points:**
- Naming your own limitations before someone else does is a sign of engineering maturity — it means you've thought through your design, not just gotten it working
- The `apache/airflow` gap is a real data quality issue. We have about 91% of their star history. For trend analysis that's probably fine; for exact counts it matters. GitHub Archive on BigQuery would give us a complete backfill
- The local DuckDB limitation is the one most likely to bite in a real team setting. The moment a second person wants to query the data or a CI job wants to run, the file-based model breaks down
- I want to be clear about what this pipeline does well: for a daily incremental load of GitHub stars into a layered data model, it works reliably. The limitations are about scale and production hardening, not about the core logic

---

## Summary

A four-tool ELT pipeline — dlt, DuckDB, dbt, Dagster — that tracks GitHub stargazer activity across five major OSS repositories. Incremental at both the ingestion layer (cursor-based API pagination) and the transformation layer (dbt incremental materialization). Tested via dbt schema tests and source freshness SLAs. Orchestrated with explicit sequential dependencies and a daily 6 AM schedule.

The production path is well-defined: move to a cloud warehouse, add a full-refresh job, wire up alerting, and fix the dlt merge edge case. The core ELT logic and dbt model structure does not need to change.

**Talking points:**
- The thing I'd emphasize: this pipeline makes deliberate choices at every layer and I can explain the reason for each one. That matters more than the specific tools
- The incremental logic is consistent end-to-end — the same `MAX(starred_at)` cursor drives both the API extraction and the dbt intermediate model. There's no place where the two layers could drift out of sync
- If I had to pick one thing to do before taking this to production, it would be moving off local DuckDB. Everything else can be improved incrementally without taking the pipeline down

---

