import sys
from pathlib import Path
from dagster import (
    asset,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    AssetSelection,
)
from dagster_dbt import DbtCliResource, dbt_assets


# paths
ROOT = Path(__file__).parent.parent
DBT_DIR = ROOT / "transform"

# dbt connection
dbt_resource = DbtCliResource(
    project_dir=str(DBT_DIR),
    profiles_dir=str(DBT_DIR),
)

# asset 1 - extraction
@asset(
    group_name="ingestion",
    config_schema={"full_refresh": bool}
)
def load_stargazers_raw():
    full_refresh = context.op_config["full_refresh"]

    if full_refresh:
        import duckdb
        conn = duckdb.connect(str(ROOT / "stargazers.duckdb"))
        conn.execute("DROP TABLE IF EXISTS github_raw.stargazers")
        conn.close()
        last_loaded_at = None
        context.log.info("Full refresh — existing data dropped")
    else:
        if str(ROOT) not in sys.path:
            sys.path.insert(0, str(ROOT))
        from ingestion.extract_github_stargazers import get_last_loaded_at
        last_loaded_at = get_last_loaded_at()
        context.log.info(f"Incremental — fetching stars newer than {last_loaded_at}")

    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from ingestion.extract_github_stargazers import run_pipeline
    run_pipeline(last_loaded_at)

# assets 2 + 3 - dbt models
@dbt_assets(manifest=DBT_DIR / "target" / "manifest.json")
def dbt_transformed_stargazers(context, dbt: DbtCliResource,load_stargazers_raw):
    # step 1: check source freshness
    context.log.info("Step 1/4 — Checking source freshness...")
    yield from dbt.cli(
        ["source", "freshness", "--profiles-dir", str(DBT_DIR)],
        context=context
    ).stream()

    # step 2: build base layer
    context.log.info("Step 2/4 — Building base layer...")
    yield from dbt.cli(
        [
            "build",
            "--select", "tag:base",
            "--profiles-dir", str(DBT_DIR)
        ],
        context=context
    ).stream()
    # step 3: build intermediate layer
    context.log.info("Step 3/4 — Building intermediate layer...")
    yield from dbt.cli(
        [
            "build",
            "--select", "tag:intermediate",
            "--profiles-dir", str(DBT_DIR)
        ],
        context=context
    ).stream()

    # step 4: build marts layer
    context.log.info("Step 4/4 — Building marts layer...")
    yield from dbt.cli(
        [
            "build",
            "--select", "tag:marts",
            "--profiles-dir", str(DBT_DIR)
        ],
        context=context
    ).stream()

# ── jobs ──────────────────────────────────────────────────────

# job 1: daily incremental — only fetch new stars
daily_job = define_asset_job(
    name="stargazers_daily",
    selection=AssetSelection.all(),
    config={
        "ops": {
            "raw_stargazers": {
                "config": {"full_refresh": False}
            }
        }
    }
)

# job 2: full refresh — wipe everything and rebuild from scratch
full_refresh_job = define_asset_job(
    name="stargazers_full_refresh",
    selection=AssetSelection.all(),
    config={
        "ops": {
            "raw_stargazers": {
                "config": {"full_refresh": True}
            }
        }
    }
)

# ── schedules ─────────────────────────────────────────────────

# runs daily at 6am UTC
daily_schedule = ScheduleDefinition(
    name="stargazers_daily",
    job=daily_job,
    cron_schedule="0 6 * * *",
)

# ── wire everything together ──────────────────────────────────

defs = Definitions(
    assets=[raw_stargazers, transformed_stargazers],
    jobs=[daily_job, full_refresh_job],
    schedules=[daily_schedule],
    resources={"dbt": dbt_resource},
)
