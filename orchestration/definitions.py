import sys
import duckdb
from pathlib import Path
from dagster import (
    op,
    job,
    In,
    Nothing,
    Definitions,
    ScheduleDefinition,
)
from dagster_dbt import DbtCliResource

ROOT = Path(__file__).parent.parent
DBT_DIR = ROOT / "transform"

dbt_resource = DbtCliResource(
    project_dir=str(DBT_DIR),
    profiles_dir=str(DBT_DIR),
)


@op
def extract_stargazers(context) -> Nothing:
    """Step 1 — Extract stargazers from GitHub API into DuckDB"""
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    from ingestion.extract_github_stargazers import run_pipeline, get_last_loaded_at

    last_loaded_at = get_last_loaded_at()
    context.log.info(f"Fetching stars newer than: {last_loaded_at or 'beginning of time'}")
    run_pipeline(last_loaded_at)
    context.log.info("Extraction complete")


@op(ins={"after_extract": In(Nothing)})
def check_freshness(context, dbt: DbtCliResource) -> Nothing:
    """Step 2 — Check source freshness against SLAs defined in sources.yml"""
    context.log.info("Checking source freshness...")
    dbt.cli(["source", "freshness"]).wait()
    context.log.info("Freshness check complete")


@op(ins={"after_freshness": In(Nothing)})
def run_dbt(context, dbt: DbtCliResource) -> Nothing:
    """Step 3 — Run dbt models: base → intermediate → marts"""
    context.log.info("Running dbt build...")
    dbt.cli(["build"]).wait()
    context.log.info("dbt complete")


@job(
    name="stargazers_daily",
    resource_defs={"dbt": dbt_resource},
)
def stargazers_daily():
    """Daily pipeline: extract stargazers → check freshness → run dbt"""
    after_extract = extract_stargazers()
    after_freshness = check_freshness(after_extract)
    run_dbt(after_freshness)


daily_schedule = ScheduleDefinition(
    name="stargazers_daily_6am",
    job=stargazers_daily,
    cron_schedule="0 6 * * *",
)

defs = Definitions(
    jobs=[stargazers_daily],
    schedules=[daily_schedule],
    resources={"dbt": dbt_resource},
)
