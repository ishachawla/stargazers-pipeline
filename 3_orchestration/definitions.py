import sys
from pathlib import Path
from dagster import asset, Definitions, ScheduleDefinition, define_asset_job, AssetSelection
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
@asset
def stargazers_raw():
    """Extract stargazers from GitHub API and load into DuckDB"""
    sys.path.append(str(ROOT))
    from ingestion.extract_github_stargazers import run_pipeline
    run_pipeline()

# assets 2 + 3 - dbt models
@dbt_assets(manifest=DBT_DIR / "target" / "manifest.json")
def dbt_stargazers(context, dbt: DbtCliResource):
    yield from dbt.cli(
        ["run", "--profiles-dir", str(DBT_DIR)],
        context=context
    ).stream()

# daily schedule
daily = ScheduleDefinition(
    job=define_asset_job("daily_job", AssetSelection.all()),
    cron_schedule="0 0 * * *",
)

# wire everything together
defs = Definitions(
    assets=[stargazers_raw, dbt_stargazers],
    schedules=[daily],
    resources={"dbt": dbt_resource},
)