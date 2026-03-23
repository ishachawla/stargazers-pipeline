import dlt
import requests
import os
import time
import duckdb

# The repos we are extracting stargazers from
REPOS = [
    ("dbt-labs", "dbt-core"),
    ("apache", "airflow"),   # note: GitHub caps pagination at 400 pages (40,000 records) for large repos
    ("dagster-io", "dagster"),
    ("duckdb", "duckdb"),
    ("dlt-hub", "dlt"),
]

# Helper function to just load incremental data. Helps us reduce the API calls.
def get_last_loaded_at():
    try:
        conn = duckdb.connect('stargazers.duckdb')
        result = conn.execute(
            "SELECT MAX(starred_at) FROM github_raw.stargazers"
        ).fetchone()
        conn.close()
        return result[0]
    except:
        return None  
        # first run — table doesn't exist yet


# Captures error messages from Github - lets us know if we have hit the API limit.
def fetch_page(url, headers, params):
    while True:
        response = requests.get(url, headers=headers, params=params)
        data = response.json()

        # GitHub returned an error — figure out which kind
        if isinstance(data, dict) and "message" in data:
            remaining = int(response.headers.get("X-RateLimit-Remaining", 999))
            print(f"  GitHub says: {data['message']}")

            # case 1: actually rate limited
            if remaining < 50:
                reset = int(response.headers.get("X-RateLimit-Reset", 0))
                wait = max(reset - time.time(), 0) + 5
                print(f"  Rate limited — waiting {int(wait)}s then retrying...")
                time.sleep(wait)
                continue  # retry the same page after waiting

            # case 2: GitHub's hard pagination cap on large repos
            # apache/airflow has 44k stars but GitHub only allows fetching 40k (400 pages)
            # this is a known GitHub API restriction.
            elif "pagination is limited" in data["message"]:
                print(f"  GitHub pagination cap reached — captured maximum available records for this repo")
                return None

            # case 3: some other error (bad token, repo not found, etc)
            else:
                print(f"  Unexpected error — skipping this repo")
                return None

        # success — return the list of stargazers
        return data

# Fetch Stargazers for all the repos
# Using our helper function from above to just load new data with each run.
def get_stargazers(last_loaded_at=None):
    token = os.environ.get("GITHUB_TOKEN")
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.star+json"
    }

    all_records = []

    for owner, repo in REPOS:
        print(f"\nFetching {owner}/{repo}...")
        page = 1

        while True:
            url = f"https://api.github.com/repos/{owner}/{repo}/stargazers"
            data = fetch_page(url, headers, {"per_page": 100, "page": page})

            # fetch_page hit an unrecoverable error — skip to next repo
            if data is None:
                break

            # empty page — no more stargazers left for this repo
            if not data:
                print(f"  Finished {owner}/{repo}")
                break

            stop = False
            for record in data:
                # GitHub returns newest stars first
                # once we see a star older than our cursor we can stop —
                # everything after this will be even older
                if last_loaded_at and record["starred_at"] <= str(last_loaded_at):
                    print(f"  Reached already-loaded data — stopping early")
                    stop = True
                    break

                all_records.append({
                    "login": record["user"]["login"],
                    "user_id": record["user"]["id"],
                    "user_type": record["user"]["type"], 
                    "repo_name": f"{owner}/{repo}",
                    "starred_at": record["starred_at"],
                })

            if stop:
                break

            print(f"  Page {page} — {len(all_records)} total records so far")
            page += 1

    return all_records

# Use Merge disposition to just load updated records or new records.
# this is a DLT feature though technically with our last_loaded_at function we just find 
# new records.
def run_pipeline(last_loaded_at=None):
    pipeline = dlt.pipeline(
        pipeline_name="stargazers",
        destination="duckdb",
        dataset_name="github_raw"
    )

    # check what we already have in DuckDB
    last_loaded_at = get_last_loaded_at()
    if last_loaded_at:
        print(f"Incremental run — fetching stars newer than: {last_loaded_at}")
    else:
        print("First run — fetching everything from scratch")

    records = get_stargazers(last_loaded_at)
    print(f"\nFound {len(records)} new records to load")

    if not records:
        print("Nothing new to load — pipeline complete")
        return

    load_info = pipeline.run(
        records,
        table_name="stargazers",
        write_disposition="merge",
        primary_key=["login", "repo_name"]
    )
    print(load_info)


if __name__ == "__main__":
    run_pipeline()
