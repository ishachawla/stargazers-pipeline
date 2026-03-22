import dlt
import requests
import os

## Repos to get stargazers dataset for.
REPOS = [
    ("dbt-labs", "dbt-core"),
    ("apache", "airflow"),
    ("dagster-io", "dagster"),
    ("duckdb", "duckdb"),
    ("dlt-hub", "dlt"),
]

def get_stargazers():
## Loading the environment variables - use the .env_example
    token = os.environ.get("GITHUB_TOKEN")
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.star+json"
    }

    all_records = []

    for owner, repo in REPOS:
        page = 1
        while True:
            response = requests.get(
                f"https://api.github.com/repos/{owner}/{repo}/stargazers",
                headers=headers,
                params={"per_page": 100, "page": page}
            )

            data = response.json()

            if isinstance(data, dict) and "message" in data:
                print(f"GitHub API error: {data['message']}")
                break

            if not data:
                break

            # Fields to save
            for record in data:
                all_records.append({
                    "login": record["user"]["login"],
                    "user_id": record["user"]["id"],
                    "user_type": record["user"]["type"],
                    "repo_name": f"{owner}/{repo}",
                    "starred_at": record["starred_at"],
                })

            print(f"Fetched page {page} of {owner}/{repo} — {len(all_records)} total records so far")
            page += 1

    return all_records


def run_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="stargazers",
        destination="duckdb",
        dataset_name="github_raw"
    )

    records = get_stargazers()
    load_info = pipeline.run(
        records,
        table_name="stargazers",
        write_disposition="merge",
        primary_key=["login", "repo_name"]
    )
    print(load_info)
    return load_info

if __name__ == "__main__":
    run_pipeline()

