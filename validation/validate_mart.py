import duckdb

conn = duckdb.connect('stargazers.duckdb')

print("\n=== MART: SAMPLE ROWS ===")
print(conn.execute("""
    SELECT *
    FROM main_mart_stargazers.mart_stargazers
    ORDER BY last_starred_at DESC
    LIMIT 10
""").df())

print("\n=== MART: STARS PER REPO ===")
print(conn.execute("""
    SELECT
        username,
        user_id,
        repos_starred,
        first_starred_at
    FROM main_mart_stargazers.mart_stargazers
    ORDER BY repos_starred DESC
""").df())

conn.close()
