import duckdb

conn = duckdb.connect('stargazers.duckdb')



print("\n=== COUNTS PER REPO BY RAW DATA SOURCE ===")
print(conn.execute("""
    SELECT repo_name, COUNT(*) as total_stargazers
    FROM github_raw.stargazers
    GROUP BY repo_name
    ORDER BY total_stargazers DESC
""").df())

print("\n=== SAMPLE ROWS IN RAW DATA SOURCE ===")
print(conn.execute("""
    SELECT * FROM github_raw.stargazers
    LIMIT 5
""").df())

conn.close()