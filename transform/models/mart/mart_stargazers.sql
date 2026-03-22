
SELECT 
    i.login,
    i.user_id,
    COUNT(DISTINCT i.repo_name) AS repos_starred,
    MIN(starred_at) AS first_starred_at,
    MAX(starred_at) AS last_starred_at,
    MIN(starred_month) AS first_starred_month,
    MAX(starred_month) AS last_starred_month
FROM {{ ref('int_stargazers') }} AS i
GROUP BY 1, 2