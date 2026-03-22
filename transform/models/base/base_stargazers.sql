

/*
    Base Model to capture 
*/
SELECT 
    s.user_id AS user_id,
    s.user_type AS user_type,
    s.login AS login,
    s.repo_name AS repo_name,
    s.starred_at AS starred_at
FROM {{ source('github_raw', 'stargazers') }} AS s 
