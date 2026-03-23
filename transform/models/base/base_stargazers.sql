/*
    Base Model to capture all the records from our rawdataset.
    We do minimal transformations at this stage to capture any source issues and 
    allow other users the ability to use this model cross domain.
*/
SELECT 
    s.user_id AS user_id,
    s.user_type AS user_type,
    s.login AS username,
    s.repo_name AS repo_name,
    s.starred_at AS starred_at,
    CURRENT_DATE() AS load_dt
FROM {{ source('github_raw', 'stargazers') }} AS s 
