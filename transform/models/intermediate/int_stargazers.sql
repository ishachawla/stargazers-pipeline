{{
    config
    (
        unique_key='stargazer_pk'
    )
}}

SELECT 
    {{ dbt_utils.generate_surrogate_key(['user_id', 'repo_name']) }} AS stargazer_pk,
    s.user_id,
    s.login,
    s.repo_name,
    s.starred_at,
    DATE_TRUNC('month', s.starred_at) AS starred_month,
    DATE_TRUNC('year', s.starred_at)  AS starred_year
FROM {{ ref('base_stargazers') }} AS s
WHERE s.user_type = 'User'
   
