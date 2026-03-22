{{
    config(
        materialized='incremental',
        unique_key='stargazer_pk'
    )
}}

{% set max_ts = '1970-01-01' %}
{% if is_incremental() %}
    {% set max_ts = run_query("SELECT COALESCE(MAX(starred_at), '1970-01-01') FROM " ~ this).columns[0].values()[0] %}
{% endif %}

SELECT 
    {{ dbt_utils.generate_surrogate_key(['user_id', 'repo_name']) }} AS stargazer_pk,
    s.user_id,
    s.username,
    s.repo_name,
    s.starred_at,
    DATE_TRUNC('month', s.starred_at) AS starred_month,
    DATE_TRUNC('year', s.starred_at)  AS starred_year
FROM {{ ref('base_stargazers') }} AS s
WHERE s.user_type = 'User'
AND s.starred_at > '{{ max_ts }}'