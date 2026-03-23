{{
    config(
        materialized='incremental',
        unique_key='stargazer_pk'
    )
}}

/*
    ---------------------------- SILVER LAYER -----------------------------
    Intermediate Model to transform data from our raw layer.
    We filter out (removing bots) and dedupe any data if we need.
    Configuring this as an incremental build to allow for faster processing 
    in case of massive increase in volume of data.
    
    Configuring the Incremental block - 
    On the first run, max_ts defaults to the beginning of time so all records are loaded.
    On subsequent runs, is_incremental() is true and we query the current table
    to find the latest starred_at — only records newer than that will be processed.
    -----------------------------------------------------------------------
*/ 

{% set max_ts = '1970-01-01' %}
{% if is_incremental() %}
    {% set max_ts = run_query("SELECT COALESCE(MAX(starred_at), '1970-01-01') FROM " ~ this).columns[0].values()[0] %}
{% endif %}

SELECT 
    -- Using user_id in lieu of username to create the surrogate key because usernames for the same user id can change.
    {{ dbt_utils.generate_surrogate_key(['user_id', 'repo_name']) }} AS stargazer_pk,
    s.user_id,
    s.username,
    s.repo_name,
    s.starred_at,
    DATE_TRUNC('month', s.starred_at) AS starred_month,
    DATE_TRUNC('year', s.starred_at)  AS starred_year
FROM {{ ref('base_stargazers') }} AS s
-- Removing Bots
WHERE s.user_type = 'User'
AND s.starred_at > '{{ max_ts }}'
