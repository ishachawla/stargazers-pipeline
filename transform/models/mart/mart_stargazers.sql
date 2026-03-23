/*
    ---------------------------- GOLD LAYER --------------------------------------
    One row per human stargazer (bots already excluded at the intermediate layer.
    Aggregates activity across all tracked repos to give a user-level view of engagement.
    --------------------------------------------------------------------------------
*/

SELECT
    i.username,
    i.user_id,

    -- How many of the 5 tracked repos this user has starred.
    -- A value of 5 means they've starred every repo in our tracked set.
    COUNT(DISTINCT i.repo_name)     AS repos_starred,

    -- First and last star timestamps — useful for engagement span and recency analysis
    MIN(starred_at)                 AS first_starred_at,
    MAX(starred_at)                 AS last_starred_at,

    -- Month-level equivalents for easier cohort bucketing without timestamp precision
    MIN(starred_month)              AS first_starred_month,
    MAX(starred_month)              AS last_starred_month

-- Builds on the intermediate model which has already filtered bots.
FROM {{ ref('int_stargazers') }} AS i

-- Grain: one row per user across all repos they've starred
GROUP BY 1, 2
