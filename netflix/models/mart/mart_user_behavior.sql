{{ config(materialized = 'table') }}

WITH user_ratings AS (
    SELECT 
        user_id,
        COUNT(*) as total_ratings,
        AVG(rating) as avg_rating,
        MIN(rating) as min_rating,
        MAX(rating) as max_rating,
        STDDEV(rating) as rating_stddev,
        MIN(rating_timestamp) as first_rating_date,
        MAX(rating_timestamp) as last_rating_date
    FROM {{ ref('fct_ratings') }}
    GROUP BY user_id
),

user_genre_preferences AS (
    SELECT 
        r.user_id,
        m.genre_array,
        COUNT(*) as ratings_in_genre
    FROM {{ ref('fct_ratings') }} r
    JOIN {{ ref('dim_movies') }} m ON r.movie_id = m.movie_id
    CROSS JOIN UNNEST(m.genre_array) AS genre
    GROUP BY r.user_id, genre
),

user_activity_summary AS (
    SELECT 
        ur.*,
        DATEDIFF('day', ur.first_rating_date, ur.last_rating_date) as days_active,
        CASE 
            WHEN ur.total_ratings >= 100 THEN 'Heavy User'
            WHEN ur.total_ratings >= 50 THEN 'Regular User'
            WHEN ur.total_ratings >= 10 THEN 'Light User'
            ELSE 'Casual User'
        END as user_segment,
        CASE 
            WHEN ur.avg_rating >= 4.0 THEN 'High Rater'
            WHEN ur.avg_rating >= 3.0 THEN 'Moderate Rater'
            ELSE 'Low Rater'
        END as rating_behavior
    FROM user_ratings ur
)

SELECT 
    uas.*,
    ugp.genre as favorite_genre,
    ugp.ratings_in_genre as favorite_genre_ratings
FROM user_activity_summary uas
LEFT JOIN (
    SELECT 
        user_id,
        genre,
        ratings_in_genre,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ratings_in_genre DESC) as rn
    FROM user_genre_preferences
) ugp ON uas.user_id = ugp.user_id AND ugp.rn = 1
