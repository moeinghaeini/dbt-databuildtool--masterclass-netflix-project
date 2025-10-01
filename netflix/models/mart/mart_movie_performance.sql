{{ config(materialized = 'table') }}

WITH movie_ratings AS (
    SELECT 
        movie_id,
        COUNT(*) as total_ratings,
        AVG(rating) as avg_rating,
        MIN(rating) as min_rating,
        MAX(rating) as max_rating,
        STDDEV(rating) as rating_stddev,
        MIN(rating_timestamp) as first_rating_date,
        MAX(rating_timestamp) as last_rating_date
    FROM {{ ref('fct_ratings') }}
    GROUP BY movie_id
),

movie_genre_analysis AS (
    SELECT 
        m.movie_id,
        m.movie_title,
        m.genre_array,
        ARRAY_SIZE(m.genre_array) as genre_count,
        mr.*
    FROM {{ ref('dim_movies') }} m
    JOIN movie_ratings mr ON m.movie_id = mr.movie_id
),

movie_performance_metrics AS (
    SELECT 
        *,
        DATEDIFF('day', first_rating_date, last_rating_date) as rating_period_days,
        CASE 
            WHEN total_ratings >= 1000 AND avg_rating >= 4.0 THEN 'Blockbuster'
            WHEN total_ratings >= 500 AND avg_rating >= 3.5 THEN 'Popular'
            WHEN total_ratings >= 100 AND avg_rating >= 3.0 THEN 'Well-Rated'
            WHEN total_ratings >= 50 THEN 'Moderate'
            ELSE 'Niche'
        END as movie_category,
        CASE 
            WHEN rating_stddev <= 0.5 THEN 'Consistent'
            WHEN rating_stddev <= 1.0 THEN 'Mixed'
            ELSE 'Polarizing'
        END as rating_consistency
    FROM movie_genre_analysis
)

SELECT 
    movie_id,
    movie_title,
    genre_array,
    genre_count,
    total_ratings,
    avg_rating,
    min_rating,
    max_rating,
    rating_stddev,
    first_rating_date,
    last_rating_date,
    rating_period_days,
    movie_category,
    rating_consistency,
    -- Calculate rating trend (recent vs overall)
    CASE 
        WHEN rating_period_days > 30 THEN 'Long-running'
        WHEN rating_period_days > 7 THEN 'Medium-term'
        ELSE 'Recent'
    END as rating_trend
FROM movie_performance_metrics
