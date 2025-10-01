-- Advanced genre analysis for content strategy
WITH genre_ratings AS (
    SELECT 
        genre,
        COUNT(*) as total_ratings,
        AVG(rating) as avg_rating,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(DISTINCT movie_id) as unique_movies
    FROM {{ ref('fct_ratings') }} r
    JOIN {{ ref('dim_movies') }} m ON r.movie_id = m.movie_id
    CROSS JOIN UNNEST(m.genre_array) AS genre
    GROUP BY genre
),

genre_popularity AS (
    SELECT 
        *,
        RANK() OVER (ORDER BY total_ratings DESC) as popularity_rank,
        RANK() OVER (ORDER BY avg_rating DESC) as quality_rank,
        RANK() OVER (ORDER BY unique_users DESC) as user_engagement_rank
    FROM genre_ratings
)

SELECT 
    genre,
    total_ratings,
    avg_rating,
    unique_users,
    unique_movies,
    popularity_rank,
    quality_rank,
    user_engagement_rank,
    -- Calculate genre diversity score
    CASE 
        WHEN unique_movies / unique_users > 0.5 THEN 'High Diversity'
        WHEN unique_movies / unique_users > 0.2 THEN 'Medium Diversity'
        ELSE 'Low Diversity'
    END as diversity_score
FROM genre_popularity
ORDER BY popularity_rank
