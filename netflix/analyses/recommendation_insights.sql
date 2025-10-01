-- Recommendation system insights and user similarity analysis
WITH user_rating_matrix AS (
    SELECT 
        user_id,
        movie_id,
        rating,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY rating_timestamp DESC) as rating_recency
    FROM {{ ref('fct_ratings') }}
),

user_similarity AS (
    SELECT 
        u1.user_id as user_1,
        u2.user_id as user_2,
        COUNT(*) as common_movies,
        AVG(ABS(u1.rating - u2.rating)) as avg_rating_difference,
        CORR(u1.rating, u2.rating) as rating_correlation
    FROM user_rating_matrix u1
    JOIN user_rating_matrix u2 ON u1.movie_id = u2.movie_id AND u1.user_id < u2.user_id
    GROUP BY u1.user_id, u2.user_id
    HAVING COUNT(*) >= 5  -- Only users with at least 5 common movies
),

high_similarity_users AS (
    SELECT 
        user_1,
        user_2,
        common_movies,
        avg_rating_difference,
        rating_correlation
    FROM user_similarity
    WHERE rating_correlation > 0.7  -- High correlation threshold
),

recommendation_candidates AS (
    SELECT 
        hsu.user_1,
        hsu.user_2,
        r.movie_id,
        r.rating,
        m.movie_title,
        m.genre_array
    FROM high_similarity_users hsu
    JOIN {{ ref('fct_ratings') }} r ON hsu.user_2 = r.user_id
    LEFT JOIN {{ ref('dim_movies') }} m ON r.movie_id = m.movie_id
    WHERE NOT EXISTS (
        SELECT 1 
        FROM {{ ref('fct_ratings') }} r2 
        WHERE r2.user_id = hsu.user_1 
        AND r2.movie_id = r.movie_id
    )
)

SELECT 
    user_1 as target_user,
    user_2 as similar_user,
    movie_id,
    movie_title,
    rating as similar_user_rating,
    genre_array,
    common_movies,
    rating_correlation,
    -- Calculate recommendation score
    rating * rating_correlation as recommendation_score
FROM recommendation_candidates rc
JOIN high_similarity_users hsu ON rc.user_1 = hsu.user_1 AND rc.user_2 = hsu.user_2
WHERE rating >= 4.0  -- Only recommend highly rated movies
ORDER BY user_1, recommendation_score DESC
