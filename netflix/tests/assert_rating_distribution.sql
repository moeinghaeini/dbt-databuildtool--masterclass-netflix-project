-- Test to ensure rating distribution is reasonable
-- This test checks that ratings are distributed across the valid range
WITH rating_stats AS (
    SELECT 
        COUNT(*) as total_ratings,
        COUNT(CASE WHEN rating < 0.5 OR rating > 5.0 THEN 1 END) as invalid_ratings,
        AVG(rating) as avg_rating,
        MIN(rating) as min_rating,
        MAX(rating) as max_rating
    FROM {{ ref('fct_ratings') }}
)

SELECT *
FROM rating_stats
WHERE 
    invalid_ratings > 0
    OR avg_rating < 1.0 
    OR avg_rating > 5.0
    OR min_rating < 0.5
    OR max_rating > 5.0
