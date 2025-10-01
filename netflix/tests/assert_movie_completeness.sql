-- Test to ensure all movies have proper metadata
-- This test checks that movies have titles and at least one genre
WITH movie_completeness AS (
    SELECT 
        movie_id,
        movie_title,
        genres,
        genre_array
    FROM {{ ref('dim_movies') }}
    WHERE 
        movie_title IS NULL 
        OR TRIM(movie_title) = ''
        OR genres IS NULL
        OR ARRAY_SIZE(genre_array) = 0
)

SELECT *
FROM movie_completeness
