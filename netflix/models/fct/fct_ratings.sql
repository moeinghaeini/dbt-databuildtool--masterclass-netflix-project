{{
  config(
    materialized = 'incremental',
    on_schema_change='fail',
    unique_key=['user_id', 'movie_id', 'rating_timestamp'],
    cluster_by=['rating_timestamp', 'user_id']
  )
}}

WITH src_ratings AS (
  SELECT * FROM {{ ref('src_ratings') }}
),

validated_ratings AS (
  SELECT
    user_id,
    movie_id,
    rating,
    rating_timestamp,
    {{ validate_rating_range('rating') }} as rating_validation
  FROM src_ratings
  WHERE rating IS NOT NULL
    AND rating >= {{ var('min_rating_threshold') }}
    AND rating <= {{ var('max_rating_threshold') }}
)

SELECT
  user_id,
  movie_id,
  rating,
  rating_timestamp,
  {{ create_audit_columns() }}
FROM validated_ratings

{% if is_incremental() %}
  WHERE rating_timestamp > (SELECT MAX(rating_timestamp) FROM {{ this }})
{% endif %}