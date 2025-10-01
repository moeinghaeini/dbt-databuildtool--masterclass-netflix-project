{% macro generate_surrogate_key(column_list) %}
    {{ return(dbt_utils.generate_surrogate_key(column_list)) }}
{% endmacro %}

{% macro validate_rating_range(column_name) %}
    CASE 
        WHEN {{ column_name }} < {{ var('min_rating_threshold') }} 
             OR {{ column_name }} > {{ var('max_rating_threshold') }}
        THEN 'INVALID_RATING'
        ELSE 'VALID_RATING'
    END
{% endmacro %}

{% macro validate_relevance_score(column_name) %}
    CASE 
        WHEN {{ column_name }} < {{ var('min_relevance_score') }} 
             OR {{ column_name }} > {{ var('max_relevance_score') }}
        THEN 'INVALID_SCORE'
        ELSE 'VALID_SCORE'
    END
{% endmacro %}

{% macro get_data_freshness(table_name, timestamp_column) %}
    SELECT 
        '{{ table_name }}' as table_name,
        MAX({{ timestamp_column }}) as latest_timestamp,
        CURRENT_TIMESTAMP() as current_timestamp,
        DATEDIFF('hour', MAX({{ timestamp_column }}), CURRENT_TIMESTAMP()) as hours_since_last_update
    FROM {{ ref(table_name) }}
{% endmacro %}

{% macro create_audit_columns() %}
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    dbt_updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
{% endmacro %}
