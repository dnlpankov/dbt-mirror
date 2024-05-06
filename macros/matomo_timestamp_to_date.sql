{% macro matomo_timestamp_to_date(ts) %}
    CASE
        WHEN DATE({{ ts }} - INTERVAL '2 hour') > DATE '2023-10-28' THEN DATE({{ ts }} - INTERVAL '2 hour')
        ELSE DATE({{ ts }} - INTERVAL '3 hour')
    END
{% endmacro %}