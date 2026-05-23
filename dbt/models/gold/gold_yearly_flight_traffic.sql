{{ config(
    materialized='table',
    on_schema_change='sync_all_columns'
) }}

SELECT
    YEAR(departure) AS year,

    COUNT(*) AS total_flights

FROM {{ ref('flights_clean') }}

GROUP BY 1