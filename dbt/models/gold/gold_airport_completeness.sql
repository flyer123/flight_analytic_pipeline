{{ config(
    materialized='table',
    on_schema_change='sync_all_columns'
) }}

SELECT
    YEAR(departure) AS year,

    COUNT(*) AS total_flights,

    COUNT(departure_airport)
        AS flights_with_departure_airport,

    COUNT(arrival_airport)
        AS flights_with_arrival_airport,

    COUNT(*) - COUNT(departure_airport)
        AS missing_departure_airport,

    COUNT(*) - COUNT(arrival_airport)
        AS missing_arrival_airport,

    ROUND(
        COUNT(departure_airport) * 100.0 / COUNT(*),
        2
    ) AS departure_airport_coverage_pct,

    ROUND(
        COUNT(arrival_airport) * 100.0 / COUNT(*),
        2
    ) AS arrival_airport_coverage_pct

FROM {{ ref('flights_clean') }}

GROUP BY 1