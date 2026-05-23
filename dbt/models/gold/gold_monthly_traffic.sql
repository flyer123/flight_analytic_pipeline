{{ config(
    materialized='table',
    on_schema_change='sync_all_columns'
) }}

SELECT
    YEAR(departure) AS year,

    MONTH(departure) AS month,

    DATE_TRUNC('month', departure) AS month_start,

    COUNT(*) AS total_flights,

    COUNT(DISTINCT icao24) AS unique_aircraft,

    ROUND(
        AVG(
            DATEDIFF(minute, departure, arrival)
        ) / 60.0,
        2
    ) AS avg_duration_hours,

    COUNT(departure_airport)
        AS flights_with_departure_airport,

    COUNT(arrival_airport)
        AS flights_with_arrival_airport,

    ROUND(
        COUNT(departure_airport) * 100.0 / COUNT(*),
        2
    ) AS departure_airport_coverage_pct,

    ROUND(
        COUNT(arrival_airport) * 100.0 / COUNT(*),
        2
    ) AS arrival_airport_coverage_pct

FROM {{ ref('flights_clean') }}

WHERE
    departure IS NOT NULL
    AND arrival IS NOT NULL
    AND arrival >= departure

GROUP BY 1,2,3