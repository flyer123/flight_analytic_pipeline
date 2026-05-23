{{ config(
    materialized='table',
    on_schema_change='sync_all_columns'
) }}

SELECT
    YEAR(departure) AS year,

    COUNT(DISTINCT icao24) AS unique_aircraft

FROM {{ ref('flights_clean') }}

WHERE icao24 IS NOT NULL

GROUP BY 1