{{ config(
    materialized='table',
    on_schema_change='sync_all_columns'
) }}

WITH base AS (

    SELECT *
    FROM {{ ref('stg_flights') }}

),

filtered AS (

    -- Enforce data quality: remove invalid keys
    SELECT *
    FROM base
    WHERE flight_id IS NOT NULL
      AND departure IS NOT NULL

),

deduplicated AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY departure, flight_id, icao24, arrival
            ORDER BY load_ts DESC
        ) AS rn
    FROM filtered

)

SELECT
    -- identifiers
    flight_id,

    -- timestamps
    departure,
    arrival,

    -- aircraft / operator
    icao24,
    icao_operator,

    -- airports
    departure_airport,
    arrival_airport,

    -- partitioning
    year,
    month,
    day,

    -- metadata
    load_ts,
    ingestion_date,
    flight_date

FROM deduplicated
WHERE rn = 1