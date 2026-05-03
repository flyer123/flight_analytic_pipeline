{{ config(
    materialized='view'
) }}

WITH source_data AS (

    SELECT *
    FROM {{ source('bronze', 'flights') }}

),

typed AS (

    SELECT
        -- identifiers
        CAST(flight_id AS STRING)                AS flight_id,

        -- timestamps
        CAST(departure AS TIMESTAMP)             AS departure,
        CAST(arrival AS TIMESTAMP)               AS arrival,

        -- aircraft / operator
        CAST(icao24 AS STRING)                   AS icao24,
        CAST(icao_operator AS STRING)            AS icao_operator,

        -- airports (normalized naming)
        CAST(ADEP AS STRING)                     AS departure_airport,
        CAST(ADES AS STRING)                     AS arrival_airport,

        -- partition columns (from ingestion path)
        CAST(year AS INT)                        AS year,
        CAST(month AS INT)                       AS month,
        CAST(day AS INT)                         AS day,

        -- metadata
        CAST(load_ts AS TIMESTAMP)               AS load_ts,

        -- derived fields
        CAST(load_ts AS DATE)                    AS ingestion_date,
        CAST(departure AS DATE)                  AS flight_date

    FROM source_data

)

SELECT *
FROM typed