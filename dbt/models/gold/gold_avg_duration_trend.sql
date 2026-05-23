{{ config(
    materialized='table',
    on_schema_change='sync_all_columns'
) }}

SELECT
    YEAR(departure) AS year,

    ROUND(
        AVG(
            DATEDIFF(minute, departure, arrival)
        ) / 60.0,
        2
    ) AS avg_duration_hours

FROM {{ ref('flights_clean') }}

WHERE
    departure IS NOT NULL
    AND arrival IS NOT NULL
    AND arrival >= departure

GROUP BY 1