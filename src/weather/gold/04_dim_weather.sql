-- Databricks notebook source
-- Load the existing daily weather dimension from hourly Silver data.
-- Grain: one row per weather_date. Stable string key: yyyyMMdd.
-- precipitation_mm uses rain; Silver has no total-precipitation measurement.
-- weather_condition is the most frequent non-null hourly description.
-- Ties are resolved alphabetically for deterministic reruns.

MERGE INTO `nyc-mobility`.nyc_gold.dim_weather AS target
USING (
  WITH hourly_weather AS (
    SELECT
      CAST(TRY_CAST(date AS TIMESTAMP) AS DATE) AS weather_date,
      temperature_2m,
      rain,
      weather_description
    FROM `nyc-mobility`.nyc_silver.weather_clean
  ),
  daily_measurements AS (
    SELECT
      weather_date,
      CAST(ROUND(MAX(temperature_2m), 2) AS DOUBLE) AS temp_max_c,
      CAST(ROUND(MIN(temperature_2m), 2) AS DOUBLE) AS temp_min_c,
      CAST(ROUND(SUM(rain), 2) AS DOUBLE) AS precipitation_mm
    FROM hourly_weather
    WHERE weather_date IS NOT NULL
    GROUP BY weather_date
  ),
  condition_counts AS (
    SELECT
      weather_date,
      weather_description,
      COUNT(*) AS hourly_count
    FROM hourly_weather
    WHERE weather_date IS NOT NULL
      AND weather_description IS NOT NULL
    GROUP BY weather_date, weather_description
  ),
  ranked_conditions AS (
    SELECT
      weather_date,
      weather_description,
      ROW_NUMBER() OVER (
        PARTITION BY weather_date
        ORDER BY hourly_count DESC, weather_description ASC
      ) AS condition_rank
    FROM condition_counts
  )
  SELECT
    DATE_FORMAT(d.weather_date, 'yyyyMMdd') AS weather_key,
    d.weather_date,
    d.temp_max_c,
    d.temp_min_c,
    d.precipitation_mm,
    COALESCE(c.weather_description, 'Unknown') AS weather_condition
  FROM daily_measurements AS d
  LEFT JOIN ranked_conditions AS c
    ON d.weather_date = c.weather_date
    AND c.condition_rank = 1
) AS source
ON target.weather_key = source.weather_key
WHEN MATCHED THEN UPDATE SET
  target.weather_date = source.weather_date,
  target.temp_max_c = source.temp_max_c,
  target.temp_min_c = source.temp_min_c,
  target.precipitation_mm = source.precipitation_mm,
  target.weather_condition = source.weather_condition
WHEN NOT MATCHED THEN INSERT (
  weather_key,
  weather_date,
  temp_max_c,
  temp_min_c,
  precipitation_mm,
  weather_condition
)
VALUES (
  source.weather_key,
  source.weather_date,
  source.temp_max_c,
  source.temp_min_c,
  source.precipitation_mm,
  source.weather_condition
);
