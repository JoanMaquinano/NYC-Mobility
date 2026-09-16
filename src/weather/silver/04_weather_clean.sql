-- Databricks notebook source
-- STEP 3: clean Bronze only. This file never calls the API or changes Bronze.
-- Outputs are a rebuildable snapshot of this March-May CSV pipeline only.
-- Separate names keep this version distinct from the earlier API-direct pipeline.
-- Units: temperature C, probability/cloud %, rain mm, visibility m, wind km/h.
CREATE SCHEMA IF NOT EXISTS `nyc-mobility`.nyc_silver;
SET TIME ZONE 'UTC';

-- COMMAND ----------
-- Parse safely: malformed strings become NULL rather than stopping the load.
CREATE OR REPLACE TEMP VIEW weather_parsed AS
SELECT *, TRY_CAST(date AS TIMESTAMP) AS weather_timestamp_utc,
       TRY_CAST(latitude AS DOUBLE) AS latitude_number,
       TRY_CAST(longitude AS DOUBLE) AS longitude_number,
       TRY_CAST(temperature_2m AS DOUBLE) AS temperature_2m_number,
       TRY_CAST(apparent_temperature AS DOUBLE) AS apparent_temperature_number,
       TRY_CAST(precipitation_probability AS DOUBLE) AS precipitation_probability_number,
       TRY_CAST(rain AS DOUBLE) AS rain_number,
       TRY_CAST(weather_code AS DOUBLE) AS weather_code_number,
       TRY_CAST(cloud_cover AS DOUBLE) AS cloud_cover_number,
       TRY_CAST(visibility AS DOUBLE) AS visibility_number,
       TRY_CAST(wind_speed_10m AS DOUBLE) AS wind_speed_10m_number,
       TRY_CAST(wind_gusts_10m AS DOUBLE) AS wind_gusts_10m_number
FROM `nyc-mobility`.nyc_bronze.weather_csv_raw;

-- Reject unusable business keys; keep the raw rows available for QC.
CREATE OR REPLACE TEMP VIEW weather_key_checked AS
SELECT *, COALESCE(
    weather_timestamp_utc >= TIMESTAMP '2026-03-01 00:00:00'
    AND weather_timestamp_utc < TIMESTAMP '2026-06-01 00:00:00'
    AND weather_timestamp_utc = date_trunc('HOUR', weather_timestamp_utc)
    AND latitude_number = 40.7143 AND longitude_number = -74.006
    AND source_series = 'historical_forecast:best_match', FALSE) AS valid_key
FROM weather_parsed;

CREATE OR REPLACE TABLE `nyc-mobility`.nyc_silver.weather_csv_rejected
USING DELTA AS
SELECT *, 'Invalid timestamp, coordinates, or source series' AS rejection_reason
FROM weather_key_checked WHERE NOT valid_key;

-- COMMAND ----------
-- Keep the newest duplicate, with deterministic tie-breaking by raw row contents.
CREATE OR REPLACE TEMP VIEW weather_ranked AS
SELECT *, ROW_NUMBER() OVER (
    PARTITION BY latitude_number, longitude_number, source_series, weather_timestamp_utc
    ORDER BY ingestion_timestamp DESC, source_file_month DESC,
        date DESC NULLS LAST, month DESC NULLS LAST, temperature_2m DESC NULLS LAST, apparent_temperature DESC NULLS LAST, precipitation_probability DESC NULLS LAST, rain DESC NULLS LAST, weather_code DESC NULLS LAST, cloud_cover DESC NULLS LAST, visibility DESC NULLS LAST, wind_speed_10m DESC NULLS LAST, wind_gusts_10m DESC NULLS LAST
) AS duplicate_rank
FROM weather_key_checked WHERE valid_key;

-- A valid measurement is finite and satisfies its physical/domain rules.
-- We do not invent temperature cutoffs that could discard real extreme weather.
CREATE OR REPLACE TEMP VIEW weather_validated AS
SELECT *,
    COALESCE(NOT ISNAN(temperature_2m_number) AND ABS(temperature_2m_number) <= 1.7976931348623157E308, FALSE) AS temperature_2m_valid,
    COALESCE(NOT ISNAN(apparent_temperature_number) AND ABS(apparent_temperature_number) <= 1.7976931348623157E308, FALSE) AS apparent_temperature_valid,
    COALESCE(NOT ISNAN(precipitation_probability_number) AND ABS(precipitation_probability_number) <= 1.7976931348623157E308 AND precipitation_probability_number BETWEEN 0 AND 100, FALSE) AS precipitation_probability_valid,
    COALESCE(NOT ISNAN(rain_number) AND ABS(rain_number) <= 1.7976931348623157E308 AND rain_number >= 0, FALSE) AS rain_valid,
    COALESCE(NOT ISNAN(weather_code_number) AND ABS(weather_code_number) <= 1.7976931348623157E308 AND weather_code_number IN (0,1,2,3,45,48,51,53,55,56,57,61,63,65,66,67,71,73,75,77,80,81,82,85,86,95,96,99), FALSE) AS weather_code_valid,
    COALESCE(NOT ISNAN(cloud_cover_number) AND ABS(cloud_cover_number) <= 1.7976931348623157E308 AND cloud_cover_number BETWEEN 0 AND 100, FALSE) AS cloud_cover_valid,
    COALESCE(NOT ISNAN(visibility_number) AND ABS(visibility_number) <= 1.7976931348623157E308 AND visibility_number >= 0, FALSE) AS visibility_valid,
    COALESCE(NOT ISNAN(wind_speed_10m_number) AND ABS(wind_speed_10m_number) <= 1.7976931348623157E308 AND wind_speed_10m_number >= 0, FALSE) AS wind_speed_10m_valid,
    COALESCE(NOT ISNAN(wind_gusts_10m_number) AND ABS(wind_gusts_10m_number) <= 1.7976931348623157E308 AND wind_gusts_10m_number >= 0, FALSE) AS wind_gusts_10m_valid
FROM weather_ranked WHERE duplicate_rank = 1;

-- COMMAND ----------
CREATE OR REPLACE TABLE `nyc-mobility`.nyc_silver.weather_csv_clean
USING DELTA AS
SELECT
    SHA2(CONCAT_WS('|', CAST(latitude_number AS STRING), CAST(longitude_number AS STRING),
        source_series, CAST(CAST(weather_timestamp_utc AS BIGINT) AS STRING)), 256) AS weather_key,
    weather_timestamp_utc,
    CAST(FROM_UTC_TIMESTAMP(weather_timestamp_utc, 'America/New_York') AS TIMESTAMP_NTZ) AS weather_local_timestamp,
    latitude_number AS latitude, longitude_number AS longitude, source_series,
    CASE WHEN temperature_2m_valid THEN temperature_2m_number END AS temperature,
    CASE WHEN apparent_temperature_valid THEN apparent_temperature_number END AS apparent_temperature,
    CASE WHEN precipitation_probability_valid THEN precipitation_probability_number END AS precipitation_probability,
    CASE WHEN rain_valid THEN rain_number END AS rain,
    CASE WHEN weather_code_valid THEN CAST(weather_code_number AS INT) END AS weather_code,
    CASE WHEN cloud_cover_valid THEN cloud_cover_number END AS cloud_cover_total,
    CASE WHEN visibility_valid THEN visibility_number END AS visibility,
    CASE WHEN wind_speed_10m_valid THEN wind_speed_10m_number END AS wind_speed,
    CASE WHEN wind_gusts_10m_valid THEN wind_gusts_10m_number END AS wind_gust,
    date_format(weather_timestamp_utc, 'yyyy-MM') AS month,
    CASE WHEN weather_code_valid THEN CASE weather_code_number
        WHEN 0 THEN 'Clear sky'
        WHEN 1 THEN 'Mainly clear'
        WHEN 2 THEN 'Partly cloudy'
        WHEN 3 THEN 'Overcast'
        WHEN 45 THEN 'Fog'
        WHEN 48 THEN 'Depositing rime fog'
        WHEN 51 THEN 'Light drizzle'
        WHEN 53 THEN 'Moderate drizzle'
        WHEN 55 THEN 'Dense drizzle'
        WHEN 56 THEN 'Light freezing drizzle'
        WHEN 57 THEN 'Dense freezing drizzle'
        WHEN 61 THEN 'Slight rain'
        WHEN 63 THEN 'Moderate rain'
        WHEN 65 THEN 'Heavy rain'
        WHEN 66 THEN 'Light freezing rain'
        WHEN 67 THEN 'Heavy freezing rain'
        WHEN 71 THEN 'Slight snowfall'
        WHEN 73 THEN 'Moderate snowfall'
        WHEN 75 THEN 'Heavy snowfall'
        WHEN 77 THEN 'Snow grains'
        WHEN 80 THEN 'Slight rain showers'
        WHEN 81 THEN 'Moderate rain showers'
        WHEN 82 THEN 'Violent rain showers'
        WHEN 85 THEN 'Slight snow showers'
        WHEN 86 THEN 'Heavy snow showers'
        WHEN 95 THEN 'Slight or moderate thunderstorm'
        WHEN 96 THEN 'Thunderstorm with slight hail'
        WHEN 99 THEN 'Thunderstorm with heavy hail'
    END ELSE 'Unknown or missing' END AS weather_description,
    FILTER(ARRAY(
        CASE WHEN temperature_2m IS NULL OR TRIM(temperature_2m) = '' THEN 'temperature_2m:missing' WHEN NOT temperature_2m_valid THEN 'temperature_2m:invalid' END,
        CASE WHEN apparent_temperature IS NULL OR TRIM(apparent_temperature) = '' THEN 'apparent_temperature:missing' WHEN NOT apparent_temperature_valid THEN 'apparent_temperature:invalid' END,
        CASE WHEN precipitation_probability IS NULL OR TRIM(precipitation_probability) = '' THEN 'precipitation_probability:missing' WHEN NOT precipitation_probability_valid THEN 'precipitation_probability:invalid' END,
        CASE WHEN rain IS NULL OR TRIM(rain) = '' THEN 'rain:missing' WHEN NOT rain_valid THEN 'rain:invalid' END,
        CASE WHEN weather_code IS NULL OR TRIM(weather_code) = '' THEN 'weather_code:missing' WHEN NOT weather_code_valid THEN 'weather_code:invalid' END,
        CASE WHEN cloud_cover IS NULL OR TRIM(cloud_cover) = '' THEN 'cloud_cover:missing' WHEN NOT cloud_cover_valid THEN 'cloud_cover:invalid' END,
        CASE WHEN visibility IS NULL OR TRIM(visibility) = '' THEN 'visibility:missing' WHEN NOT visibility_valid THEN 'visibility:invalid' END,
        CASE WHEN wind_speed_10m IS NULL OR TRIM(wind_speed_10m) = '' THEN 'wind_speed_10m:missing' WHEN NOT wind_speed_10m_valid THEN 'wind_speed_10m:invalid' END,
        CASE WHEN wind_gusts_10m IS NULL OR TRIM(wind_gusts_10m) = '' THEN 'wind_gusts_10m:missing' WHEN NOT wind_gusts_10m_valid THEN 'wind_gusts_10m:invalid' END,
        CASE WHEN NOT (month <=> date_format(weather_timestamp_utc, 'yyyy-MM')) THEN 'month:corrected_from_timestamp' END,
        CASE WHEN source_file_month <> lower(date_format(weather_timestamp_utc, 'MMMM')) THEN 'source_file_month:mismatch' END
    ), x -> x IS NOT NULL) AS quality_issues,
    ingestion_timestamp AS bronze_ingestion_timestamp, ingestion_date AS bronze_ingestion_date,
    source_file_month, current_timestamp() AS cleaned_at
FROM weather_validated;

-- COMMAND ----------
-- Reconciliation: Bronze = rejected keys + duplicate rows removed + Silver.
SELECT b.n AS bronze_rows, r.n AS rejected_rows, d.n AS duplicates_removed,
       s.n AS silver_rows, b.n - r.n - d.n - s.n AS unexplained_difference
FROM (SELECT COUNT(*) n FROM `nyc-mobility`.nyc_bronze.weather_csv_raw) b
CROSS JOIN (SELECT COUNT(*) n FROM `nyc-mobility`.nyc_silver.weather_csv_rejected) r
CROSS JOIN (SELECT COUNT(*) n FROM weather_ranked WHERE duplicate_rank > 1) d
CROSS JOIN (SELECT COUNT(*) n FROM `nyc-mobility`.nyc_silver.weather_csv_clean) s;

-- Expected 744, 720, 744. Quality issues remain visible, never silently zero-filled.
SELECT month, COUNT(*) AS silver_rows,
       COUNT_IF(SIZE(quality_issues) > 0) AS flagged_rows,
       COUNT(*) - COUNT(DISTINCT weather_key) AS duplicate_keys
FROM `nyc-mobility`.nyc_silver.weather_csv_clean GROUP BY month ORDER BY month;

-- Explicit gap check against all 2,208 expected UTC hours. Expect zero rows.
SELECT expected_hour AS missing_hour
FROM (SELECT EXPLODE(SEQUENCE(TIMESTAMP '2026-03-01 00:00:00',
    TIMESTAMP '2026-05-31 23:00:00', INTERVAL 1 HOUR)) AS expected_hour) expected
LEFT ANTI JOIN `nyc-mobility`.nyc_silver.weather_csv_clean actual
ON actual.weather_timestamp_utc = expected.expected_hour;

SELECT * FROM `nyc-mobility`.nyc_silver.weather_csv_clean
ORDER BY weather_timestamp_utc LIMIT 10;
