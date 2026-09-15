-- Databricks notebook source
-- Run after Bronze and shared/setup/02_silver_tables.sql. UTC session is required.
SET TIME ZONE 'UTC';

-- COMMAND ----------
-- The view is a reproducible audit trail across all successful response versions.
CREATE OR REPLACE VIEW `nyc-mobility`.nyc_quality.weather_candidates AS
WITH expanded AS (
    SELECT b.batch_id, b.ingested_at, b.location_id, b.series_id, b.month,
        EXPLODE(FROM_JSON(b.hourly_rows_json, 'ARRAY<STRUCT<event_epoch:BIGINT,temperature_2m:STRING,apparent_temperature:STRING,precipitation_probability:STRING,rain:STRING,weather_code:STRING,cloud_cover:STRING,visibility:STRING,wind_speed_10m:STRING,wind_gusts_10m:STRING>>')) AS r
    FROM `nyc-mobility`.nyc_bronze.weather_api_response AS b
    WHERE b.status = 'SUCCESS'
), typed AS (
    SELECT *,
        TRY_CAST(r.temperature_2m AS DOUBLE) AS n_temperature_2m,
        TRY_CAST(r.apparent_temperature AS DOUBLE) AS n_apparent_temperature,
        TRY_CAST(r.precipitation_probability AS DOUBLE) AS n_precipitation_probability,
        TRY_CAST(r.rain AS DOUBLE) AS n_rain,
        TRY_CAST(r.weather_code AS DOUBLE) AS n_weather_code,
        TRY_CAST(r.cloud_cover AS DOUBLE) AS n_cloud_cover,
        TRY_CAST(r.visibility AS DOUBLE) AS n_visibility,
        TRY_CAST(r.wind_speed_10m AS DOUBLE) AS n_wind_speed_10m,
        TRY_CAST(r.wind_gusts_10m AS DOUBLE) AS n_wind_gusts_10m
    FROM expanded
), cleaned AS (
    SELECT
        SHA2(CONCAT_WS('|', location_id, series_id, CAST(r.event_epoch AS STRING)), 256) AS weather_key,
        location_id, series_id, r.event_epoch AS event_epoch,
        TIMESTAMP_SECONDS(r.event_epoch) AS weather_timestamp_utc,
        CAST(FROM_UTC_TIMESTAMP(TIMESTAMP_SECONDS(r.event_epoch), 'America/New_York') AS TIMESTAMP_NTZ)
            AS weather_local_timestamp,
        CASE WHEN n_temperature_2m IS NOT NULL AND NOT ISNAN(n_temperature_2m) AND ABS(n_temperature_2m) <= 1.7976931348623157E308 THEN n_temperature_2m END AS temperature_2m,
        CASE WHEN n_apparent_temperature IS NOT NULL AND NOT ISNAN(n_apparent_temperature) AND ABS(n_apparent_temperature) <= 1.7976931348623157E308 THEN n_apparent_temperature END AS apparent_temperature,
        CASE WHEN n_precipitation_probability IS NOT NULL AND NOT ISNAN(n_precipitation_probability) AND ABS(n_precipitation_probability) <= 1.7976931348623157E308 AND n_precipitation_probability BETWEEN 0 AND 100 THEN n_precipitation_probability END AS precipitation_probability,
        CASE WHEN n_rain IS NOT NULL AND NOT ISNAN(n_rain) AND ABS(n_rain) <= 1.7976931348623157E308 AND n_rain >= 0 THEN n_rain END AS rain,
        CASE WHEN n_weather_code IS NOT NULL AND NOT ISNAN(n_weather_code) AND ABS(n_weather_code) <= 1.7976931348623157E308 AND n_weather_code IN (0,1,2,3,45,48,51,53,55,56,57,61,63,65,66,67,71,73,75,77,80,81,82,85,86,95,96,99) THEN CAST(n_weather_code AS INT) END AS weather_code,
        CASE WHEN n_cloud_cover IS NOT NULL AND NOT ISNAN(n_cloud_cover) AND ABS(n_cloud_cover) <= 1.7976931348623157E308 AND n_cloud_cover BETWEEN 0 AND 100 THEN n_cloud_cover END AS cloud_cover,
        CASE WHEN n_visibility IS NOT NULL AND NOT ISNAN(n_visibility) AND ABS(n_visibility) <= 1.7976931348623157E308 AND n_visibility >= 0 THEN n_visibility END AS visibility,
        CASE WHEN n_wind_speed_10m IS NOT NULL AND NOT ISNAN(n_wind_speed_10m) AND ABS(n_wind_speed_10m) <= 1.7976931348623157E308 AND n_wind_speed_10m >= 0 THEN n_wind_speed_10m END AS wind_speed_10m,
        CASE WHEN n_wind_gusts_10m IS NOT NULL AND NOT ISNAN(n_wind_gusts_10m) AND ABS(n_wind_gusts_10m) <= 1.7976931348623157E308 AND n_wind_gusts_10m >= 0 THEN n_wind_gusts_10m END AS wind_gusts_10m,
        FILTER(ARRAY(
            CASE WHEN r.temperature_2m IS NOT NULL AND NOT (n_temperature_2m IS NOT NULL AND NOT ISNAN(n_temperature_2m) AND ABS(n_temperature_2m) <= 1.7976931348623157E308) THEN 'temperature_2m:invalid' END,
            CASE WHEN r.apparent_temperature IS NOT NULL AND NOT (n_apparent_temperature IS NOT NULL AND NOT ISNAN(n_apparent_temperature) AND ABS(n_apparent_temperature) <= 1.7976931348623157E308) THEN 'apparent_temperature:invalid' END,
            CASE WHEN r.precipitation_probability IS NOT NULL AND NOT (n_precipitation_probability IS NOT NULL AND NOT ISNAN(n_precipitation_probability) AND ABS(n_precipitation_probability) <= 1.7976931348623157E308 AND n_precipitation_probability BETWEEN 0 AND 100) THEN 'precipitation_probability:invalid' END,
            CASE WHEN r.rain IS NOT NULL AND NOT (n_rain IS NOT NULL AND NOT ISNAN(n_rain) AND ABS(n_rain) <= 1.7976931348623157E308 AND n_rain >= 0) THEN 'rain:invalid' END,
            CASE WHEN r.weather_code IS NOT NULL AND NOT (n_weather_code IS NOT NULL AND NOT ISNAN(n_weather_code) AND ABS(n_weather_code) <= 1.7976931348623157E308 AND n_weather_code IN (0,1,2,3,45,48,51,53,55,56,57,61,63,65,66,67,71,73,75,77,80,81,82,85,86,95,96,99)) THEN 'weather_code:invalid' END,
            CASE WHEN r.cloud_cover IS NOT NULL AND NOT (n_cloud_cover IS NOT NULL AND NOT ISNAN(n_cloud_cover) AND ABS(n_cloud_cover) <= 1.7976931348623157E308 AND n_cloud_cover BETWEEN 0 AND 100) THEN 'cloud_cover:invalid' END,
            CASE WHEN r.visibility IS NOT NULL AND NOT (n_visibility IS NOT NULL AND NOT ISNAN(n_visibility) AND ABS(n_visibility) <= 1.7976931348623157E308 AND n_visibility >= 0) THEN 'visibility:invalid' END,
            CASE WHEN r.wind_speed_10m IS NOT NULL AND NOT (n_wind_speed_10m IS NOT NULL AND NOT ISNAN(n_wind_speed_10m) AND ABS(n_wind_speed_10m) <= 1.7976931348623157E308 AND n_wind_speed_10m >= 0) THEN 'wind_speed_10m:invalid' END,
            CASE WHEN r.wind_gusts_10m IS NOT NULL AND NOT (n_wind_gusts_10m IS NOT NULL AND NOT ISNAN(n_wind_gusts_10m) AND ABS(n_wind_gusts_10m) <= 1.7976931348623157E308 AND n_wind_gusts_10m >= 0) THEN 'wind_gusts_10m:invalid' END
        ), x -> x IS NOT NULL) AS quality_errors,
        FILTER(ARRAY(
            CASE WHEN r.temperature_2m IS NULL THEN 'temperature_2m' END,
            CASE WHEN r.apparent_temperature IS NULL THEN 'apparent_temperature' END,
            CASE WHEN r.precipitation_probability IS NULL AND series_id NOT LIKE 'reanalysis:%' THEN 'precipitation_probability' END,
            CASE WHEN r.rain IS NULL THEN 'rain' END,
            CASE WHEN r.weather_code IS NULL THEN 'weather_code' END,
            CASE WHEN r.cloud_cover IS NULL THEN 'cloud_cover' END,
            CASE WHEN r.visibility IS NULL AND series_id NOT LIKE 'reanalysis:%' THEN 'visibility' END,
            CASE WHEN r.wind_speed_10m IS NULL THEN 'wind_speed_10m' END,
            CASE WHEN r.wind_gusts_10m IS NULL THEN 'wind_gusts_10m' END
        ), x -> x IS NOT NULL) AS missing_fields,
        CASE WHEN series_id LIKE 'reanalysis:%' THEN ARRAY('precipitation_probability', 'visibility')
            ELSE CAST(ARRAY() AS ARRAY<STRING>) END AS unavailable_fields,
        batch_id AS source_batch_id, ingested_at AS source_ingested_at
    FROM typed
)
SELECT *, CAST(weather_local_timestamp AS DATE) AS weather_date,
    HOUR(weather_local_timestamp) AS weather_hour,
    SHA2(TO_JSON(NAMED_STRUCT('temperature_2m', temperature_2m, 'apparent_temperature', apparent_temperature, 'precipitation_probability', precipitation_probability, 'rain', rain, 'weather_code', weather_code, 'cloud_cover', cloud_cover, 'visibility', visibility, 'wind_speed_10m', wind_speed_10m, 'wind_gusts_10m', wind_gusts_10m, 'quality_errors', quality_errors, 'missing_fields', missing_fields, 'unavailable_fields', unavailable_fields)), 256) AS value_hash,
    ROW_NUMBER() OVER (
        PARTITION BY location_id, series_id, event_epoch
        ORDER BY source_ingested_at DESC, source_batch_id DESC
    ) AS version_rank
FROM cleaned;

-- COMMAND ----------
-- Invalid measurements become NULL and remain explicitly flagged; no hourly keys
-- are dropped. Malformed envelopes/timestamps fail in Bronze before SUCCESS.
MERGE INTO `nyc-mobility`.nyc_silver.weather_hourly AS t
USING (SELECT * EXCEPT (version_rank) FROM `nyc-mobility`.nyc_quality.weather_candidates WHERE version_rank = 1) AS s
ON t.weather_key = s.weather_key
WHEN MATCHED AND t.value_hash <> s.value_hash THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
