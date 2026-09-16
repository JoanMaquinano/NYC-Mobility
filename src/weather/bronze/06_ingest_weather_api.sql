-- Databricks notebook source
-- STEP 2: monthly CSV files to raw Bronze Delta tables.
-- Run 01 first. Values stay STRING exactly as read; cleaning belongs in Silver.
-- Tables are managed in the team's catalog; the Volume holds CSV files only.
-- IF NOT EXISTS makes reruns a no-op. To refresh frozen data, plan a new version.
CREATE SCHEMA IF NOT EXISTS `nyc-mobility`.nyc_bronze;
SET TIME ZONE 'UTC';

-- COMMAND ----------
CREATE TABLE IF NOT EXISTS `nyc-mobility`.nyc_bronze.weather_march_2026_bronze
USING DELTA AS
SELECT *, current_timestamp() AS ingestion_timestamp,
       current_date() AS ingestion_date,
       'march' AS source_file_month
FROM read_files(
  '/Volumes/workspace/default/ftw_b12_de/groups/week-08/group-d/weather/weather_march_2026.csv',
  format => 'csv', header => true,
  schema => 'date STRING, temperature_2m STRING, apparent_temperature STRING, precipitation_probability STRING, rain STRING, weather_code STRING, cloud_cover STRING, visibility STRING, wind_speed_10m STRING, wind_gusts_10m STRING, month STRING, latitude STRING, longitude STRING, source_series STRING',
  mode => 'FAILFAST'
);

-- COMMAND ----------
CREATE TABLE IF NOT EXISTS `nyc-mobility`.nyc_bronze.weather_april_2026_bronze
USING DELTA AS
SELECT *, current_timestamp() AS ingestion_timestamp,
       current_date() AS ingestion_date,
       'april' AS source_file_month
FROM read_files(
  '/Volumes/workspace/default/ftw_b12_de/groups/week-08/group-d/weather/weather_april_2026.csv',
  format => 'csv', header => true,
  schema => 'date STRING, temperature_2m STRING, apparent_temperature STRING, precipitation_probability STRING, rain STRING, weather_code STRING, cloud_cover STRING, visibility STRING, wind_speed_10m STRING, wind_gusts_10m STRING, month STRING, latitude STRING, longitude STRING, source_series STRING',
  mode => 'FAILFAST'
);

-- COMMAND ----------
CREATE TABLE IF NOT EXISTS `nyc-mobility`.nyc_bronze.weather_may_2026_bronze
USING DELTA AS
SELECT *, current_timestamp() AS ingestion_timestamp,
       current_date() AS ingestion_date,
       'may' AS source_file_month
FROM read_files(
  '/Volumes/workspace/default/ftw_b12_de/groups/week-08/group-d/weather/weather_may_2026.csv',
  format => 'csv', header => true,
  schema => 'date STRING, temperature_2m STRING, apparent_temperature STRING, precipitation_probability STRING, rain STRING, weather_code STRING, cloud_cover STRING, visibility STRING, wind_speed_10m STRING, wind_gusts_10m STRING, month STRING, latitude STRING, longitude STRING, source_series STRING',
  mode => 'FAILFAST'
);

-- COMMAND ----------
-- UNION ALL preserves duplicates so Silver can identify and account for them.
CREATE OR REPLACE VIEW `nyc-mobility`.nyc_bronze.weather_csv_raw AS
SELECT * FROM `nyc-mobility`.nyc_bronze.weather_march_2026_bronze
UNION ALL
SELECT * FROM `nyc-mobility`.nyc_bronze.weather_april_2026_bronze
UNION ALL
SELECT * FROM `nyc-mobility`.nyc_bronze.weather_may_2026_bronze;

