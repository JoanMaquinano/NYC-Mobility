-- Databricks notebook source
-- WEATHER BRONZE: load one monthly CSV into the shared Bronze Delta table.
-- Parameter: weather_file (example: weather_april_2026.csv)
-- The table is created separately in the shared table-definition script.

-- COMMAND ----------
MERGE INTO `nyc-mobility`.nyc_bronze.weather AS target
USING (
  SELECT
    date,
    temperature_2m,
    apparent_temperature,
    precipitation_probability,
    rain,
    weather_code,
    cloud_cover,
    visibility,
    wind_speed_10m,
    wind_gusts_10m,
    month,
    CURRENT_TIMESTAMP() AS ingestion_timestamp,
    '{weather_file}'  AS source_file_month
  FROM read_files(
    '/Volumes/workspace/default/ftw_b12_de/groups/week-08/group-d/weather/{weather_file}' ,
    format => 'csv',
    header => true,
    schema => 'date STRING, temperature_2m STRING, apparent_temperature STRING, precipitation_probability STRING, rain STRING, weather_code STRING, cloud_cover STRING, visibility STRING, wind_speed_10m STRING, wind_gusts_10m STRING, month STRING, latitude STRING, longitude STRING, source_series STRING',
    mode => 'FAILFAST'
  )
  -- Protect the first load from duplicate business keys inside the CSV itself.
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY date
    ORDER BY SHA2(TO_JSON(NAMED_STRUCT(
      'temperature_2m', temperature_2m,
      'apparent_temperature', apparent_temperature,
      'precipitation_probability', precipitation_probability,
      'rain', rain,
      'weather_code', weather_code,
      'cloud_cover', cloud_cover,
      'visibility', visibility,
      'wind_speed_10m', wind_speed_10m,
      'wind_gusts_10m', wind_gusts_10m,
      'month', month
    )), 256) DESC
  ) = 1
) AS source
ON target.date <=> source.date
WHEN NOT MATCHED THEN INSERT (
  date,
  temperature_2m,
  apparent_temperature,
  precipitation_probability,
  rain,
  weather_code,
  cloud_cover,
  visibility,
  wind_speed_10m,
  wind_gusts_10m,
  month,
  ingestion_timestamp,
  source_file_month
)
VALUES (
  source.date,
  source.temperature_2m,
  source.apparent_temperature,
  source.precipitation_probability,
  source.rain,
  source.weather_code,
  source.cloud_cover,
  source.visibility,
  source.wind_speed_10m,
  source.wind_gusts_10m,
  source.month,
  source.ingestion_timestamp,
  source.source_file_month
);

