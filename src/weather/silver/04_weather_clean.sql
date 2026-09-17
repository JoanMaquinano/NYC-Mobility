-- Databricks notebook source
-- DBTITLE 1,Weather Silver - Clean and Validate
-- WEATHER SILVER: Clean and validate bronze weather data
-- Reads from: `nyc-mobility`.nyc_bronze.weather
-- Writes to: `nyc-mobility`.nyc_silver.weather_clean (MERGE)
-- Converts string columns to proper types, validates ranges, deduplicates

-- COMMAND ----------

-- DBTITLE 1,Create cleaned weather silver table
-- MERGE cleaned weather data into silver table (rounded to 2 decimal places, with weather descriptions)
MERGE INTO `nyc-mobility`.nyc_silver.weather_clean AS target
USING (
  SELECT
    date,
    temperature_2m,
    apparent_temperature,
    precipitation_probability,
    rain,
    weather_code,
    weather_description,
    cloud_cover,
    visibility,
    wind_speed_10m,
    wind_gusts_10m,
    month,
    ingestion_timestamp
  FROM (
  SELECT
    -- Core attributes with rounding to 2 decimal places
    date,
    ROUND(CAST(temperature_2m AS DOUBLE), 2) AS temperature_2m,
    ROUND(CAST(apparent_temperature AS DOUBLE), 2) AS apparent_temperature,
    ROUND(CAST(precipitation_probability AS DOUBLE), 2) AS precipitation_probability,
    ROUND(CAST(rain AS DOUBLE), 2) AS rain,
    CAST(TRY_CAST(weather_code AS DOUBLE) AS INT) AS weather_code,
    ROUND(CAST(cloud_cover AS DOUBLE), 2) AS cloud_cover,
    ROUND(CAST(visibility AS DOUBLE), 2) AS visibility,
    ROUND(CAST(wind_speed_10m AS DOUBLE), 2) AS wind_speed_10m,
    ROUND(CAST(wind_gusts_10m AS DOUBLE), 2) AS wind_gusts_10m,
    month,
    ingestion_timestamp,
    
    -- Weather code description (Open-Meteo WMO codes)
    CASE CAST(TRY_CAST(weather_code AS DOUBLE) AS INT)
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
      WHEN 95 THEN 'Thunderstorm'
      WHEN 96 THEN 'Thunderstorm with slight hail'
      WHEN 99 THEN 'Thunderstorm with heavy hail'
      ELSE 'Unknown'
    END AS weather_description,
    
    -- Parse for validation only
    TRY_CAST(date AS TIMESTAMP) AS weather_timestamp
  FROM `nyc-mobility`.nyc_bronze.weather
) cleaned_data

-- Validate date is in expected range (March-May 2026)
WHERE weather_timestamp IS NOT NULL
  AND weather_timestamp >= TIMESTAMP '2026-03-01 00:00:00'
  AND weather_timestamp < TIMESTAMP '2026-06-01 00:00:00'

-- Deduplicate by date, keeping newest ingestion
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY date
  ORDER BY ingestion_timestamp DESC
) = 1
) AS source
ON target.date <=> source.date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- DBTITLE 1,Validation queries
-- Validation queries
-- 1. Count rows by month (expect: March=744, April=720, May=744)
SELECT month, COUNT(*) AS row_count
FROM `nyc-mobility`.nyc_silver.weather_clean
GROUP BY month
ORDER BY month;

-- 2. Check for duplicates
SELECT COUNT(*) AS total_rows,
       COUNT(DISTINCT date) AS unique_dates
FROM `nyc-mobility`.nyc_silver.weather_clean;

-- 3. Preview cleaned data with weather descriptions
SELECT date, temperature_2m, apparent_temperature, rain, 
       weather_code, weather_description, wind_speed_10m, cloud_cover
FROM `nyc-mobility`.nyc_silver.weather_clean
ORDER BY date
LIMIT 10;