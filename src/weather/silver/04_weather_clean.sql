-- Databricks notebook source
-- DBTITLE 1,Weather Silver - Clean and Validate
-- WEATHER SILVER: Clean and validate bronze weather data
-- Reads from: `nyc-mobility`.nyc_bronze.weather
-- Writes to: `nyc-mobility`.nyc_silver.weather_clean (MERGE)
-- Converts string columns to proper types, validates ranges, deduplicates.

-- COMMAND ----------

-- DBTITLE 1,Create cleaned weather silver table
-- MERGE cleaned weather data into silver table (rounded to 2 decimal places, with weather descriptions)
-- added columns for QC [Jemma]
SET TIME ZONE 'America/New_York';
USE CATALOG `nyc-mobility`;
USE SCHEMA nyc_silver;

MERGE INTO `nyc-mobility`.nyc_silver.weather_clean AS target
USING (
  SELECT
    *,
    -- dq_status
    CASE
      WHEN size(qc_error_descriptions) = 0                             THEN 'PASS'
      WHEN exists(qc_error_descriptions, x -> startswith(x, 'FAIL:'))  THEN 'FAIL'
      ELSE 'WARN'
    END                                          AS dq_status,
    current_timestamp()                          AS silver_at
  FROM (
    SELECT
      weather_hour,
      `date`,
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
      `month`,
      ingestion_timestamp,
      source_file_month,
 
      ARRAY_COMPACT(ARRAY(
 
        -- FAIL: hour cannot be parsed to timestamp
        CASE WHEN weather_hour IS NULL
             THEN 'FAIL: date: hour could not be parsed to a timestamp' END,
        -- Warn: nulls
        CASE WHEN raw_temperature_2m IS NOT NULL AND temperature_2m IS NULL
             THEN 'WARN: temperature_2m: value did not parse' END,
        CASE WHEN raw_apparent_temperature IS NOT NULL AND apparent_temperature IS NULL
             THEN 'WARN: apparent_temperature: value did not parse' END,
        CASE WHEN raw_precipitation_probability IS NOT NULL AND precipitation_probability IS NULL
             THEN 'WARN: precipitation_probability: value did not parse' END,
        CASE WHEN raw_rain IS NOT NULL AND rain IS NULL
             THEN 'WARN: rain: value did not parse' END,
        CASE WHEN raw_cloud_cover IS NOT NULL AND cloud_cover IS NULL
             THEN 'WARN: cloud_cover: value did not parse' END,
        CASE WHEN raw_visibility IS NOT NULL AND visibility IS NULL
             THEN 'WARN: visibility: value did not parse' END,
        CASE WHEN raw_wind_speed_10m IS NOT NULL AND wind_speed_10m IS NULL
             THEN 'WARN: wind_speed_10m: value did not parse' END,
        CASE WHEN raw_wind_gusts_10m IS NOT NULL AND wind_gusts_10m IS NULL
             THEN 'WARN: wind_gusts_10m: value did not parse' END,
        CASE WHEN raw_weather_code IS NOT NULL AND weather_code IS NULL
             THEN 'WARN: weather_code: value did not parse' END,
        -- Warn: outside range
        CASE WHEN temperature_2m IS NOT NULL
               AND temperature_2m NOT BETWEEN -40.0 AND 130.0
             THEN 'WARN: temperature_2m: outside a plausible range' END,
        CASE WHEN rain IS NOT NULL AND rain < 0.0
             THEN 'WARN: rain: negative rainfall' END,
        CASE WHEN cloud_cover IS NOT NULL
               AND cloud_cover NOT BETWEEN 0.0 AND 100.0
             THEN 'WARN: cloud_cover: outside 0 to 100' END,
        CASE WHEN precipitation_probability IS NOT NULL
               AND precipitation_probability NOT BETWEEN 0.0 AND 100.0
             THEN 'WARN: precipitation_probability: outside 0 to 100' END,
        CASE WHEN visibility IS NOT NULL AND visibility < 0.0
             THEN 'WARN: visibility: negative distance' END,
        CASE WHEN wind_speed_10m IS NOT NULL AND wind_speed_10m < 0.0
             THEN 'WARN: wind_speed_10m: negative speed' END,
        -- A gust is by definition a peak of the wind, so it cannot sit below
        -- the sustained speed. The two are measured over different intervals
        -- and rounded independently, which is where the handful of violations
        -- come from. A column swap would show up as tens of percent.
        CASE WHEN wind_gusts_10m IS NOT NULL AND wind_speed_10m IS NOT NULL
               AND wind_gusts_10m < wind_speed_10m
             THEN 'WARN: wind_gusts_10m: gust is below the sustained wind speed' END,
        -- A code that parsed but is not in the WMO 4677 set the CASE below
        -- knows about. The description silently becomes 'Unknown', so without
        -- this the only evidence is a string nobody queries.
        CASE WHEN weather_code IS NOT NULL AND weather_description = 'Unknown'
             THEN 'WARN: weather_code: code is not a known WMO 4677 value' END,
        -- The loader bug, recorded per row rather than only in the run log.
        CASE WHEN source_file_month IS NULL OR source_file_month LIKE '%{%'
             THEN 'WARN: source_file_month: placeholder rather than a filename' END
      )) AS qc_error_descriptions
    FROM (
      SELECT
        -- The typed hour. try_cast, so a malformed string becomes NULL and is
        -- classified rather than killing the statement. SET TIME ZONE above is
        -- what makes this land on the right instant.
        TRY_CAST(`date` AS TIMESTAMP)                               AS weather_hour,
        `date`,
        ROUND(TRY_CAST(temperature_2m AS DOUBLE), 2)                AS temperature_2m,
        ROUND(TRY_CAST(apparent_temperature AS DOUBLE), 2)          AS apparent_temperature,
        ROUND(TRY_CAST(precipitation_probability AS DOUBLE), 2)     AS precipitation_probability,
        ROUND(TRY_CAST(rain AS DOUBLE), 2)                          AS rain,
 
        -- Parsed as DOUBLE first, then narrowed. try_cast('3.0' AS INT) is
        -- NULL -- a CSV that writes a code as 3.0 would report 100 percent
        -- unparseable when the data is fine and the cast is wrong.
        CAST(TRY_CAST(weather_code AS DOUBLE) AS INT)               AS weather_code,
        ROUND(TRY_CAST(cloud_cover AS DOUBLE), 2)                   AS cloud_cover,
        ROUND(TRY_CAST(visibility AS DOUBLE), 2)                    AS visibility,
        ROUND(TRY_CAST(wind_speed_10m AS DOUBLE), 2)                AS wind_speed_10m,
        ROUND(TRY_CAST(wind_gusts_10m AS DOUBLE), 2)                AS wind_gusts_10m,
        `month`,
        ingestion_timestamp,
        source_file_month,
        -- Weather code description (Open-Meteo WMO 4677). Unchanged.
        CASE CAST(TRY_CAST(weather_code AS DOUBLE) AS INT)
          WHEN 0  THEN 'Clear sky'
          WHEN 1  THEN 'Mainly clear'
          WHEN 2  THEN 'Partly cloudy'
          WHEN 3  THEN 'Overcast'
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
        END  AS weather_description,
        -- Raw values, so a parse failure can be told apart from a value the
        -- source never sent. Without these, NULL means both things.
        temperature_2m            AS raw_temperature_2m,
        apparent_temperature      AS raw_apparent_temperature,
        precipitation_probability AS raw_precipitation_probability,
        rain                      AS raw_rain,
        cloud_cover               AS raw_cloud_cover,
        visibility                AS raw_visibility,
        wind_speed_10m            AS raw_wind_speed_10m,
        wind_gusts_10m            AS raw_wind_gusts_10m,
        weather_code              AS raw_weather_code
      FROM `nyc-mobility`.nyc_bronze.weather
    ) cleaned_data
 
    -- Deduplicate on the PARSED hour, not the raw text.
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY COALESCE(CAST(weather_hour AS STRING), `date`)
      ORDER BY ingestion_timestamp DESC, `date`
    ) = 1
  ) audited
) AS source
-- Keyed on the parsed hour. `date` is a secondary key so that unparseable
-- rows, whose hour is NULL, do not all match each other -- <=> treats NULL as
-- equal to NULL.
ON  target.weather_hour <=> source.weather_hour
AND target.`date`       <=> source.`date`
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *;


-- Views. These should be used in Gold layer
CREATE OR REPLACE VIEW `nyc-mobility`.nyc_silver.vw_weather_valid
COMMENT 'Weather hours usable downstream: clean and flagged-but-interpretable. What Gold reads.'
AS
SELECT *
FROM   `nyc-mobility`.nyc_silver.weather_clean
WHERE  dq_status IN ('PASS', 'WARN');
 
CREATE OR REPLACE VIEW `nyc-mobility`.nyc_silver.vw_weather_quarantined
COMMENT 'Weather hours excluded from Gold, with the reasons.'
AS
SELECT *,
    filter(qc_error_descriptions, x -> startswith(x, 'FAIL:')) AS fail_reasons
FROM   `nyc-mobility`.nyc_silver.weather_clean
WHERE  dq_status = 'FAIL';



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