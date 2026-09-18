-- Grain: one observation per hour, keeping the newest ingestion.
-- Stable weather_key: yyyyMMddHH. Use the same key in fact tables.
-- Parse weather and taxi timestamps using the same timezone convention.
-- If the DAILY merge was previously run, migrate daily rows and their fact
-- references before loading hourly data; do not mix daily and hourly keys.
-- Temperature min/max are daily aggregates; temp_avg is the hourly observation.
-- precipitation_mm measures rain only. Wind fields retain Silver's units.

MERGE INTO `nyc-mobility`.nyc_gold.dim_weather AS target
USING (
  WITH parsed_weather AS (
    SELECT
      TRY_CAST(date AS TIMESTAMP) AS observation_timestamp,
      temperature_2m,
      apparent_temperature,
      rain,
      wind_speed_10m,
      wind_gusts_10m,
      weather_code,
      weather_description,
      ingestion_timestamp
    FROM `nyc-mobility`.nyc_silver.vw_weather_valid
  ),
  hourly_weather AS (
    SELECT
      DATE_TRUNC('HOUR', observation_timestamp) AS weather_timestamp,
      temperature_2m,
      apparent_temperature,
      rain,
      wind_speed_10m,
      wind_gusts_10m,
      weather_code,
      weather_description
    FROM parsed_weather
    WHERE observation_timestamp IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY DATE_TRUNC('HOUR', observation_timestamp)
      ORDER BY ingestion_timestamp DESC NULLS LAST,
               observation_timestamp DESC,
               temperature_2m DESC NULLS LAST,
               apparent_temperature DESC NULLS LAST,
               rain DESC NULLS LAST,
               wind_speed_10m DESC NULLS LAST,
               wind_gusts_10m DESC NULLS LAST,
               weather_code DESC NULLS LAST,
               weather_description ASC NULLS LAST
    ) = 1
  )
  SELECT
    DATE_FORMAT(weather_timestamp, 'yyyyMMddHH') AS weather_key,
    CAST(weather_timestamp AS DATE) AS weather_date,
    weather_timestamp,
    MAX(CAST(temperature_2m AS DOUBLE)) OVER (PARTITION BY CAST(weather_timestamp AS DATE)) AS temp_max_c,
    MIN(CAST(temperature_2m AS DOUBLE)) OVER (PARTITION BY CAST(weather_timestamp AS DATE)) AS temp_min_c,
    CAST(temperature_2m AS DOUBLE) AS temp_avg_c,
    CAST(apparent_temperature AS DOUBLE) AS feels_like_avg_c,
    CAST(rain AS DOUBLE) AS precipitation_mm,
    -- Sum across dimension hours to count rainy hours; NULL means unknown.
    CASE
      WHEN rain IS NULL THEN CAST(NULL AS INT)
      WHEN CAST(rain AS DOUBLE) > 0 THEN 1
      ELSE 0
    END AS rain_hours,
    CAST(wind_speed_10m AS DOUBLE) AS wind_speed,
    CAST(wind_gusts_10m AS DOUBLE) AS wind_gust,
    CAST(weather_code AS INT) AS weather_code,
    COALESCE(weather_description, 'Unknown') AS weather_condition
  FROM hourly_weather
) AS source
ON target.weather_key = source.weather_key
WHEN MATCHED THEN UPDATE SET
  target.weather_date = source.weather_date,
  target.weather_timestamp = source.weather_timestamp,
  target.temp_max_c = source.temp_max_c,
  target.temp_min_c = source.temp_min_c,
  target.temp_avg_c = source.temp_avg_c,
  target.feels_like_avg_c = source.feels_like_avg_c,
  target.precipitation_mm = source.precipitation_mm,
  target.rain_hours = source.rain_hours,
  target.wind_speed = source.wind_speed,
  target.wind_gust = source.wind_gust,
  target.weather_code = source.weather_code,
  target.weather_condition = source.weather_condition
WHEN NOT MATCHED THEN INSERT (
  weather_key,
  weather_date,
  weather_timestamp,
  temp_max_c,
  temp_min_c,
  temp_avg_c,
  feels_like_avg_c,
  precipitation_mm,
  rain_hours,
  wind_speed,
  wind_gust,
  weather_code,
  weather_condition
)
VALUES (
  source.weather_key,
  source.weather_date,
  source.weather_timestamp,
  source.temp_max_c,
  source.temp_min_c,
  source.temp_avg_c,
  source.feels_like_avg_c,
  source.precipitation_mm,
  source.rain_hours,
  source.wind_speed,
  source.wind_gust,
  source.weather_code,
  source.weather_condition
);