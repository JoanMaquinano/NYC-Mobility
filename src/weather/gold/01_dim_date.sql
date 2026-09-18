-- GOLD LAYER: Date Dimension
-- Reads from: `nyc-mobility`.nyc_silver.weather_clean

MERGE INTO `nyc-mobility`.nyc_gold.dim_date AS target
USING (
  SELECT
    CAST(DATE_FORMAT(weather_date, 'yyyyMMdd') AS INT) AS date_key,
    weather_date AS full_date,
    YEAR(weather_date) AS year,
    QUARTER(weather_date) AS quarter,
    MONTH(weather_date) AS month,
    MONTHNAME(weather_date) AS month_name,
    DAYOFWEEK(weather_date) AS day_of_week,
    DAYNAME(weather_date) AS day_name,
    DAYOFWEEK(weather_date) IN (1, 7) AS is_weekend
  FROM (
    SELECT DISTINCT
      CAST(date AS DATE) AS weather_date
    FROM `nyc-mobility`.nyc_silver.vw_weather_valid
    WHERE TRY_CAST(date AS TIMESTAMP) IS NOT NULL
  ) dates
) AS source
ON target.date_key <=> source.date_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;