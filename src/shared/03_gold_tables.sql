-- GOLD DIMENSION TABLES

CREATE TABLE IF NOT EXISTS `nyc-mobility`.nyc_gold.dim_date (
    date_key INT NOT NULL,
    full_date DATE,
    year INT,
    quarter INT,
    month INT,
    month_name STRING,
    day_of_week INT,
    day_name STRING,
    is_weekend BOOLEAN
)
USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name'
);

CREATE TABLE IF NOT EXISTS `nyc-mobility`.nyc_gold.dim_taxi_zone (
    location_id INT NOT NULL,
    borough STRING,
    zone_name STRING,
    service_zone STRING
)
USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name'
);

CREATE TABLE IF NOT EXISTS `nyc-mobility`.nyc_gold.dim_weather (
  weather_key STRING,
  weather_date DATE,
  weather_timestamp TIMESTAMP,
  temp_max_c DOUBLE,
  temp_min_c DOUBLE,
  temp_avg_c DOUBLE,
  feels_like_avg_c DOUBLE,
  precipitation_mm DOUBLE,
  rain_hours DOUBLE,
  wind_speed DOUBLE,
  wind_gust DOUBLE,
  weather_code INT,
  weather_condition STRING
)
USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name'
);

-- GOLD FACT TABLE
-- Grain: 1 row = 1 taxi trip

CREATE TABLE IF NOT EXISTS `nyc-mobility`.nyc_gold.fact_trip (
    trip_key STRING NOT NULL,

    date_key INT,
    weather_key STRING,

    pickup_location_id INT,
    dropoff_location_id INT,
    pickup_hour INT,

    vendor_id INT,
    vendor_name STRING,

    payment_type_id INT,
    payment_type STRING,

    ratecode_id INT,
    ratecode_description STRING,

    trip_type_id INT,
    trip_type_description STRING,

    passenger_count INT,
    trip_distance DOUBLE,
    fare_amount DOUBLE,
    tip_amount DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE,

    trip_duration_minutes DOUBLE
)
USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name'
);

--GOLD: dim_table
CREATE TABLE IF NOT EXISTS `nyc-mobility`.nyc_gold.dim_date (
  date_key      INT,
  full_date     DATE,
  year          INT,
  quarter       INT,
  month         INT,
  month_name    STRING,
  day_of_week   INT,
  day_name      STRING,
  is_weekend    BOOLEAN
)
USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name'
);