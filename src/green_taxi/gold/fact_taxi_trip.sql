-- FACT_TAXI_TRIP Table

--  Create the fact table structure
CREATE TABLE IF NOT EXISTS nyc_mobility.nyc_gold.fact_taxi_trip (
  trip_key STRING COMMENT 'MD5 hash of trip attributes for deduplication',
  pickup_date DATE COMMENT 'Foreign key to dim_date',
  dropoff_date DATE COMMENT 'Foreign key to dim_date',
  weather_key STRING COMMENT 'Foreign key to dim_weather',
  pickup_location_id INT COMMENT 'Foreign key to dim_taxi_zone',
  dropoff_location_id INT COMMENT 'Foreign key to dim_taxi_zone',
  lpep_pickup_datetime TIMESTAMP,
  lpep_dropoff_datetime TIMESTAMP,
  trip_duration_minutes DOUBLE COMMENT 'Calculated trip duration',
  passenger_count INT,
  trip_distance DOUBLE,
  vendor_id INT,
  vendor_name STRING,
  payment_type_id INT,
  payment_type STRING,
  ratecode_id INT,
  ratecode_description STRING,
  trip_type_id INT,
  trip_type_description STRING,
  fare_amount DOUBLE,
  extra DOUBLE,
  mta_tax DOUBLE,
  tip_amount DOUBLE,
  tolls_amount DOUBLE,
  improvement_surcharge DOUBLE,
  congestion_surcharge DOUBLE,
  cbd_congestion_fee DOUBLE,
  total_amount DOUBLE,
  store_and_fwd_flag STRING,
  qc_error_descriptions ARRAY<STRING> COMMENT 'Quality control flags from silver layer',
  created_at TIMESTAMP COMMENT 'Audit timestamp'
)
USING DELTA
COMMENT 'Green Taxi fact table with trip-level grain'
PARTITIONED BY (pickup_date);


SET TIME ZONE 'UTC';
USE CATALOG nyc_mobility;

-- Populate the fact table
MERGE INTO nyc_mobility.nyc_gold.fact_taxi_trip AS target
USING (
  WITH trip_with_keys AS (
    SELECT
      -- Generate deterministic trip_key using MD5 hash
      MD5(CONCAT_WS('|',
        CAST(t.vendor_id AS STRING),
        CAST(t.lpep_pickup_datetime AS STRING),
        CAST(t.lpep_dropoff_datetime AS STRING),
        CAST(t.pu_location_id AS STRING),
        CAST(t.do_location_id AS STRING),
        CAST(t.trip_distance AS STRING),
        CAST(t.total_amount AS STRING)
      )) AS trip_key,

      -- Date keys
      DATE(t.lpep_pickup_datetime) AS pickup_date,
      DATE(t.lpep_dropoff_datetime) AS dropoff_date,

      -- Timestamps
      t.lpep_pickup_datetime,
      t.lpep_dropoff_datetime,

      -- Trip duration in minutes (DOUBLE for fractional values)
      ROUND(
        (UNIX_TIMESTAMP(t.lpep_dropoff_datetime) - UNIX_TIMESTAMP(t.lpep_pickup_datetime)) / 60.0,
        2
      ) AS trip_duration_minutes,

      -- Location IDs (foreign keys to dim_taxi_zone)
      t.pu_location_id AS pickup_location_id,
      t.do_location_id AS dropoff_location_id,

      -- Vendor information (ID + description)
      t.vendor_id,
      CASE t.vendor_id
        WHEN 1 THEN 'Creative Mobile Technologies, LLC'
        WHEN 2 THEN 'VeriFone Inc.'
        WHEN 6 THEN 'Other'
        ELSE 'Unknown'
      END AS vendor_name,

      -- Payment information (ID + description)
      t.payment_type AS payment_type_id,
      CASE t.payment_type
        WHEN 0 THEN 'No charge'
        WHEN 1 THEN 'Credit card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided trip'
        ELSE 'Unknown'
      END AS payment_type,

      -- Ratecode information (ID + description)
      t.ratecode_id,
      CASE t.ratecode_id
        WHEN 1 THEN 'Standard rate'
        WHEN 2 THEN 'JFK'
        WHEN 3 THEN 'Newark'
        WHEN 4 THEN 'Nassau or Westchester'
        WHEN 5 THEN 'Negotiated fare'
        WHEN 6 THEN 'Group ride'
        WHEN 99 THEN 'Unknown'
        ELSE 'Unknown'
      END AS ratecode_description,

      -- Trip type information (ID + description)
      t.trip_type AS trip_type_id,
      CASE t.trip_type
        WHEN 1 THEN 'Street-hail'
        WHEN 2 THEN 'Dispatch'
        ELSE 'Unknown'
      END AS trip_type_description,

      -- Trip metrics
      t.passenger_count,
      t.trip_distance,

      -- Fare breakdown
      t.fare_amount,
      t.extra,
      t.mta_tax,
      t.tip_amount,
      t.tolls_amount,
      t.improvement_surcharge,
      t.congestion_surcharge,
      t.cbd_congestion_fee,
      t.total_amount,

      -- Additional attributes
      t.store_and_fwd_flag,

      -- Quality control flags
      t.qc_error_descriptions

    FROM nyc_mobility.nyc_silver.green_taxi_clean t
  ),

  trip_with_weather AS (
    SELECT
      t.*,
      w.weather_key
    FROM trip_with_keys t
    LEFT JOIN nyc_mobility.nyc_gold.dim_weather w
      -- dim_weather.weather_timestamp is UTC; lpep_pickup_datetime is NYC local
      -- time loaded as-is (bronze CAST with UTC session). Convert to UTC before
      -- truncating to the hour so each trip gets the correct hourly weather record.
      ON w.weather_timestamp = DATE_TRUNC('HOUR', to_utc_timestamp(t.lpep_pickup_datetime, 'America/New_York'))
  )

  SELECT
    -- Primary Key
    trip_key,

    -- Date Dimensions
    pickup_date,
    dropoff_date,

    -- Weather Dimension (foreign key)
    weather_key,

    -- Location Dimensions (foreign keys to dim_taxi_zone)
    pickup_location_id,
    dropoff_location_id,

    -- Timestamps
    lpep_pickup_datetime,
    lpep_dropoff_datetime,

    -- Trip Metrics
    trip_duration_minutes,
    passenger_count,
    trip_distance,

    -- Vendor (denormalized)
    vendor_id,
    vendor_name,

    -- Payment Type (denormalized)
    payment_type_id,
    payment_type,

    -- Rate Code (denormalized)
    ratecode_id,
    ratecode_description,

    -- Trip Type (denormalized)
    trip_type_id,
    trip_type_description,

    -- Fare Components
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    cbd_congestion_fee,
    total_amount,

    -- Additional Attributes
    store_and_fwd_flag,

    -- Quality Control
    qc_error_descriptions,

    -- Audit columns
    CURRENT_TIMESTAMP() AS created_at

  FROM trip_with_weather
) AS source
ON target.trip_key = source.trip_key
WHEN MATCHED THEN
  UPDATE SET
    pickup_date = source.pickup_date,
    dropoff_date = source.dropoff_date,
    weather_key = source.weather_key,
    pickup_location_id = source.pickup_location_id,
    dropoff_location_id = source.dropoff_location_id,
    lpep_pickup_datetime = source.lpep_pickup_datetime,
    lpep_dropoff_datetime = source.lpep_dropoff_datetime,
    trip_duration_minutes = source.trip_duration_minutes,
    passenger_count = source.passenger_count,
    trip_distance = source.trip_distance,
    vendor_id = source.vendor_id,
    vendor_name = source.vendor_name,
    payment_type_id = source.payment_type_id,
    payment_type = source.payment_type,
    ratecode_id = source.ratecode_id,
    ratecode_description = source.ratecode_description,
    trip_type_id = source.trip_type_id,
    trip_type_description = source.trip_type_description,
    fare_amount = source.fare_amount,
    extra = source.extra,
    mta_tax = source.mta_tax,
    tip_amount = source.tip_amount,
    tolls_amount = source.tolls_amount,
    improvement_surcharge = source.improvement_surcharge,
    congestion_surcharge = source.congestion_surcharge,
    cbd_congestion_fee = source.cbd_congestion_fee,
    total_amount = source.total_amount,
    store_and_fwd_flag = source.store_and_fwd_flag,
    qc_error_descriptions = source.qc_error_descriptions
    -- created_at intentionally omitted to preserve original insert timestamp
WHEN NOT MATCHED THEN
  INSERT *;