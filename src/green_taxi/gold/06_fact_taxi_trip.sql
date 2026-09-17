-- FACT_TAXI_TRIP table
-- Grain: one row per Green Taxi trip.
--
-- Timestamp convention:
--   TLC taxi timestamps and Open-Meteo weather timestamps are treated as
--   America/New_York local timestamps. Both sides are truncated to the hour
--   before joining. If the upstream tables are changed to UTC, convert both
--   timestamps to UTC before DATE_TRUNC instead of changing only one side.

CREATE TABLE IF NOT EXISTS `nyc-mobility`.nyc_gold.fact_taxi_trip (
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
  created_at TIMESTAMP COMMENT 'Original insert timestamp; preserved on updates'
)
USING DELTA
COMMENT 'Green Taxi fact table with trip-level grain'
PARTITIONED BY (pickup_date);

MERGE INTO `nyc-mobility`.nyc_gold.fact_taxi_trip AS target
USING (
  WITH trip_candidates AS (
    SELECT
      MD5(CONCAT_WS('|',
        COALESCE(CAST(t.vendor_id AS STRING), '<NULL>'),
        COALESCE(CAST(t.lpep_pickup_datetime AS STRING), '<NULL>'),
        COALESCE(CAST(t.lpep_dropoff_datetime AS STRING), '<NULL>'),
        COALESCE(CAST(t.pu_location_id AS STRING), '<NULL>'),
        COALESCE(CAST(t.do_location_id AS STRING), '<NULL>'),
        COALESCE(CAST(t.trip_distance AS STRING), '<NULL>'),
        COALESCE(CAST(t.total_amount AS STRING), '<NULL>')
      )) AS trip_key,
      DATE(t.lpep_pickup_datetime) AS pickup_date,
      DATE(t.lpep_dropoff_datetime) AS dropoff_date,
      t.lpep_pickup_datetime,
      t.lpep_dropoff_datetime,
      ROUND(
        (UNIX_TIMESTAMP(t.lpep_dropoff_datetime)
          - UNIX_TIMESTAMP(t.lpep_pickup_datetime)) / 60.0,
        2
      ) AS trip_duration_minutes,
      t.pu_location_id AS pickup_location_id,
      t.do_location_id AS dropoff_location_id,
      t.vendor_id,
      CASE t.vendor_id
        WHEN 1 THEN 'Creative Mobile Technologies, LLC'
        WHEN 2 THEN 'VeriFone Inc.'
        WHEN 6 THEN 'Other'
        ELSE 'Unknown'
      END AS vendor_name,
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
      t.trip_type AS trip_type_id,
      CASE t.trip_type
        WHEN 1 THEN 'Street-hail'
        WHEN 2 THEN 'Dispatch'
        ELSE 'Unknown'
      END AS trip_type_description,
      t.passenger_count,
      t.trip_distance,
      t.fare_amount,
      t.extra,
      t.mta_tax,
      t.tip_amount,
      t.tolls_amount,
      t.improvement_surcharge,
      t.congestion_surcharge,
      t.cbd_congestion_fee,
      t.total_amount,
      t.store_and_fwd_flag,
      t.qc_error_descriptions
    FROM `nyc-mobility`.nyc_silver.green_taxi_clean AS t
    WHERE t.lpep_pickup_datetime IS NOT NULL
      AND t.lpep_dropoff_datetime IS NOT NULL
  ),
  deduplicated_trips AS (
    SELECT *
    FROM trip_candidates
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY trip_key
      ORDER BY lpep_pickup_datetime DESC, lpep_dropoff_datetime DESC
    ) = 1
  ),
  trips_with_weather AS (
    SELECT
      t.*,
      w.weather_key
    FROM deduplicated_trips AS t
    LEFT JOIN `nyc-mobility`.nyc_gold.dim_weather AS w
      ON w.weather_timestamp = DATE_TRUNC('HOUR', t.lpep_pickup_datetime)
  )
  SELECT
    trip_key,
    pickup_date,
    dropoff_date,
    weather_key,
    pickup_location_id,
    dropoff_location_id,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    trip_duration_minutes,
    passenger_count,
    trip_distance,
    vendor_id,
    vendor_name,
    payment_type_id,
    payment_type,
    ratecode_id,
    ratecode_description,
    trip_type_id,
    trip_type_description,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    cbd_congestion_fee,
    total_amount,
    store_and_fwd_flag,
    qc_error_descriptions,
    CURRENT_TIMESTAMP() AS created_at
  FROM trips_with_weather
) AS source
ON target.trip_key = source.trip_key
WHEN MATCHED THEN UPDATE SET
  target.pickup_date = source.pickup_date,
  target.dropoff_date = source.dropoff_date,
  target.weather_key = source.weather_key,
  target.pickup_location_id = source.pickup_location_id,
  target.dropoff_location_id = source.dropoff_location_id,
  target.lpep_pickup_datetime = source.lpep_pickup_datetime,
  target.lpep_dropoff_datetime = source.lpep_dropoff_datetime,
  target.trip_duration_minutes = source.trip_duration_minutes,
  target.passenger_count = source.passenger_count,
  target.trip_distance = source.trip_distance,
  target.vendor_id = source.vendor_id,
  target.vendor_name = source.vendor_name,
  target.payment_type_id = source.payment_type_id,
  target.payment_type = source.payment_type,
  target.ratecode_id = source.ratecode_id,
  target.ratecode_description = source.ratecode_description,
  target.trip_type_id = source.trip_type_id,
  target.trip_type_description = source.trip_type_description,
  target.fare_amount = source.fare_amount,
  target.extra = source.extra,
  target.mta_tax = source.mta_tax,
  target.tip_amount = source.tip_amount,
  target.tolls_amount = source.tolls_amount,
  target.improvement_surcharge = source.improvement_surcharge,
  target.congestion_surcharge = source.congestion_surcharge,
  target.cbd_congestion_fee = source.cbd_congestion_fee,
  target.total_amount = source.total_amount,
  target.store_and_fwd_flag = source.store_and_fwd_flag,
  target.qc_error_descriptions = source.qc_error_descriptions
WHEN NOT MATCHED THEN INSERT (
  trip_key,
  pickup_date,
  dropoff_date,
  weather_key,
  pickup_location_id,
  dropoff_location_id,
  lpep_pickup_datetime,
  lpep_dropoff_datetime,
  trip_duration_minutes,
  passenger_count,
  trip_distance,
  vendor_id,
  vendor_name,
  payment_type_id,
  payment_type,
  ratecode_id,
  ratecode_description,
  trip_type_id,
  trip_type_description,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  congestion_surcharge,
  cbd_congestion_fee,
  total_amount,
  store_and_fwd_flag,
  qc_error_descriptions,
  created_at
)
VALUES (
  source.trip_key,
  source.pickup_date,
  source.dropoff_date,
  source.weather_key,
  source.pickup_location_id,
  source.dropoff_location_id,
  source.lpep_pickup_datetime,
  source.lpep_dropoff_datetime,
  source.trip_duration_minutes,
  source.passenger_count,
  source.trip_distance,
  source.vendor_id,
  source.vendor_name,
  source.payment_type_id,
  source.payment_type,
  source.ratecode_id,
  source.ratecode_description,
  source.trip_type_id,
  source.trip_type_description,
  source.fare_amount,
  source.extra,
  source.mta_tax,
  source.tip_amount,
  source.tolls_amount,
  source.improvement_surcharge,
  source.congestion_surcharge,
  source.cbd_congestion_fee,
  source.total_amount,
  source.store_and_fwd_flag,
  source.qc_error_descriptions,
  source.created_at
);
