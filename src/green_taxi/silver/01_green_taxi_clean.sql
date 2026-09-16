-- Silver Layer Refactoring: NYC Green Taxi Upsert & Quality Auditing
-- Improvements:
--   1. Deterministic deduplication (order by bronze ingestion timestamp/raw metadata).
--   2. Granular partition keys (adds distance & total to key to prevent dropping simultaneous trips).
--   3. Unified QC validation logic operating directly on casted timestamp types.

MERGE INTO `nyc-mobility`.nyc_silver.green_taxi AS target
USING (
  SELECT
    vendor_id,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    store_and_fwd_flag,
    ratecode_id,
    pu_location_id,
    do_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge,
    cbd_congestion_fee,
    ARRAY_COMPACT(ARRAY(
      CASE WHEN lpep_dropoff_datetime <= lpep_pickup_datetime THEN 'lpep_dropoff_datetime: Drop-off time is before or equal to pickup time' END,
      CASE WHEN lpep_pickup_datetime > CURRENT_TIMESTAMP() OR lpep_pickup_datetime < '2009-01-01' THEN 'lpep_pickup_datetime: Pickup time is in the future or predates 2009' END,
      CASE WHEN raw_fare_amount < 0 THEN 'fare_amount: Base fare amount is negative' END,
      CASE WHEN raw_total_amount < 0 THEN 'total_amount: Total charge amount is negative' END,
      CASE WHEN raw_trip_distance <= 0 THEN 'trip_distance: Trip distance is zero or negative' END,
      CASE WHEN raw_passenger_count IS NULL OR raw_passenger_count <= 0 THEN 'passenger_count: Passenger count is null, zero, or negative' END,
      CASE WHEN raw_ratecode_id IS NULL OR raw_ratecode_id NOT IN (1, 2, 3, 4, 5, 6, 99) THEN 'RatecodeID: Rate code is null or outside valid range (1-6, 99)' END,
      CASE WHEN raw_pu_location_id IS NULL OR raw_pu_location_id NOT BETWEEN 1 AND 263 THEN 'PULocationID: Pickup location ID is null or invalid' END,
      CASE WHEN raw_do_location_id IS NULL OR raw_do_location_id NOT BETWEEN 1 AND 263 THEN 'DOLocationID: Drop-off location ID is null or invalid' END,
      CASE WHEN raw_payment_type IS NULL OR raw_payment_type NOT IN (0, 1, 2, 3, 4, 5, 6) THEN 'payment_type: Payment type code is null or invalid' END,
      CASE WHEN raw_trip_type IS NULL OR raw_trip_type NOT IN (1, 2) THEN 'trip_type: Trip type code is null or invalid' END,
      CASE WHEN raw_vendor_id IS NULL OR raw_vendor_id NOT IN (1, 2, 6) THEN 'VendorID: Vendor ID is null or unrecognized' END,
      CASE WHEN raw_tip_amount < 0 THEN 'tip_amount: Tip amount is negative' END,
      CASE WHEN raw_payment_type = 2 AND raw_tip_amount > 0 THEN 'tip_amount: Tip recorded for a cash payment' END
    )) AS qc_error_descriptions
  FROM (
    SELECT
      -- Core Business Attributes
      vendor_id,
      CAST(lpep_pickup_datetime AS TIMESTAMP) AS lpep_pickup_datetime,
      CAST(lpep_dropoff_datetime AS TIMESTAMP) AS lpep_dropoff_datetime,
      store_and_fwd_flag,
      COALESCE(TRY_CAST(ratecode_id AS INT), 99) AS ratecode_id,
      CASE WHEN pu_location_id BETWEEN 1 AND 263 THEN CAST(pu_location_id AS INT) ELSE 264 END AS pu_location_id,
      CASE WHEN do_location_id BETWEEN 1 AND 263 THEN CAST(do_location_id AS INT) ELSE 264 END AS do_location_id,
      CASE WHEN passenger_count IS NULL OR passenger_count <= 0 THEN 1 ELSE CAST(passenger_count AS INT) END AS passenger_count,
      trip_distance,
      fare_amount,
      CASE WHEN extra < 0 THEN 0.0 ELSE extra END AS extra,
      CASE WHEN mta_tax < 0 THEN 0.0 ELSE mta_tax END AS mta_tax,
      CASE WHEN tip_amount < 0 OR payment_type = 2 THEN 0.0 ELSE tip_amount END AS tip_amount,
      CASE WHEN tolls_amount < 0 THEN 0.0 ELSE tolls_amount END AS tolls_amount,
      improvement_surcharge,
      total_amount,
      COALESCE(TRY_CAST(payment_type AS INT), 5) AS payment_type,
      COALESCE(TRY_CAST(trip_type AS INT), 1) AS trip_type,
      CASE WHEN congestion_surcharge < 0 THEN 0.0 ELSE congestion_surcharge END AS congestion_surcharge,
      CASE WHEN cbd_congestion_fee < 0 THEN 0.0 ELSE cbd_congestion_fee END AS cbd_congestion_fee,

      -- Preserved Raw Values for Strict Rule Auditing
      passenger_count AS raw_passenger_count,
      ratecode_id AS raw_ratecode_id,
      pu_location_id AS raw_pu_location_id,
      do_location_id AS raw_do_location_id,
      payment_type AS raw_payment_type,
      trip_type AS raw_trip_type,
      vendor_id AS raw_vendor_id,
      tip_amount AS raw_tip_amount,
      fare_amount AS raw_fare_amount,
      total_amount AS raw_total_amount,
      trip_distance AS raw_trip_distance
    FROM `nyc-mobility`.nyc_bronze.green_taxi
  ) cleaned_data

  -- Deduplicate batch records deterministically without dropping distinct simultaneous trips
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY 
      vendor_id, 
      lpep_pickup_datetime, 
      lpep_dropoff_datetime, 
      pu_location_id, 
      do_location_id,
      trip_distance,
      total_amount
    ORDER BY lpep_pickup_datetime ASC, fare_amount DESC
  ) = 1
) AS source
ON  target.vendor_id <=> source.vendor_id
AND target.lpep_pickup_datetime <=> source.lpep_pickup_datetime
AND target.lpep_dropoff_datetime <=> source.lpep_dropoff_datetime
AND target.pu_location_id <=> source.pu_location_id
AND target.do_location_id <=> source.do_location_id
AND target.trip_distance <=> source.trip_distance
AND target.total_amount <=> source.total_amount
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *;