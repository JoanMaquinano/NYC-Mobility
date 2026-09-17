-- NYC Green Taxi Bronze Ingestion
-- Parameter: year_month (format YYYY-MM, e.g. "2025-04")

-- Ingest Green Taxi data for the specified year-month from the raw files volume
-- MERGE INTO is idempotent: re-running with the same year_month will not reload
MERGE INTO `nyc-mobility`.nyc_bronze.green_taxi AS t
USING (
    SELECT
        VendorID AS vendor_id,
        CAST(lpep_pickup_datetime AS TIMESTAMP) AS lpep_pickup_datetime,
        CAST(lpep_dropoff_datetime AS TIMESTAMP) AS lpep_dropoff_datetime,
        store_and_fwd_flag,
        CAST(RatecodeID AS INT) AS ratecode_id,
        PULocationID AS pu_location_id,
        DOLocationID AS do_location_id,
        CAST(passenger_count AS INT) AS passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        CAST(payment_type AS INT) AS payment_type,
        CAST(trip_type AS INT) AS trip_type,
        congestion_surcharge,
        cbd_congestion_fee,
        current_timestamp() AS ingestion_time,
        concat('green_tripdata_', :year_month, '.parquet') AS source_file
    FROM read_files(concat('/Volumes/workspace/default/ftw-b12-de/groups/week-08/group-d/green-taxi/green_tripdata_', :year_month, '.parquet'), format => 'parquet')
) AS s
ON t.source_file = s.source_file
WHEN NOT MATCHED THEN INSERT (
    vendor_id, lpep_pickup_datetime, lpep_dropoff_datetime, store_and_fwd_flag,
    ratecode_id, pu_location_id, do_location_id, passenger_count, trip_distance,
    fare_amount, extra, mta_tax, tip_amount, tolls_amount,
    improvement_surcharge, total_amount, payment_type, trip_type,
    congestion_surcharge, cbd_congestion_fee, ingestion_time, source_file
)
VALUES (
    s.vendor_id, s.lpep_pickup_datetime, s.lpep_dropoff_datetime, s.store_and_fwd_flag,
    s.ratecode_id, s.pu_location_id, s.do_location_id, s.passenger_count, s.trip_distance,
    s.fare_amount, s.extra, s.mta_tax, s.tip_amount, s.tolls_amount,
    s.improvement_surcharge, s.total_amount, s.payment_type, s.trip_type,
    s.congestion_surcharge, s.cbd_congestion_fee, s.ingestion_time, s.source_file
)