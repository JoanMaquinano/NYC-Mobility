-- Create bronze green_taxi_raw table
CREATE TABLE IF NOT EXISTS `nyc-mobility`.nyc_bronze.green_taxi
(
    vendor_id INT,
    lpep_pickup_datetime TIMESTAMP,
    lpep_dropoff_datetime TIMESTAMP,
    store_and_fwd_flag STRING,
    ratecode_id INT,
    pu_location_id INT,
    do_location_id INT,
    passenger_count INT,
    trip_distance DOUBLE,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    payment_type INT,
    trip_type INT,
    congestion_surcharge DOUBLE,
    cbd_congestion_fee DOUBLE,
    ingestion_time TIMESTAMP,
    source_file STRING
)
USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name'
);

-- Create bronze taxi_zones table
CREATE TABLE IF NOT EXISTS `nyc-mobility`.nyc_bronze.taxi_zones
(
    location_id INT,
    borough STRING,
    zone STRING,
    service_zone STRING,
    ingestion_time TIMESTAMP,
    source_file STRING
)
USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name'
);



-- Weather Bronze Table
CREATE TABLE IF NOT EXISTS `nyc-mobility`.nyc_bronze.weather (
    date STRING,
    temperature_2m STRING,
    apparent_temperature STRING,
    precipitation_probability STRING,
    rain STRING,
    weather_code STRING,
    cloud_cover STRING,
    visibility STRING,
    wind_speed_10m STRING,
    wind_gusts_10m STRING,
    month STRING,
    ingestion_timestamp TIMESTAMP,
    source_file_month STRING
)
USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name'
);
