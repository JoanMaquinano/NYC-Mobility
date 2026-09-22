
-- Create silver green_taxi
CREATE TABLE IF NOT EXISTS nyc_mobility.nyc_silver.green_taxi_clean (
    vendor_id             INT,
    lpep_pickup_datetime  TIMESTAMP,
    lpep_dropoff_datetime TIMESTAMP,
    store_and_fwd_flag    STRING,
    ratecode_id           INT,
    pu_location_id        INT,
    do_location_id        INT,
    passenger_count       INT,
    trip_distance         DOUBLE,
    fare_amount           DOUBLE,
    extra                 DOUBLE,
    mta_tax               DOUBLE,
    tip_amount            DOUBLE,
    tolls_amount          DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount          DOUBLE,
    payment_type          INT,
    trip_type             INT,
    congestion_surcharge  DOUBLE,
    cbd_congestion_fee    DOUBLE,

    -- Every issue found on this row. Each entry is prefixed 'FAIL: ' or
    -- 'WARN: '; an empty array means the row is clean.
    qc_error_descriptions ARRAY<STRING>,

    -- ----------------------------- added -----------------------------
    -- PASS | WARN | FAIL, derived from the array above and nothing else.
    -- vw_green_taxi_valid filters on this; Gold reads that view.
    dq_status             STRING,
    -- Lineage. Bronze's source_file_recorded is a BLOCKING check; dropping the
    -- column here undoes that gate, and a bad month can no longer be traced
    -- back to the file it came from.
    source_file           STRING,
    ingestion_time        TIMESTAMP,
    silver_at             TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name'
);

-- Create silver weather table
CREATE TABLE IF NOT EXISTS nyc_mobility.nyc_silver.weather_clean (
    -- the typed hour: the key, and what Gold joins on
    weather_hour              TIMESTAMP,
    -- the raw text, kept exactly as it arrived
    `date`                    STRING,

    temperature_2m            DOUBLE,
    apparent_temperature      DOUBLE,
    precipitation_probability DOUBLE,
    rain                      DOUBLE,
    weather_code              INT,
    weather_description       STRING,
    cloud_cover               DOUBLE,
    visibility                DOUBLE,
    wind_speed_10m            DOUBLE,
    wind_gusts_10m            DOUBLE,
    `month`                   STRING,
    ingestion_timestamp       TIMESTAMP,
    source_file_month         STRING,
    -- added
    qc_error_descriptions     ARRAY<STRING>,
    dq_status                 STRING,
    silver_at                 TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name'
);

CREATE TABLE IF NOT EXISTS nyc_mobility.nyc_silver.taxi_zones_clean (
    location_id    INT       COMMENT '1 to 265. 264 and 265 mean the meter recorded no zone',
    borough        STRING,
    zone_name      STRING    COMMENT 'named zone_name, not zone: ZONE is SQL syntax',
    service_zone   STRING,
    source_file    STRING    COMMENT 'Bronze lineage: which file this lookup came from',
    ingestion_time TIMESTAMP COMMENT 'Bronze lineage: when the row landed in Bronze',
    silver_at      TIMESTAMP COMMENT 'when this row was cleaned'
)
USING DELTA
COMMENT 'TLC taxi zone lookup, deduplicated. Full refresh on every run.'
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name'
);

