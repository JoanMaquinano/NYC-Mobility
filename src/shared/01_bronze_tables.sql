-- Create bronze green_taxi_raw table

-- Create bronze taxi_zones_raw table
CREATE TABLE IF NOT EXISTS `nyc-mobility`.nyc_bronze.taxi_zones_raw
(
    LocationID INT,
    Borough STRING,
    Zone STRING,
    service_zone STRING,
    ingestion_time TIMESTAMP,
    source_file STRING
)
USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name'
);
-- Create bronze traffic_advisories_raw table

-- Create bronze weather_raw table​‌
