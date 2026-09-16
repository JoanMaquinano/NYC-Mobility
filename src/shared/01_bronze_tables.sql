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
