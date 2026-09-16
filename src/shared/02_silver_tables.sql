-- Create silver green_taxi_clean table

-- Create silver taxi_zones_clean table
CREATE TABLE IF NOT EXISTS `nyc-mobility`.nyc_silver.taxi_zones_clean
(
    location_id INT,
    borough STRING,
    zone STRING,
    service_zone STRING,
    ingestion_time TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name'
);
-- Create silver traffic_advisories_clean table

-- Create silver weather_clean table​‌
