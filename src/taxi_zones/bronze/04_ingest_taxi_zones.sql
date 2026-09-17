CREATE OR REPLACE TABLE `nyc-mobility`.nyc_bronze.taxi_zones
SELECT
    LocationID AS location_id,
    Borough AS borough,
    Zone AS zone,
    service_zone,
    current_timestamp() AS ingestion_time,
    _metadata.file_path AS source_file
FROM read_files(
    '/Volumes/workspace/default/ftw-b12-de/groups/week-08/group-d/taxi-zone-lookup/',
    format => 'csv',
    header => 'true'
)
USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name'
);
