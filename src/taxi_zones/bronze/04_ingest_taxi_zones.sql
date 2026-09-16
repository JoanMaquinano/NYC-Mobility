-- Create bronze taxi_zones table
CREATE TABLE IF NOT EXISTS workspace.taxi_bronze.taxi_zones
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

INSERT INTO workspace.taxi_bronze.taxi_zones
SELECT
    LocationID,
    Borough,
    Zone,
    service_zone,
    current_timestamp() AS ingestion_time,
    _metadata.file_path AS source_file
FROM read_files(
    '/Volumes/workspace/default/ftw-b12-de/groups/week-08/group-d/taxi-zone-lookup/',
    format => 'csv',
    header => 'true'
);