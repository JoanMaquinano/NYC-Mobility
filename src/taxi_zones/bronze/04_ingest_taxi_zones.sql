CREATE TABLE IF NOT EXISTS workspace.taxi_bronze.taxi_zone_may2026
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS
SELECT *,
  current_timestamp() AS ingestion_time,
  _metadata.file_path AS source_file
FROM read_files(
  '/Volumes/workspace/default/ftw-b12-de/groups/week-08/group-d/taxi-zone-lookup/',
  format => 'csv',
  header => 'true'
)