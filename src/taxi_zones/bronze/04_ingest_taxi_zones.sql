MERGE INTO `nyc-mobility`.nyc_bronze.taxi_zones AS t
USING (
    SELECT
        LocationID AS location_id,
        Borough AS borough,
        Zone AS zone,
        service_zone,
        current_timestamp() AS ingestion_time,
        element_at(split(_metadata.file_path, '/'), -1) AS source_file
    FROM read_files(
        '/Volumes/workspace/default/ftw-b12-de/groups/week-08/group-d/taxi-zone-lookup/',
        format => 'csv',
        header => 'true'
    )
) AS s
ON t.location_id = s.location_id
WHEN NOT MATCHED THEN INSERT (
    location_id, borough, zone, service_zone, ingestion_time, source_file
)
VALUES (
    s.location_id, s.borough, s.zone, s.service_zone, s.ingestion_time, s.source_file
);