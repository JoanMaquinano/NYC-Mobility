INSERT OVERWRITE `nyc-mobility`.nyc_silver.taxi_zones_clean
SELECT
    CAST(location_id AS INT) AS location_id,
    TRIM(Borough) AS borough,
    TRIM(Zone) AS zone_name,
    TRIM(service_zone) AS service_zone,
    source_file,
   ingestion_time,
   current_timestamp() AS silver_at
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY location_id
            ORDER BY ingestion_time DESC
        ) AS rn
    FROM `nyc-mobility`.nyc_bronze.taxi_zones
)
WHERE rn = 1
  AND location_id IS NOT NULL;