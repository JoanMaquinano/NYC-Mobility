MERGE INTO `nyc-mobility`.nyc_gold.dim_taxi_zone t
USING (
    SELECT
        location_id,
        CASE
            WHEN TRIM(borough) IN ('N/A', 'Unknown', '') THEN 'Unknown'
            ELSE TRIM(borough)
        END AS borough,
        TRIM(zone_name) AS zone_name,
        CASE
            WHEN TRIM(service_zone) IN ('N/A', 'Unknown', '') THEN 'Unknown'
            ELSE TRIM(service_zone)
        END AS service_zone
    FROM `nyc-mobility`.nyc_silver.taxi_zones_clean
) s
ON t.location_id = s.location_id
WHEN MATCHED THEN
UPDATE SET
    t.borough = s.borough,
    t.zone_name = s.zone_name,
    t.service_zone = s.service_zone
WHEN NOT MATCHED THEN
INSERT (
    location_id,
    borough,
    zone_name,
    service_zone
)
VALUES (
    s.location_id,
    s.borough,
    s.zone_name,
    s.service_zone
);
