CREATE OR REPLACE TABLE `nyc-mobility`.nyc_gold.dim_taxi_zone
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS
SELECT
  location_id                                          AS zone_key,
  location_id,
  CASE 
    WHEN TRIM(borough) IN ('N/A', 'Unknown', '') THEN 'Unknown'
    ELSE TRIM(borough)
  END                                                   AS borough,
  zone,
  CASE 
    WHEN TRIM(service_zone) IN ('N/A', 'Unknown', '') THEN 'Unknown'
    ELSE TRIM(service_zone)
  END                                                   AS service_zone
FROM `nyc-mobility`.nyc_silver.taxi_zones_clean;