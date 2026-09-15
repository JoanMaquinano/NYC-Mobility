CREATE SCHEMA IF NOT EXISTS workspace.taxi_silver;

CREATE OR REPLACE TABLE workspace.taxi_silver.taxi_may_clean
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS
SELECT
  CAST(LocationID AS INT)      AS location_id,
  TRIM(Borough)                AS borough,
  TRIM(Zone)                   AS zone,
  TRIM(service_zone)           AS service_zone,
  ingestion_time
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY LocationID ORDER BY ingestion_time DESC) AS rn
  FROM workspace.taxi_bronze.taxi_zone_may2026
)
WHERE rn = 1
  AND LocationID IS NOT NULL
