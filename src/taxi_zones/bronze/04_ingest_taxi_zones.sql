
CREATE OR REPLACE TABLE workspace.taxi_silver.taxi_zone_may
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS
SELECT
  CAST(LocationID AS INT) AS location_id,
  CASE 
    WHEN TRIM(Borough) IN ('N/A', 'Unknown', '') THEN 'Unknown'
    ELSE TRIM(Borough)
  END AS borough,
  TRIM(Zone) AS zone,
  CASE 
    WHEN TRIM(service_zone) IN ('N/A', 'Unknown', '') THEN 'Unknown'
    ELSE TRIM(service_zone)
  END AS service_zone,
  ingestion_time
FROM workspace.taxi_bronze.taxi_zone_may2026;

-- PROFILE: ROW COUNT

SELECT COUNT(*) AS total_rows
FROM workspace.taxi_silver.taxi_zone_may;

-- PROFILE: NULL COUNTS PER COLUMN

SELECT
  COUNT(*)                                    AS total_rows,
  COUNT(*) - COUNT(location_id)               AS null_location_id,
  COUNT(*) - COUNT(borough)                   AS null_borough,
  COUNT(*) - COUNT(zone)                      AS null_zone,
  COUNT(*) - COUNT(service_zone)              AS null_service_zone
FROM workspace.taxi_silver.taxi_zone_may;

--  PROFILE: UNIQUENESS / CARDINALITY OF location_id (primary key check)

SELECT
  COUNT(*)                     AS total_rows,
  COUNT(DISTINCT location_id)  AS distinct_location_ids
FROM workspace.taxi_silver.taxi_zone_may;

-- PROFILE: DUPLICATE location_id ROWS (should return zero rows)

SELECT location_id, COUNT(*) AS occurrences
FROM workspace.taxi_silver.taxi_zone_may
GROUP BY location_id
HAVING COUNT(*) > 1;

--  PROFILE: VALUE DISTRIBUTION — borough

SELECT borough, COUNT(*) AS row_count
FROM workspace.taxi_silver.taxi_zone_may
GROUP BY borough
ORDER BY row_count DESC;

--  PROFILE: VALUE DISTRIBUTION — service_zone

SELECT service_zone, COUNT(*) AS row_count
FROM workspace.taxi_silver.taxi_zone_may
GROUP BY service_zone
ORDER BY row_count DESC;


-- PROFILE: DOES 'Unknown' IN service_zone CORRELATE WITH 'Unknown' IN borough?

-- PROFILE: DOES 'Unknown'/'N/A' IN service_zone CORRELATE WITH 'Unknown'/'N/A' IN borough?

SELECT borough, service_zone, COUNT(*) AS row_count
FROM workspace.taxi_silver.taxi_zone_may
WHERE service_zone IN ('N/A', 'Unknown', '') 
   OR borough IN ('N/A', 'Unknown', '')
GROUP BY borough, service_zone
ORDER BY row_count DESC;

-- QUALITY GATE: FAIL-LOUD ASSERTION ON location_id UNIQUENESS
-- Returns a row (alert) only if duplicates are detected

SELECT 'DUPLICATE LOCATION_ID DETECTED IN SILVER' AS alert
WHERE (
  SELECT COUNT(*) FROM workspace.taxi_silver.taxi_zone_may
) != (
  SELECT COUNT(DISTINCT location_id) FROM workspace.taxi_silver.taxi_zone_may
);

-- QUALITY GATE: FAIL-LOUD ASSERTION ON NULL location_id
-- Returns a row (alert) only if any location_id is null

SELECT 'NULL LOCATION_ID DETECTED IN SILVER' AS alert
WHERE EXISTS (
  SELECT 1 FROM workspace.taxi_silver.taxi_zone_may WHERE location_id IS NULL
);

-- Distribution skew: what % of zones fall in each borough?
SELECT 
  borough,
  COUNT(*) AS zone_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM workspace.taxi_silver.taxi_zone_may
GROUP BY borough
ORDER BY zone_count DESC;
