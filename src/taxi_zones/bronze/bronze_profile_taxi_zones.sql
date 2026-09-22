-- 1. Row count and basic shape
SELECT COUNT(*) AS total_rows
FROM nyc_mobility.nyc_bronze.taxi_zones;

-- 2. Null counts per column
SELECT
  COUNT(*) AS total_rows,
  COUNT(*) - COUNT(location_id) AS null_location_id,
  COUNT(*) - COUNT(borough) AS null_borough,
  COUNT(*) - COUNT(zone) AS null_zone,
  COUNT(*) - COUNT(service_zone) AS null_service_zone
FROM nyc_mobility.nyc_bronze.taxi_zones;

-- 3. Cardinality / uniqueness check (is location_id actually a valid key?)
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT location_id) AS distinct_location_ids
FROM nyc_mobility.nyc_bronze.taxi_zones;

-- 4. Duplicate location_id, if any (to inspect the actual offending rows)
SELECT location_id, COUNT(*) AS occurrences
FROM nyc_mobility.nyc_bronze.taxi_zones
GROUP BY location_id
HAVING COUNT(*) > 1;

-- 5. Value distribution for categorical columns (catches typos, unexpected values)
SELECT borough, COUNT(*) AS row_count
FROM nyc_mobility.nyc_bronze.taxi_zones
GROUP BY borough
ORDER BY row_count DESC;

SELECT service_zone, COUNT(*) AS row_count
FROM nyc_mobility.nyc_bronze.taxi_zones
GROUP BY service_zone
ORDER BY row_count DESC;

-- 6. Sample of raw values to eyeball formatting issues (extra whitespace, casing, etc.)
SELECT DISTINCT zone
FROM nyc_mobility.nyc_bronze.taxi_zones
ORDER BY zone
LIMIT 20;