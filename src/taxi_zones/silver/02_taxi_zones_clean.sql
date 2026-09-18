SET TIME ZONE 'America/New_York';
 
USE CATALOG `nyc-mobility`;
USE SCHEMA nyc_silver;

INSERT OVERWRITE `nyc-mobility`.nyc_silver.taxi_zones_clean
SELECT
    -- try_cast, not CAST: location_id is text in Bronze, and a bare cast on a
    -- malformed value takes the whole statement down. A NULL here is caught by
    -- location_id_not_null, which is blocking.
    try_cast(location_id AS INT)  AS location_id,
    TRIM(borough)                 AS borough,
    TRIM(`zone`)                  AS zone_name,
    TRIM(service_zone)            AS service_zone,
    source_file,
    ingestion_time,
    current_timestamp()           AS silver_at
FROM (
    SELECT
        *,
        -- ingestion_time is identical for all 265 rows of one load, so it
        -- cannot break a tie by itself. source_file and location_id make the
        -- ordering reproducible across runs rather than arbitrary. 
        ROW_NUMBER() OVER (
            PARTITION BY location_id
            ORDER BY ingestion_time DESC, source_file, location_id
        ) AS rn
    FROM `nyc-mobility`.nyc_bronze.taxi_zones
)
WHERE rn = 1
  AND location_id IS NOT NULL;
 
-- verify
-- Expect 265. A different number means the lookup version changed or the load
-- is partial, and every zone-level result downstream is suspect.
SELECT COUNT(*)                                        AS zones,
       COUNT(DISTINCT location_id)                     AS distinct_ids,
       SUM(CASE WHEN location_id IS NULL THEN 1 ELSE 0 END) AS unparseable_ids,
       MIN(location_id)                                AS min_id,
       MAX(location_id)                                AS max_id
FROM   `nyc-mobility`.nyc_silver.taxi_zones_clean;
 
-- The two ids that mean "the meter did not record a zone". Silver is where
-- they most often get lost -- a BETWEEN 1 AND 263 written anywhere removes
-- them, and then every trip with an unrecorded zone has nothing to join to.
-- Expect two rows.
SELECT location_id, borough, zone_name, service_zone
FROM   `nyc-mobility`.nyc_silver.taxi_zones_clean
WHERE  location_id IN (264, 265)
ORDER  BY location_id;
 
-- The three airport zones the airport split in Gold is built on.
-- Expect Newark, JFK and LaGuardia.
SELECT location_id, borough, zone_name, service_zone
FROM   `nyc-mobility`.nyc_silver.taxi_zones_clean
WHERE  location_id IN (1, 132, 138)
ORDER  BY location_id;

-- Ids present in Bronze but missing from Silver, and the reverse. Expect no
-- rows either way: the only thing the cleaning removes is a null key.
SELECT 'in bronze, not in silver' AS gap, b.id
FROM  (SELECT DISTINCT try_cast(location_id AS INT) AS id
       FROM `nyc-mobility`.nyc_bronze.taxi_zones
       WHERE location_id IS NOT NULL) b
LEFT  JOIN `nyc-mobility`.nyc_silver.taxi_zones_clean s ON b.id = s.location_id
WHERE s.location_id IS NULL
UNION ALL
SELECT 'in silver, not in bronze', s.location_id
FROM       `nyc-mobility`.nyc_silver.taxi_zones_clean s
LEFT  JOIN (SELECT DISTINCT try_cast(location_id AS INT) AS id
            FROM `nyc-mobility`.nyc_bronze.taxi_zones
            WHERE location_id IS NOT NULL) b ON b.id = s.location_id
WHERE b.id IS NULL;
-- Zone names shared by more than one id. 264 and 265 are both "Unknown" by
-- design, so expect exactly that pair and nothing else. Anything more means
-- two real zones carry the same name and a name-based join would merge them.
SELECT zone_name, COUNT(*) AS ids, collect_list(location_id) AS location_ids
FROM   `nyc-mobility`.nyc_silver.taxi_zones_clean
GROUP  BY zone_name
HAVING COUNT(*) > 1
ORDER  BY ids DESC, zone_name;
 