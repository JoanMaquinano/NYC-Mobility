-- Gold — dim_date


SET TIME ZONE 'America/New_York';
USE CATALOG `nyc-mobility`;


-- 1. The bounds, before anything is built
--
-- Look at this first. If `span_days` is large, the `outlier_dates` query
-- below names the rows responsible — they are almost certainly the TLC
-- quirk, and they are real trips that Silver deliberately kept.


CREATE OR REPLACE TEMPORARY VIEW vw_dim_date_bounds AS
SELECT
    LEAST(w_min, t_min) AS date_from,
    GREATEST(w_max, t_max) AS date_to,
    datediff(GREATEST(w_max, t_max), LEAST(w_min, t_min)) + 1 AS span_days,
    w_min, w_max, t_min, t_max
FROM (
    SELECT
        (SELECT MIN(to_date(weather_hour)) FROM nyc_silver.vw_weather_valid
         WHERE weather_hour IS NOT NULL)                                   AS w_min,
        (SELECT MAX(to_date(weather_hour)) FROM nyc_silver.vw_weather_valid
         WHERE weather_hour IS NOT NULL)                                   AS w_max,
        -- Pickup AND dropoff: a trip starting at 23:50 on the last day ends on
        -- the next one, and dropoff_date is a foreign key too.
        (SELECT LEAST(MIN(DATE(lpep_pickup_datetime)), MIN(DATE(lpep_dropoff_datetime)))
         FROM   nyc_silver.green_taxi_clean
         WHERE  lpep_pickup_datetime IS NOT NULL
           AND  lpep_dropoff_datetime IS NOT NULL)                         AS t_min,
        (SELECT GREATEST(MAX(DATE(lpep_pickup_datetime)), MAX(DATE(lpep_dropoff_datetime)))
         FROM   nyc_silver.green_taxi_clean
         WHERE  lpep_pickup_datetime IS NOT NULL
           AND  lpep_dropoff_datetime IS NOT NULL)                         AS t_max
);


SELECT date_from, date_to, span_days,
       w_min AS weather_from, w_max AS weather_to,
       t_min AS trips_from,   t_max AS trips_to
FROM   vw_dim_date_bounds;


-- The trips responsible for stretching the range, if any. Expect a handful --
-- the TLC quirk. A large number here means something else is wrong.
SELECT DATE(lpep_pickup_datetime) AS pickup_date,
       COUNT(*)                   AS trips,
       MIN(source_file)           AS example_file
FROM   nyc_silver.green_taxi_clean
WHERE  lpep_pickup_datetime IS NOT NULL
  AND  DATE(lpep_pickup_datetime) NOT BETWEEN
       (SELECT w_min FROM vw_dim_date_bounds)
   AND (SELECT w_max FROM vw_dim_date_bounds)
GROUP  BY DATE(lpep_pickup_datetime)
ORDER  BY pickup_date;


-- ## A guard on the span
--
-- `sequence()` builds the whole array in memory before `explode` unpacks
-- it. A single corrupted year — 1900, or 9999 — would ask for millions of
-- rows, and the failure would be an out-of-memory error rather than
-- anything that names its cause.
--
-- Fifty years is far beyond any legitimate range here (LPEP started in
-- 2009) and far below anything that hurts.




SELECT CASE WHEN span_days > 18262
    THEN raise_error(CONCAT(
           'dim_date span is ', CAST(span_days AS STRING), ' days (',
           CAST(date_from AS STRING), ' to ', CAST(date_to AS STRING),
           ') -- over 50 years. That is a corrupted date, not a wide window. ',
           'Run the outlier query above to find it; do not widen this guard.'))
    ELSE CONCAT('OK - building ', CAST(span_days AS STRING), ' days from ',
                CAST(date_from AS STRING), ' to ', CAST(date_to AS STRING))
END AS precondition
FROM vw_dim_date_bounds;


-- 2. Build


MERGE INTO `nyc-mobility`.nyc_gold.dim_date AS target
USING (
  SELECT
    CAST(DATE_FORMAT(full_date, 'yyyyMMdd') AS INT) AS date_key,
    full_date,
    YEAR(full_date)                                 AS year,
    QUARTER(full_date)                              AS quarter,
    MONTH(full_date)                                AS month,
    MONTHNAME(full_date)                            AS month_name,
    DAYOFWEEK(full_date)                            AS day_of_week,
    DAYNAME(full_date)                              AS day_name,
    DAYOFWEEK(full_date) IN (1, 7)                  AS is_weekend
  FROM (
    SELECT explode(sequence(date_from, date_to, INTERVAL 1 DAY)) AS full_date
    FROM   vw_dim_date_bounds
  ) calendar
) AS source
ON target.date_key <=> source.date_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

--3. Verify

SELECT
    (SELECT COUNT(*) FROM `nyc-mobility`.nyc_gold.fact_taxi_trip f
     LEFT JOIN `nyc-mobility`.nyc_gold.dim_date d ON f.pickup_date = d.full_date
     WHERE f.pickup_date IS NOT NULL AND d.full_date IS NULL)   AS trips_with_no_pickup_date_row,
    (SELECT COUNT(*) FROM `nyc-mobility`.nyc_gold.fact_taxi_trip f
     LEFT JOIN `nyc-mobility`.nyc_gold.dim_date d ON f.dropoff_date = d.full_date
     WHERE f.dropoff_date IS NOT NULL AND d.full_date IS NULL)  AS trips_with_no_dropoff_date_row;

-- No gaps: a contiguous range has exactly span_days rows and no missing day.
SELECT
    (SELECT COUNT(*)   FROM `nyc-mobility`.nyc_gold.dim_date)      AS rows_built,
    (SELECT span_days  FROM vw_dim_date_bounds)                    AS span_days,
    (SELECT COUNT(*) - COUNT(DISTINCT date_key)
     FROM `nyc-mobility`.nyc_gold.dim_date)                        AS duplicate_keys,
    datediff((SELECT MAX(full_date) FROM `nyc-mobility`.nyc_gold.dim_date),
             (SELECT MIN(full_date) FROM `nyc-mobility`.nyc_gold.dim_date)) + 1
      - (SELECT COUNT(*) FROM `nyc-mobility`.nyc_gold.dim_date)    AS missing_days;
-- Weekends, as a sanity check on the dayofweek convention. A 2026 calendar
-- year holds 104 weekend days; a partial range holds proportionally fewer.
-- If this comes back at roughly 2/7 of the total you have Sat+Sun; if it is
-- Friday and Saturday, someone changed IN (1, 7) to IN (6, 7).
SELECT day_name, day_of_week, is_weekend, COUNT(*) AS days
FROM   `nyc-mobility`.nyc_gold.dim_date
GROUP  BY day_name, day_of_week, is_weekend
ORDER  BY day_of_week;

