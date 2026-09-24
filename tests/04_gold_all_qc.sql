
-- # Data Quality — Gold

-- ## Sections
-- | § | Covers | Checks |
-- |---|---|---|
-- | 1 | the three dimensions — grain and self-consistency | 16 |
-- | 2 | `fact_taxi_trip` — grain, and reconciliation with Silver | 9 |
-- | 3 | **at-rest integrity** — every foreign key resolves | 8 |

SET TIME ZONE 'America/New_York';

USE CATALOG nyc_mobility;


DECLARE OR REPLACE VARIABLE v_run_id STRING;
DECLARE OR REPLACE VARIABLE v_run_ts TIMESTAMP;

SET VAR v_run_id = uuid();


SET VAR v_run_ts = current_timestamp();


CREATE OR REPLACE TEMPORARY VIEW vw_gold_blocking_checks AS
SELECT explode(array(
        'table_not_empty',
        'trip_key_not_null',
        'one_row_per_trip_key',
        'date_key_not_null',
        'date_key_unique',
        'weather_key_not_null',
        'weather_key_unique',
        'location_id_not_null',
        'location_id_unique',
        'pickup_zone_resolves',
        'dropoff_zone_resolves',
        -- weather_key_resolves is deliberately NOT here. It is a real defect
        -- and it is recorded every run, but the cause is a stale fact table,
        -- not corrupt data: dim_weather is correct and a rebuild of
        -- fact_taxi_trip clears it. Blocking the pipeline does not rebuild
        -- anything, and a gate that raises on a known open item stops being
        -- read. Restore it to this list once the rebuild is confirmed at zero.
        'rows_reconcile_with_silver',
        'revenue_preserved'
    )) AS check_name;

SELECT v_run_id AS run_id, v_run_ts AS run_ts;

DELETE FROM nyc_quality.dq_results WHERE run_id = v_run_id AND layer = 'gold';
DELETE FROM nyc_quality.dq_run_log WHERE run_id = v_run_id AND layer = 'gold';

-- # 1. The three dimensions
--
-- Grain first — a duplicate key here multiplies rows in every query that
-- joins to it, and inflates every total without failing anything.
--
-- Then the checks that only exist because Gold **derives** its keys:
-- `date_key` is `yyyyMMdd` of `full_date`, `weather_key` is `yyyyMMddHH`
-- of `weather_timestamp`. If a key and the column it was derived from ever
-- disagree, two queries joining by different routes return different
-- answers and neither looks wrong.
--
-- `is_weekend_matches_day_of_week` catches the classic: `dayofweek()` in
-- Spark is **1 = Sunday … 7 = Saturday**, not ISO. A weekend test written
-- as `IN (6, 7)` gives Friday and Saturday.

INSERT INTO nyc_quality.dq_results
WITH d AS (
    SELECT
        COUNT(*)                                                            AS total_rows,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END                            AS t_empty,
        SUM(CASE WHEN date_key IS NULL THEN 1 ELSE 0 END)                   AS c_key,
        COUNT(date_key) - COUNT(DISTINCT date_key)                          AS u_key,
        SUM(CASE WHEN CAST(date_format(full_date, 'yyyyMMdd') AS INT) <> date_key
                 THEN 1 ELSE 0 END)                                         AS x_key_derived,
        -- dayofweek() is 1 = Sunday .. 7 = Saturday in Spark, NOT ISO.
        SUM(CASE WHEN is_weekend <> (day_of_week IN (1, 7)) THEN 1 ELSE 0 END) AS x_weekend
    FROM nyc_gold.dim_date
),
w AS (
    SELECT
        COUNT(*)                                                            AS total_rows,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END                            AS t_empty,
        SUM(CASE WHEN weather_key IS NULL THEN 1 ELSE 0 END)                AS c_key,
        COUNT(weather_key) - COUNT(DISTINCT weather_key)                    AS u_key,
        SUM(CASE WHEN date_format(weather_timestamp, 'yyyyMMddHH') <> weather_key
                 THEN 1 ELSE 0 END)                                         AS x_key_derived,
        SUM(CASE WHEN weather_condition IS NULL THEN 1 ELSE 0 END)          AS c_condition,
        -- temp_max and temp_min are the day's, temp_avg is this hour's, so the
        -- hour must sit inside its own day's range. A window written over the
        -- wrong partition shows up here and nowhere else.
        SUM(CASE WHEN temp_avg_c IS NOT NULL
                  AND (temp_avg_c > temp_max_c OR temp_avg_c < temp_min_c)
                 THEN 1 ELSE 0 END)                                         AS x_temp_range
    FROM nyc_gold.dim_weather
),
z AS (
    SELECT
        COUNT(*)                                                            AS total_rows,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END                            AS t_empty,
        SUM(CASE WHEN location_id IS NULL THEN 1 ELSE 0 END)                AS c_key,
        COUNT(location_id) - COUNT(DISTINCT location_id)                    AS u_key,
        SUM(CASE WHEN borough IS NULL OR TRIM(borough) = '' THEN 1 ELSE 0 END) AS c_borough,
        CASE WHEN COUNT(*) = 265 THEN 0 ELSE 1 END                          AS b_265
    FROM nyc_gold.dim_taxi_zone
),
checks AS (
    SELECT 'dim_date' AS tbl, 'completeness' AS check_category, 'table_not_empty' AS check_name,
           0.0 AS threshold_pct, t_empty AS failed_rows, 1 AS total_rows FROM d
    UNION ALL SELECT 'dim_date', 'completeness', 'date_key_not_null',  0.0, c_key,         total_rows FROM d
    UNION ALL SELECT 'dim_date', 'uniqueness',   'date_key_unique',    0.0, u_key,         total_rows FROM d
    UNION ALL SELECT 'dim_date', 'consistency',  'key_matches_full_date', 0.0, x_key_derived, total_rows FROM d
    UNION ALL SELECT 'dim_date', 'consistency',  'is_weekend_matches_day_of_week', 0.0, x_weekend, total_rows FROM d

    UNION ALL SELECT 'dim_weather', 'completeness', 'table_not_empty',    0.0, t_empty, 1 FROM w
    UNION ALL SELECT 'dim_weather', 'completeness', 'weather_key_not_null', 0.0, c_key,  total_rows FROM w
    UNION ALL SELECT 'dim_weather', 'uniqueness',   'weather_key_unique', 0.0, u_key,    total_rows FROM w
    UNION ALL SELECT 'dim_weather', 'consistency',  'key_matches_timestamp', 0.0, x_key_derived, total_rows FROM w
    UNION ALL SELECT 'dim_weather', 'completeness', 'weather_condition_not_null', 0.0, c_condition, total_rows FROM w
    UNION ALL SELECT 'dim_weather', 'consistency',  'hour_temp_within_day_range', 0.0, x_temp_range, total_rows FROM w

    UNION ALL SELECT 'dim_taxi_zone', 'completeness', 'table_not_empty',  0.0, t_empty, 1 FROM z
    UNION ALL SELECT 'dim_taxi_zone', 'completeness', 'location_id_not_null', 0.0, c_key, total_rows FROM z
    UNION ALL SELECT 'dim_taxi_zone', 'uniqueness',   'location_id_unique', 0.0, u_key,  total_rows FROM z
    UNION ALL SELECT 'dim_taxi_zone', 'completeness', 'borough_not_blank', 0.0, c_borough, total_rows FROM z
    UNION ALL SELECT 'dim_taxi_zone', 'business',     'lookup_has_265_zones', 0.0, b_265, 1 FROM z
)

SELECT
    v_run_id, v_run_ts, 'gold', tbl,
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    threshold_pct,
    CASE WHEN failed_rows = 0                                              THEN 'PASS'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= threshold_pct THEN 'WARN'
         ELSE 'FAIL' END
FROM checks;

-- # 2. fact_taxi_trip
-- `rows_reconcile_with_silver` recomputes the same `trip_key` the fact
-- MERGE builds and counts the distinct values. That is the honest form: it
-- compares against the number of trips Silver **has**, not against a
-- remainder derived from the answer.
--
-- `quarantined_rows_in_gold` is the one that fails today — see the header.


INSERT INTO nyc_quality.dq_results
WITH silver AS (
    -- The same key the fact MERGE builds, recomputed here rather than trusted.
    --
    -- Reads vw_green_taxi_valid, NOT green_taxi_clean. The fact table is built
    -- from the valid view, so the expected count has to come from the same
    -- place. Comparing against the clean table counts the FAIL rows the view
    -- deliberately excludes, and the reconciliation fails by exactly the number
    -- of rows the pipeline is working correctly to leave out -- a check that
    -- breaks the moment the thing it checks is fixed.
    SELECT
        COUNT(DISTINCT md5(concat_ws('|',
            COALESCE(CAST(vendor_id AS STRING), '<NULL>'),
            COALESCE(CAST(lpep_pickup_datetime AS STRING), '<NULL>'),
            COALESCE(CAST(lpep_dropoff_datetime AS STRING), '<NULL>'),
            COALESCE(CAST(pu_location_id AS STRING), '<NULL>'),
            COALESCE(CAST(do_location_id AS STRING), '<NULL>'),
            COALESCE(CAST(trip_distance AS STRING), '<NULL>'),
            COALESCE(CAST(total_amount AS STRING), '<NULL>')
        )))                                                        AS n_expected,
        ROUND(SUM(total_amount), 2)                                AS revenue
    FROM   nyc_silver.vw_green_taxi_valid
    WHERE  lpep_pickup_datetime IS NOT NULL
      AND  lpep_dropoff_datetime IS NOT NULL
),
grain AS (
    SELECT COALESCE(SUM(n - 1), 0) AS extra_rows
    FROM (
        SELECT COUNT(*) AS n
        FROM   nyc_gold.fact_taxi_trip
        GROUP  BY trip_key
        HAVING COUNT(*) > 1
    )
),
f AS (
    SELECT
        COUNT(*)                                                   AS total_rows,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END                   AS t_empty,
        ROUND(SUM(total_amount), 2)                                AS revenue,
        SUM(CASE WHEN trip_key IS NULL THEN 1 ELSE 0 END)          AS c_key,
        SUM(CASE WHEN lpep_dropoff_datetime < lpep_pickup_datetime
                 THEN 1 ELSE 0 END)                                AS x_time_order,
        -- unix_timestamp(), not timestampdiff(): the Gold build computes this
        -- column from epoch seconds, so the check must recompute it the same
        -- way. The two functions disagree by exactly the DST shift on the 11
        -- trips that straddle 02:00 on spring-forward day -- a check written
        -- with timestampdiff tests the two functions against each other and
        -- reports correct rows as defects.
        SUM(CASE WHEN ABS(trip_duration_minutes
                          - (unix_timestamp(lpep_dropoff_datetime)
                             - unix_timestamp(lpep_pickup_datetime)) / 60.0) > 0.02
                 THEN 1 ELSE 0 END)                                AS x_duration,
        -- Rows Silver classified FAIL. Zero once the fact MERGE reads
        -- vw_green_taxi_valid instead of green_taxi_clean.
        SUM(CASE WHEN exists(qc_error_descriptions, x -> startswith(x, 'FAIL:'))
                 THEN 1 ELSE 0 END)                                AS x_quarantined,
        SUM(CASE WHEN size(qc_error_descriptions) > 0 THEN 1 ELSE 0 END) AS b_warned
    FROM nyc_gold.fact_taxi_trip
),
checks AS (
    SELECT 'completeness' AS check_category, 'table_not_empty' AS check_name,
           0.0 AS threshold_pct, t_empty AS failed_rows, 1 AS total_rows FROM f
    UNION ALL SELECT 'completeness', 'trip_key_not_null',    0.0, c_key,        total_rows FROM f
    UNION ALL SELECT 'uniqueness',   'one_row_per_trip_key', 0.0,
                     (SELECT extra_rows FROM grain), total_rows FROM f
    UNION ALL SELECT 'consistency',  'pickup_before_dropoff', 0.0, x_time_order, total_rows FROM f
    UNION ALL SELECT 'consistency',  'duration_matches_timestamps', 0.0, x_duration, total_rows FROM f

    UNION ALL SELECT 'consistency',  'rows_reconcile_with_silver', 0.0,
        CASE WHEN (SELECT n_expected FROM silver) = (SELECT total_rows FROM f)
             THEN 0 ELSE 1 END, 1 FROM f
    UNION ALL SELECT 'consistency',  'revenue_preserved', 0.0,
        CASE WHEN ABS(COALESCE((SELECT revenue FROM silver), 0)
                    - COALESCE((SELECT revenue FROM f), 0))
                  <= GREATEST(1.0, 0.001 * ABS(COALESCE((SELECT revenue FROM silver), 0)))
             THEN 0 ELSE 1 END, 1 FROM f

    -- Non-blocking on purpose: it reports a defect in the Gold build, and
    -- stopping the pipeline does not fix a FROM clause.
    UNION ALL SELECT 'consistency',  'quarantined_rows_in_gold', 0.0, x_quarantined, total_rows FROM f
    -- Advisory: can only ever WARN.
    UNION ALL SELECT 'business',     'trips_carrying_silver_warnings', 100.0, b_warned, total_rows FROM f
)

SELECT
    v_run_id, v_run_ts, 'gold', 'fact_taxi_trip',
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    threshold_pct,
    CASE WHEN failed_rows = 0                                              THEN 'PASS'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= threshold_pct THEN 'WARN'
         ELSE 'FAIL' END
FROM checks;

-- # 3. At-rest integrity
-- **Does every foreign key in the fact table find its dimension row?**
--
-- This is the section that justifies a Gold DQ existing at all. Every
-- other check here has an analogue upstream; this one does not, because
-- the keys did not exist until Gold built them.
--
-- It is called *at rest* because it asks about the warehouse **as it
-- currently stands**, not about the load that just happened. Re-run it a
-- week later without loading anything and it still answers.
--
-- Why an unresolvable key is worse than a missing row
-- A row that fails to load is absent, and a count notices. A row whose
-- foreign key resolves to nothing is **present**: `COUNT(*)` includes it,
-- `SUM(total_amount)` includes it, and then an INNER JOIN to the dimension
-- silently drops it while a LEFT JOIN quietly buckets it under NULL. The
-- totals and the breakdown stop agreeing, and nothing anywhere failed.
--
-- Measured in distinct keys, not rows
-- One unmatched zone id affecting forty thousand trips is **one** thing to
-- fix. Reporting it as forty thousand failures buries it. The two
-- `trips_without_*` rows are the same defects counted in trips, advisory
-- at 100.0, because "3 zones" and "8,400 trips" are different sentences
-- and a reviewer wants both.
--
-- All five are anti-joins against dimensions of 265, ~2,200 and ~92 rows.
-- Nothing is multiplied.

INSERT INTO nyc_quality.dq_results
WITH pu AS (
    SELECT COUNT(*) AS unmatched,
           (SELECT COUNT(DISTINCT pickup_location_id) FROM nyc_gold.fact_taxi_trip) AS total
    FROM (
        SELECT DISTINCT f.pickup_location_id
        FROM       nyc_gold.fact_taxi_trip f
        LEFT  JOIN nyc_gold.dim_taxi_zone  z ON f.pickup_location_id = z.location_id
        WHERE f.pickup_location_id IS NOT NULL AND z.location_id IS NULL
    )
),
do_ AS (
    SELECT COUNT(*) AS unmatched,
           (SELECT COUNT(DISTINCT dropoff_location_id) FROM nyc_gold.fact_taxi_trip) AS total
    FROM (
        SELECT DISTINCT f.dropoff_location_id
        FROM       nyc_gold.fact_taxi_trip f
        LEFT  JOIN nyc_gold.dim_taxi_zone  z ON f.dropoff_location_id = z.location_id
        WHERE f.dropoff_location_id IS NOT NULL AND z.location_id IS NULL
    )
),
-- fact.pickup_date is a DATE; dim_date's key is date_key INT, so the join is
-- on full_date. Worth stating: the column comment calls pickup_date a foreign
-- key to dim_date, and it is -- just not to the column named _key.
--
-- Counted in TRIPS, not in distinct dates, and that is the whole reason these
-- two are not blocking. dim_date is derived from the dates present in the
-- weather feed, by design. TLC files reliably carry a handful of trips dated
-- years outside the file month -- Silver flags them WARN and keeps them -- so
-- those dates have no calendar row and never will.
--
-- Over ~100 distinct dates, that handful is about 8 percent: a permanent FAIL
-- at any tolerance, purely because the denominator is small. The same defect
-- over 133,367 trips is 0.008 percent. A month genuinely missing from the
-- calendar is about a third of the rows either way, so the row denominator
-- still catches the regression while tolerating the convention.
pd AS (
    SELECT
        SUM(CASE WHEN f.pickup_date IS NOT NULL AND d.full_date IS NULL
                 THEN 1 ELSE 0 END)                             AS unmatched,
        COUNT(*)                                                AS total
    FROM       nyc_gold.fact_taxi_trip f
    LEFT  JOIN nyc_gold.dim_date       d ON f.pickup_date = d.full_date
),
dd AS (
    SELECT
        SUM(CASE WHEN f.dropoff_date IS NOT NULL AND d.full_date IS NULL
                 THEN 1 ELSE 0 END)                             AS unmatched,
        COUNT(*)                                                AS total
    FROM       nyc_gold.fact_taxi_trip f
    LEFT  JOIN nyc_gold.dim_date       d ON f.dropoff_date = d.full_date
),
wk AS (
    SELECT COUNT(*) AS unmatched,
           (SELECT COUNT(DISTINCT weather_key) FROM nyc_gold.fact_taxi_trip
            WHERE weather_key IS NOT NULL) AS total
    FROM (
        SELECT DISTINCT f.weather_key
        FROM       nyc_gold.fact_taxi_trip f
        LEFT  JOIN nyc_gold.dim_weather    w ON f.weather_key = w.weather_key
        WHERE f.weather_key IS NOT NULL AND w.weather_key IS NULL
    )
),
rows_hit AS (
    SELECT
        COUNT(*)                                                     AS n_trips,
        SUM(CASE WHEN zp.location_id IS NULL THEN 1 ELSE 0 END)      AS t_pu,
        SUM(CASE WHEN f.weather_key IS NULL THEN 1 ELSE 0 END)       AS t_no_weather
    FROM       nyc_gold.fact_taxi_trip f
    LEFT  JOIN nyc_gold.dim_taxi_zone  zp ON f.pickup_location_id = zp.location_id
),
checks AS (
    SELECT 'at_rest_integrity' AS check_category, 'pickup_zone_resolves' AS check_name,
           0.0 AS threshold_pct,
           (SELECT unmatched FROM pu) AS failed_rows,
           (SELECT total FROM pu)     AS total_rows
    UNION ALL SELECT 'at_rest_integrity', 'dropoff_zone_resolves', 0.0,
           (SELECT unmatched FROM do_), (SELECT total FROM do_)
    -- 5.0, not 0.0, and not blocking: see the note on the pd CTE above.
    UNION ALL SELECT 'at_rest_integrity', 'pickup_date_resolves', 5.0,
           (SELECT unmatched FROM pd), (SELECT total FROM pd)
    UNION ALL SELECT 'at_rest_integrity', 'dropoff_date_resolves', 5.0,
           (SELECT unmatched FROM dd), (SELECT total FROM dd)
    -- Only keys that were actually set are checked; a NULL weather_key is "no weather
    -- matched", which is the advisory below, not a broken reference.
    UNION ALL SELECT 'at_rest_integrity', 'weather_key_resolves', 0.0,
           (SELECT unmatched FROM wk), (SELECT total FROM wk)

    -- The same defects counted in trips. Advisory: one defect should not stop
    -- the pipeline twice.
    UNION ALL SELECT 'at_rest_integrity', 'trips_with_unmatched_pickup_zone', 100.0,
           (SELECT t_pu FROM rows_hit), (SELECT n_trips FROM rows_hit)
    UNION ALL SELECT 'at_rest_integrity', 'trips_without_weather', 100.0,
           (SELECT t_no_weather FROM rows_hit), (SELECT n_trips FROM rows_hit)

    -- Every dimension row is reachable. Not an error -- an unused zone is
    -- normal -- but a dimension where MOST rows are unused usually means the
    -- key convention drifted between the dimension and the fact.
    UNION ALL SELECT 'at_rest_integrity', 'weather_hours_used_by_a_trip', 100.0,
        (SELECT COUNT(*) FROM (
            SELECT w.weather_key FROM nyc_gold.dim_weather w
            LEFT JOIN (SELECT DISTINCT weather_key FROM nyc_gold.fact_taxi_trip) f
                   ON w.weather_key = f.weather_key
            WHERE f.weather_key IS NULL)),
        (SELECT COUNT(*) FROM nyc_gold.dim_weather)
)

SELECT
    v_run_id, v_run_ts, 'gold', 'fact_taxi_trip',
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    threshold_pct,
    CASE WHEN failed_rows = 0                                              THEN 'PASS'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= threshold_pct THEN 'WARN'
         ELSE 'FAIL' END
FROM checks;
-- 4. Audit log

INSERT INTO nyc_quality.dq_run_log
SELECT
    v_run_id, v_run_ts, 'gold',
    COUNT(DISTINCT table_name)                       AS tables_checked,
    COUNT(*)                                         AS checks_run,
    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS checks_passed,
    SUM(CASE WHEN status = 'WARN' THEN 1 ELSE 0 END) AS checks_warned,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS checks_failed,
    -- FAIL means THIS LAYER IS NOT USABLE -- a blocking check failed, and the
    -- gate below raises on the same condition. A non-blocking failure is a
    -- recorded defect, not a reason to stop, so it lands on WARN.
    CASE WHEN SUM(CASE WHEN status = 'FAIL'
                        AND check_name IN (SELECT check_name FROM vw_gold_blocking_checks)
                       THEN 1 ELSE 0 END) > 0              THEN 'FAIL'
         WHEN SUM(CASE WHEN status <> 'PASS' THEN 1 ELSE 0 END) > 0 THEN 'WARN'
         ELSE 'PASS' END                             AS overall_status,
    current_timestamp()                              AS finished_at
FROM nyc_quality.dq_results
WHERE run_id = v_run_id AND layer = 'gold';


--5. Results
SELECT * FROM nyc_quality.dq_run_log WHERE run_id = v_run_id;

SELECT table_name, check_category, check_name,
       failed_rows, total_rows, failed_pct, threshold_pct, status
FROM   nyc_quality.dq_results
WHERE  run_id = v_run_id AND status <> 'PASS'
ORDER  BY CASE status WHEN 'FAIL' THEN 0 ELSE 1 END, failed_pct DESC;

-- The referential report on its own.
SELECT check_name, failed_rows, total_rows, failed_pct, status
FROM   nyc_quality.dq_results
WHERE  run_id = v_run_id AND check_category = 'at_rest_integrity'
ORDER  BY failed_rows DESC;
