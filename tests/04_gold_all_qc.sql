-- Data Quality — Gold, group pipeline
--
-- Runs AFTER the Gold MERGEs. Its question is "did the star schema get built
-- from Silver correctly" — one more link in the same chain as preload →
-- Bronze → Silver.
--
-- ## Why this one is not per batch
--
-- Bronze and Silver check a batch because a batch is what they loaded. Gold
-- is different in two ways:
--   1. `dim_date` is rebuilt across the FULL span every run — its bounds come
--      from MIN/MAX over all of Silver. There is no March dim_date.
--   2. `dim_weather` computes `temp_max_c` / `temp_min_c` as a window over
--      the whole day, and `fact_taxi_trip` joins to dimensions that span
--      every month. A batch-scoped check cannot see whether the join works.
-- So every check here reads the tables whole. 
--
-- ## Sections
-- | § | Covers | Checks |
-- |---|---|---|
-- | 1 | `dim_date` | 8 |
-- | 2 | `dim_weather` | 10 |
-- | 3 | `dim_taxi_zone` | 7 |
-- | 4 | `fact_taxi_trip` — grain, derivation, denormalised labels | 14 |
-- | 5 | reconciliation with Silver, and batch coverage | 6 |
--
-- Then 6. Audit log · 7. Results · 8. Gate.
SET TIME ZONE 'UTC';
USE CATALOG nyc_mobility;

DECLARE OR REPLACE VARIABLE v_run_id  STRING;
DECLARE OR REPLACE VARIABLE v_run_ts  TIMESTAMP;
DECLARE OR REPLACE VARIABLE v_as_of   STRING;

SET VAR v_run_id = uuid();
SET VAR v_run_ts = current_timestamp();

-- The newest month with trips in the fact table. COALESCE covers the first
-- run against an empty fact: 'none' is a legible label, and a NULL here would
-- silently make every clear-down below match nothing.
SET VAR v_as_of = COALESCE(
    (SELECT date_format(MAX(pickup_date), 'yyyy-MM')
     FROM   nyc_gold.fact_taxi_trip
     WHERE  pickup_date IS NOT NULL),
    'none');

SELECT v_run_id AS run_id, v_run_ts AS run_ts, v_as_of AS as_of;


-- ## Threshold policy
-- | Constant | Value | Meaning |
-- |---|---|---|
-- | `v_strict_pct`   | 0%   | one occurrence corrupts a join or the grain |
-- | `v_tol_pct`      | 10%  | the source is imperfect and this much is tolerated |
-- | `v_advisory_pct` | 100% | reported every run, can only ever WARN |
-- | `v_min_rows`     | 5    | under this many failing rows it is a WARN whatever the rate |
--
-- `warn_pct` is 0.0 throughout: any failure is at least a WARN. Nothing in
-- Gold is expected to be imperfect at a low rate — Gold either derived a
-- value correctly or it did not.

DECLARE OR REPLACE VARIABLE v_strict_pct   DOUBLE;
DECLARE OR REPLACE VARIABLE v_tol_pct      DOUBLE;
DECLARE OR REPLACE VARIABLE v_advisory_pct DOUBLE;
DECLARE OR REPLACE VARIABLE v_min_rows     BIGINT;

SET VAR v_strict_pct   = 0.0;
SET VAR v_tol_pct      = 10.0;
SET VAR v_advisory_pct = 100.0;
SET VAR v_min_rows     = 5;


-- ## The blocking list
-- `(table, check)` pairs, not bare names. The old version used
-- `explode(array('table_not_empty', ...))`, which blocks that name on all
-- four tables at once — an empty `dim_weather` would have stopped the trip
-- path for a reason that has nothing to do with trips.
--
-- What earns a gate here: the key, the grain, and the derivation. A derived
-- key that disagrees with the column it came from is the worst defect in a
-- star schema, because two queries joining by different routes return
-- different answers and neither looks wrong.

CREATE OR REPLACE TEMPORARY VIEW vw_gold_blocking_checks AS
SELECT * FROM VALUES
    -- dim_date -- the calendar is a primary key or it is nothing
    ('dim_date',       'table_not_empty'),
    ('dim_date',       'date_key_not_null'),
    ('dim_date',       'date_key_unique'),
    ('dim_date',       'key_matches_full_date'),
    ('dim_date',       'no_missing_days'),

    -- dim_weather
    ('dim_weather',    'table_not_empty'),
    ('dim_weather',    'weather_key_not_null'),
    ('dim_weather',    'weather_key_unique'),
    ('dim_weather',    'key_matches_timestamp'),

    -- dim_taxi_zone
    ('dim_taxi_zone',  'table_not_empty'),
    ('dim_taxi_zone',  'location_id_not_null'),
    ('dim_taxi_zone',  'location_id_unique'),

    -- fact_taxi_trip -- grain, derivation, and the reconciliations
    ('fact_taxi_trip', 'table_not_empty'),
    ('fact_taxi_trip', 'trip_key_not_null'),
    ('fact_taxi_trip', 'one_row_per_trip_key'),
    ('fact_taxi_trip', 'duration_matches_timestamps'),
    ('fact_taxi_trip', 'trip_key_matches_its_inputs'),
    ('fact_taxi_trip', 'pickup_date_matches_timestamp'),
    ('fact_taxi_trip', 'dropoff_date_matches_timestamp'),
    -- weather_key_matches_pickup_hour is strict but NOT blocking: it reports a
    -- wrong join rather than an unusable table, and the fix is a rebuild that
    -- stopping the run does not perform.
    ('fact_taxi_trip', 'rows_reconcile_with_silver'),
    ('fact_taxi_trip', 'revenue_preserved')
AS blocking(table_name, check_name);


-- ## Which tables the run cannot proceed without
-- `fact_taxi_trip` and `dim_taxi_zone`: no trips and no zones, no dashboard.
-- A broken `dim_weather` costs the weather breakdown and nothing else, and
-- `dim_date` is rebuilt from scratch every run so a bad one is self-healing.

DECLARE OR REPLACE VARIABLE v_required_tables ARRAY<STRING>;
SET VAR v_required_tables = array('fact_taxi_trip', 'dim_taxi_zone');


-- ## Clear-down
-- On (layer, batch_month), not on run_id. `DELETE ... WHERE run_id = v_run_id`
-- was the old form: a fresh uuid every run matches nothing, so it deleted
-- zero rows while appearing to clear the slate, and dq_results accumulated a
-- full set per execution.
--
-- Re-running the same as-of month replaces that month's picture. Loading a
-- new month and re-running adds one.

DELETE FROM nyc_quality.dq_results
WHERE layer = 'gold' AND batch_month = v_as_of;

DELETE FROM nyc_quality.dq_run_log
WHERE layer = 'gold' AND batch_month = v_as_of;


-- # 1. dim_date
-- Grain first — a duplicate key here multiplies rows in every query that
-- joins to it and inflates every total without failing anything.
--
-- Then the checks that exist only because Gold **derives** its keys.
-- `date_key` is `yyyyMMdd` of `full_date`. If the key and the column it came
-- from ever disagree, a query joining on `date_key` and a query joining on
-- `full_date` return different answers and neither looks wrong.
--
-- `is_weekend_matches_day_of_week` catches the classic: `dayofweek()` in
-- Spark is **1 = Sunday … 7 = Saturday**, not ISO. A weekend test written as
-- `IN (6, 7)` gives Friday and Saturday. The build uses `IN (1, 7)`; this
-- asserts the table agrees.
--
-- `no_missing_days` is new. The build verifies contiguity in a SELECT at the
-- bottom of the dim_date cell, where nobody reads it and nothing acts on it.
-- A calendar with a hole silently drops every trip on the missing day from
-- any inner join, so it belongs here, and it blocks.

INSERT INTO nyc_quality.dq_results (
    run_id, run_ts, layer, table_name, check_category, check_name,
    failed_rows, total_rows, failed_pct, threshold_pct, status,
    batch_month, min_failed_rows, warn_pct
)
WITH d AS (
    SELECT
        COUNT(*)                                                     AS total_rows,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END                     AS t_empty,
        SUM(CASE WHEN date_key IS NULL THEN 1 ELSE 0 END)            AS c_key,
        COUNT(date_key) - COUNT(DISTINCT date_key)                   AS u_key,
        SUM(CASE WHEN full_date IS NULL THEN 1 ELSE 0 END)           AS c_full_date,
        SUM(CASE WHEN CAST(date_format(full_date, 'yyyyMMdd') AS INT) <> date_key
                 THEN 1 ELSE 0 END)                                  AS x_key_derived,
        -- dayofweek() is 1 = Sunday .. 7 = Saturday in Spark, NOT ISO.
        SUM(CASE WHEN is_weekend <> (day_of_week IN (1, 7))
                 THEN 1 ELSE 0 END)                                  AS x_weekend,
        -- Every attribute is a pure function of full_date. Checking one of
        -- them is checking the lot, so this takes the cheapest: year.
        SUM(CASE WHEN year <> YEAR(full_date) OR month <> MONTH(full_date)
                 THEN 1 ELSE 0 END)                                  AS x_parts,
        -- Contiguity, stated as a count rather than a boolean so the number
        -- of missing days is visible in failed_rows.
        GREATEST(datediff(MAX(full_date), MIN(full_date)) + 1 - COUNT(*), 0)
                                                                     AS x_gaps
    FROM nyc_gold.dim_date
),
checks AS (
    SELECT 'completeness' AS check_category, 'table_not_empty' AS check_name,
           v_strict_pct AS threshold_pct, t_empty AS failed_rows, 1 AS total_rows FROM d
    UNION ALL SELECT 'completeness', 'date_key_not_null',   v_strict_pct, c_key,        total_rows FROM d
    UNION ALL SELECT 'completeness', 'full_date_not_null',  v_strict_pct, c_full_date,  total_rows FROM d
    UNION ALL SELECT 'uniqueness',   'date_key_unique',     v_strict_pct, u_key,        total_rows FROM d
    UNION ALL SELECT 'consistency',  'key_matches_full_date', v_strict_pct, x_key_derived, total_rows FROM d
    UNION ALL SELECT 'consistency',  'is_weekend_matches_day_of_week', v_strict_pct, x_weekend, total_rows FROM d
    UNION ALL SELECT 'consistency',  'date_parts_match_full_date', v_strict_pct, x_parts, total_rows FROM d
    UNION ALL SELECT 'completeness', 'no_missing_days',     v_strict_pct, x_gaps,       total_rows FROM d
)
SELECT
    v_run_id, v_run_ts, 'gold', 'dim_date',
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    threshold_pct,
    -- The five-branch ladder, identical to Bronze and Silver. The old Gold
    -- notebook had three: no SKIP, so an empty table produced a FAIL from
    -- every check at once; and no row floor, so one bad row failed the run.
    CASE WHEN total_rows = 0 AND check_name <> 'table_not_empty'           THEN 'SKIP'
         WHEN failed_rows = 0                                              THEN 'PASS'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= warn_pct      THEN 'PASS'
         WHEN failed_rows <= min_failed_rows                               THEN 'WARN'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= threshold_pct THEN 'WARN'
         ELSE 'FAIL' END,
    v_as_of,
    min_failed_rows,
    warn_pct
FROM (
    SELECT c.*,
           CASE WHEN c.total_rows <= 1                       THEN 0
                WHEN c.check_name IN ('date_key_unique',
                                      'date_key_not_null')   THEN 0
                ELSE v_min_rows END AS min_failed_rows,
           0.0 AS warn_pct
    FROM checks c
);


-- # 2. dim_weather
-- Same shape, plus three things the build does that nothing else checks.
--
-- `hour_temp_within_day_range`: `temp_max_c` and `temp_min_c` are the DAY's,
-- computed as a window partitioned by date; `temp_avg_c` is THIS hour's
-- reading. So the hour must sit inside its own day's range. A window written
-- over the wrong partition shows up here and nowhere else.
--
-- `one_row_per_hour`: the build deduplicates with QUALIFY ROW_NUMBER() over
-- `DATE_TRUNC('HOUR', observation_timestamp)`. `weather_key_unique` already
-- proves the key is unique, but the key is `yyyyMMddHH` of a timestamp that
-- was already truncated — so it proves the truncation, not the dedup. This
-- checks the timestamp itself.
--
-- `rain_hours_in_domain` is new. The DDL types it DOUBLE and the column
-- comment calls it a count of rainy hours, but the build emits 0, 1 or NULL
-- per row — it is a flag, not a count. Anyone who writes SUM(rain_hours)
-- expecting hours gets hours; anyone who writes AVG gets a proportion. The
-- check pins the actual domain so the mismatch is recorded rather than
-- discovered in a dashboard.

INSERT INTO nyc_quality.dq_results (
    run_id, run_ts, layer, table_name, check_category, check_name,
    failed_rows, total_rows, failed_pct, threshold_pct, status,
    batch_month, min_failed_rows, warn_pct
)
WITH hour_dupes AS (
    SELECT COALESCE(SUM(n - 1), 0) AS extra_rows
    FROM (
        SELECT COUNT(*) AS n
        FROM   nyc_gold.dim_weather
        WHERE  weather_timestamp IS NOT NULL
        GROUP  BY weather_timestamp
        HAVING COUNT(*) > 1
    )
),
w AS (
    SELECT
        COUNT(*)                                                     AS total_rows,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END                     AS t_empty,
        SUM(CASE WHEN weather_key IS NULL THEN 1 ELSE 0 END)         AS c_key,
        COUNT(weather_key) - COUNT(DISTINCT weather_key)             AS u_key,
        SUM(CASE WHEN weather_timestamp IS NULL THEN 1 ELSE 0 END)   AS c_ts,
        SUM(CASE WHEN date_format(weather_timestamp, 'yyyyMMddHH') <> weather_key
                 THEN 1 ELSE 0 END)                                  AS x_key_derived,
        SUM(CASE WHEN CAST(weather_timestamp AS DATE) <> weather_date
                 THEN 1 ELSE 0 END)                                  AS x_date_derived,
        SUM(CASE WHEN weather_condition IS NULL THEN 1 ELSE 0 END)   AS c_condition,
        SUM(CASE WHEN temp_avg_c IS NOT NULL
                  AND (temp_avg_c > temp_max_c OR temp_avg_c < temp_min_c)
                 THEN 1 ELSE 0 END)                                  AS x_temp_range,
        -- The build's own domain, whatever the DDL type says.
        SUM(CASE WHEN rain_hours IS NOT NULL AND rain_hours NOT IN (0, 1)
                 THEN 1 ELSE 0 END)                                  AS x_rain_flag,
        -- The hour is truncated in the build, so a non-zero minute or second
        -- means the truncation was dropped somewhere.
        SUM(CASE WHEN weather_timestamp IS NOT NULL
                  AND weather_timestamp <> date_trunc('HOUR', weather_timestamp)
                 THEN 1 ELSE 0 END)                                  AS x_not_on_hour
    FROM nyc_gold.dim_weather
),
checks AS (
    SELECT 'completeness' AS check_category, 'table_not_empty' AS check_name,
           v_strict_pct AS threshold_pct, t_empty AS failed_rows, 1 AS total_rows FROM w
    UNION ALL SELECT 'completeness', 'weather_key_not_null',  v_strict_pct, c_key,  total_rows FROM w
    UNION ALL SELECT 'completeness', 'weather_timestamp_not_null', v_strict_pct, c_ts, total_rows FROM w
    UNION ALL SELECT 'uniqueness',   'weather_key_unique',    v_strict_pct, u_key,  total_rows FROM w
    UNION ALL SELECT 'uniqueness',   'one_row_per_hour',      v_strict_pct,
                     (SELECT extra_rows FROM hour_dupes), total_rows FROM w
    UNION ALL SELECT 'consistency',  'key_matches_timestamp', v_strict_pct, x_key_derived,  total_rows FROM w
    UNION ALL SELECT 'consistency',  'weather_date_matches_timestamp', v_strict_pct, x_date_derived, total_rows FROM w
    UNION ALL SELECT 'consistency',  'timestamp_is_on_the_hour', v_strict_pct, x_not_on_hour, total_rows FROM w
    UNION ALL SELECT 'completeness', 'weather_condition_not_null', v_strict_pct, c_condition, total_rows FROM w
    UNION ALL SELECT 'consistency',  'hour_temp_within_day_range', v_strict_pct, x_temp_range, total_rows FROM w
    -- Advisory: the column is misnamed, not wrong. Recording it every run is
    -- the point; stopping the pipeline over a naming choice is not.
    UNION ALL SELECT 'validity',     'rain_hours_in_domain',  v_advisory_pct, x_rain_flag, total_rows FROM w
)
SELECT
    v_run_id, v_run_ts, 'gold', 'dim_weather',
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    threshold_pct,
    CASE WHEN total_rows = 0 AND check_name <> 'table_not_empty'           THEN 'SKIP'
         WHEN failed_rows = 0                                              THEN 'PASS'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= warn_pct      THEN 'PASS'
         WHEN failed_rows <= min_failed_rows                               THEN 'WARN'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= threshold_pct THEN 'WARN'
         ELSE 'FAIL' END,
    v_as_of,
    min_failed_rows,
    warn_pct
FROM (
    SELECT c.*,
           CASE WHEN c.total_rows <= 1                        THEN 0
                WHEN c.check_name IN ('weather_key_unique',
                                      'weather_key_not_null',
                                      'one_row_per_hour')     THEN 0
                ELSE v_min_rows END AS min_failed_rows,
           0.0 AS warn_pct
    FROM checks c
);


-- # 3. dim_taxi_zone
-- The smallest dimension and the one every trip joins to twice. It is copied
-- from Silver with two CASE expressions that fold 'N/A', 'Unknown' and ''
-- into 'Unknown', so `rows_reconcile_with_silver` is a real check here: the
-- MERGE has no WHERE clause and should move every row.
--
-- `unknown_zones_present` asserts the two ids TLC reserves (264, 265) survived
-- the copy. They are how an unresolvable pickup stays joinable instead of
-- becoming a NULL that quietly drops out of an inner join.

INSERT INTO nyc_quality.dq_results (
    run_id, run_ts, layer, table_name, check_category, check_name,
    failed_rows, total_rows, failed_pct, threshold_pct, status,
    batch_month, min_failed_rows, warn_pct
)
WITH silver_z AS (
    SELECT COUNT(*) AS n_silver FROM nyc_silver.taxi_zones_clean
),
z AS (
    SELECT
        COUNT(*)                                                     AS total_rows,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END                     AS t_empty,
        SUM(CASE WHEN location_id IS NULL THEN 1 ELSE 0 END)         AS c_key,
        COUNT(location_id) - COUNT(DISTINCT location_id)             AS u_key,
        SUM(CASE WHEN borough IS NULL OR TRIM(borough) = '' THEN 1 ELSE 0 END)
                                                                     AS c_borough,
        SUM(CASE WHEN zone_name IS NULL OR TRIM(zone_name) = '' THEN 1 ELSE 0 END)
                                                                     AS c_zone,
        SUM(CASE WHEN location_id < 1 OR location_id > 265 THEN 1 ELSE 0 END)
                                                                     AS x_range,
        CASE WHEN COUNT(*) = 265 THEN 0 ELSE 1 END                   AS b_265,
        CASE WHEN COUNT(DISTINCT CASE WHEN location_id IN (264, 265)
                                      THEN location_id END) = 2
             THEN 0 ELSE 1 END                                       AS b_unknown
    FROM nyc_gold.dim_taxi_zone
),
checks AS (
    SELECT 'completeness' AS check_category, 'table_not_empty' AS check_name,
           v_strict_pct AS threshold_pct, t_empty AS failed_rows, 1 AS total_rows FROM z
    UNION ALL SELECT 'completeness', 'location_id_not_null', v_strict_pct, c_key,     total_rows FROM z
    UNION ALL SELECT 'uniqueness',   'location_id_unique',   v_strict_pct, u_key,     total_rows FROM z
    UNION ALL SELECT 'validity',     'location_id_in_range', v_strict_pct, x_range,   total_rows FROM z
    UNION ALL SELECT 'completeness', 'borough_not_blank',    v_strict_pct, c_borough, total_rows FROM z
    UNION ALL SELECT 'completeness', 'zone_name_not_blank',  v_strict_pct, c_zone,    total_rows FROM z
    UNION ALL SELECT 'business',     'lookup_has_265_zones', v_strict_pct, b_265,     1 FROM z
    UNION ALL SELECT 'business',     'unknown_zones_present', v_strict_pct, b_unknown, 1 FROM z
    UNION ALL SELECT 'consistency',  'rows_reconcile_with_silver', v_strict_pct,
        CASE WHEN (SELECT n_silver FROM silver_z) = (SELECT total_rows FROM z)
             THEN 0 ELSE 1 END, 1 FROM z
)
SELECT
    v_run_id, v_run_ts, 'gold', 'dim_taxi_zone',
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    threshold_pct,
    CASE WHEN total_rows = 0 AND check_name <> 'table_not_empty'           THEN 'SKIP'
         WHEN failed_rows = 0                                              THEN 'PASS'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= warn_pct      THEN 'PASS'
         WHEN failed_rows <= min_failed_rows                               THEN 'WARN'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= threshold_pct THEN 'WARN'
         ELSE 'FAIL' END,
    v_as_of,
    min_failed_rows,
    warn_pct
FROM (
    SELECT c.*,
           CASE WHEN c.total_rows <= 1                          THEN 0
                WHEN c.check_name IN ('location_id_unique',
                                      'location_id_not_null')   THEN 0
                ELSE v_min_rows END AS min_failed_rows,
           0.0 AS warn_pct
    FROM checks c
);


-- # 4. fact_taxi_trip
-- `trip_key` is `MD5(CONCAT_WS('|', ...))` over seven columns. Two checks
-- guard it, and the second one is the important one.
-- **`concat_ws` skips NULL arguments — it does not render them.** So a trip
-- with a NULL `do_location_id` produces `a|b|c|e|f|g`, the same string a
-- different trip with six values produces. Two unrelated trips can hash to
-- one key, and the MERGE then treats them as the same row: one overwrites
-- the other and the fact silently loses a trip.
--
-- `trip_key_inputs_not_null` counts the rows exposed to that. It is the
-- population at risk, not the collisions themselves — a collision needs two
-- rows to line up — but it is the number that matters, because the fix is to
-- wrap each argument in `COALESCE(..., '<NULL>')` in the Gold MERGE and the
-- risk goes to zero regardless of the data.
--
-- `trip_key_matches_its_inputs` recomputes the hash from the fact's OWN
-- columns using the build's exact expression, NULL-skipping included. It
-- asserts the stored key belongs to the row it sits on. Written with COALESCE
-- instead it would test a key the build never produced and fail on every row
-- that has a NULL — a check that reports the pipeline as broken because the
-- check disagrees with it.
--
-- ## 4b. The derived columns
-- `trip_duration_minutes` is recomputed with `unix_timestamp()`, not
-- `timestampdiff()`, because that is what the build uses. The two disagree by
-- exactly the DST shift on trips straddling 02:00 on spring-forward day, so a
-- check written the other way tests the two functions against each other and
-- reports correct rows as defects. Tolerance 0.02 covers the build's ROUND.
--
-- `pickup_date` / `dropoff_date` are `DATE()` of their timestamps and are the
-- foreign keys to dim_date, so a disagreement misroutes the join.
--
-- `weather_key_matches_pickup_hour` is the one that encodes the timezone
-- convention. dim_weather holds UTC hours; `lpep_pickup_datetime` is
-- wall-clock New York. The build joins on
-- `to_utc_timestamp(pickup, 'America/New_York')` truncated to the hour, and
-- this asserts the stored key agrees. It is the only automated statement
-- anywhere in the pipeline that the UTC-vs-New-York conversion is applied in
-- the right direction — get it backwards and every trip still gets a weather
-- row, just the wrong one, eight hours out.
--
-- ## 4c. The denormalised labels
--
-- Four `CASE` expressions turn an id into a label. Nothing downstream checks
-- them, and a dashboard grouped by `vendor_name` after someone edits the CASE
-- reads as a real change in the business. Cheap to assert, so assert them.

INSERT INTO nyc_quality.dq_results (
    run_id, run_ts, layer, table_name, check_category, check_name,
    failed_rows, total_rows, failed_pct, threshold_pct, status,
    batch_month, min_failed_rows, warn_pct
)
WITH grain AS (
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
        COUNT(*)                                                     AS total_rows,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END                     AS t_empty,
        SUM(CASE WHEN trip_key IS NULL THEN 1 ELSE 0 END)            AS c_key,
        -- The build's expression, NULL-skipping and all. See 4a.
        SUM(CASE WHEN trip_key <> MD5(CONCAT_WS('|',
                        CAST(vendor_id AS STRING),
                        CAST(lpep_pickup_datetime AS STRING),
                        CAST(lpep_dropoff_datetime AS STRING),
                        CAST(pickup_location_id AS STRING),
                        CAST(dropoff_location_id AS STRING),
                        CAST(trip_distance AS STRING),
                        CAST(total_amount AS STRING)))
                 THEN 1 ELSE 0 END)                                  AS x_key_derived,
        -- Rows where at least one hash input is NULL, so concat_ws drops a
        -- field and the key becomes ambiguous.
        SUM(CASE WHEN vendor_id            IS NULL
                   OR lpep_pickup_datetime IS NULL
                   OR lpep_dropoff_datetime IS NULL
                   OR pickup_location_id   IS NULL
                   OR dropoff_location_id  IS NULL
                   OR trip_distance        IS NULL
                   OR total_amount         IS NULL
                 THEN 1 ELSE 0 END)                                  AS x_key_nulls,
        SUM(CASE WHEN lpep_dropoff_datetime < lpep_pickup_datetime
                 THEN 1 ELSE 0 END)                                  AS x_time_order,
        SUM(CASE WHEN ABS(trip_duration_minutes
                          - (unix_timestamp(lpep_dropoff_datetime)
                             - unix_timestamp(lpep_pickup_datetime)) / 60.0) > 0.02
                 THEN 1 ELSE 0 END)                                  AS x_duration,
        SUM(CASE WHEN pickup_date <> DATE(lpep_pickup_datetime)
                 THEN 1 ELSE 0 END)                                  AS x_pickup_date,
        SUM(CASE WHEN dropoff_date <> DATE(lpep_dropoff_datetime)
                 THEN 1 ELSE 0 END)                                  AS x_dropoff_date,
        -- The timezone convention, asserted. Only rows that got a key.
        SUM(CASE WHEN weather_key IS NOT NULL
                  AND weather_key <> date_format(
                        date_trunc('HOUR',
                            to_utc_timestamp(lpep_pickup_datetime, 'America/New_York')),
                        'yyyyMMddHH')
                 THEN 1 ELSE 0 END)                                  AS x_weather_key,
        -- Denormalised labels against their ids.
        SUM(CASE WHEN vendor_name <> CASE vendor_id
                        WHEN 1 THEN 'Creative Mobile Technologies, LLC'
                        WHEN 2 THEN 'VeriFone Inc.'
                        WHEN 6 THEN 'Other'
                        ELSE 'Unknown' END
                 THEN 1 ELSE 0 END)                                  AS x_vendor,
        SUM(CASE WHEN payment_type <> CASE payment_type_id
                        WHEN 0 THEN 'No charge'  WHEN 1 THEN 'Credit card'
                        WHEN 2 THEN 'Cash'       WHEN 3 THEN 'No charge'
                        WHEN 4 THEN 'Dispute'    WHEN 5 THEN 'Unknown'
                        WHEN 6 THEN 'Voided trip' ELSE 'Unknown' END
                 THEN 1 ELSE 0 END)                                  AS x_payment,
        SUM(CASE WHEN ratecode_description <> CASE ratecode_id
                        WHEN 1 THEN 'Standard rate' WHEN 2 THEN 'JFK'
                        WHEN 3 THEN 'Newark'        WHEN 4 THEN 'Nassau or Westchester'
                        WHEN 5 THEN 'Negotiated fare' WHEN 6 THEN 'Group ride'
                        WHEN 99 THEN 'Unknown'      ELSE 'Unknown' END
                 THEN 1 ELSE 0 END)                                  AS x_ratecode,
        SUM(CASE WHEN trip_type_description <> CASE trip_type_id
                        WHEN 1 THEN 'Street-hail' WHEN 2 THEN 'Dispatch'
                        ELSE 'Unknown' END
                 THEN 1 ELSE 0 END)                                  AS x_triptype,
        SUM(CASE WHEN created_at IS NULL THEN 1 ELSE 0 END)          AS c_created
    FROM nyc_gold.fact_taxi_trip
),
checks AS (
    SELECT 'completeness' AS check_category, 'table_not_empty' AS check_name,
           v_strict_pct AS threshold_pct, t_empty AS failed_rows, 1 AS total_rows FROM f
    UNION ALL SELECT 'completeness', 'trip_key_not_null',     v_strict_pct, c_key,        total_rows FROM f
    UNION ALL SELECT 'uniqueness',   'one_row_per_trip_key',  v_strict_pct,
                     (SELECT extra_rows FROM grain), total_rows FROM f
    UNION ALL SELECT 'consistency',  'trip_key_matches_its_inputs', v_strict_pct, x_key_derived, total_rows FROM f
    -- Tolerated, not blocking: it reports a weakness in the key expression,
    -- and stopping the pipeline does not rewrite a CONCAT_WS. Recorded every
    -- run so the number is visible until the MERGE is fixed.
    UNION ALL SELECT 'validity',     'trip_key_inputs_not_null', v_tol_pct, x_key_nulls, total_rows FROM f
    UNION ALL SELECT 'consistency',  'pickup_before_dropoff', v_strict_pct, x_time_order, total_rows FROM f
    UNION ALL SELECT 'consistency',  'duration_matches_timestamps', v_strict_pct, x_duration, total_rows FROM f
    UNION ALL SELECT 'consistency',  'pickup_date_matches_timestamp',  v_strict_pct, x_pickup_date,  total_rows FROM f
    UNION ALL SELECT 'consistency',  'dropoff_date_matches_timestamp', v_strict_pct, x_dropoff_date, total_rows FROM f
    UNION ALL SELECT 'consistency',  'weather_key_matches_pickup_hour', v_strict_pct, x_weather_key, total_rows FROM f
    UNION ALL SELECT 'consistency',  'vendor_name_matches_id',       v_strict_pct, x_vendor,   total_rows FROM f
    UNION ALL SELECT 'consistency',  'payment_type_matches_id',      v_strict_pct, x_payment,  total_rows FROM f
    UNION ALL SELECT 'consistency',  'ratecode_description_matches_id', v_strict_pct, x_ratecode, total_rows FROM f
    UNION ALL SELECT 'consistency',  'trip_type_description_matches_id', v_strict_pct, x_triptype, total_rows FROM f
    UNION ALL SELECT 'completeness', 'created_at_recorded',   v_strict_pct, c_created, total_rows FROM f
)
SELECT
    v_run_id, v_run_ts, 'gold', 'fact_taxi_trip',
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    threshold_pct,
    CASE WHEN total_rows = 0 AND check_name <> 'table_not_empty'           THEN 'SKIP'
         WHEN failed_rows = 0                                              THEN 'PASS'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= warn_pct      THEN 'PASS'
         WHEN failed_rows <= min_failed_rows                               THEN 'WARN'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= threshold_pct THEN 'WARN'
         ELSE 'FAIL' END,
    v_as_of,
    min_failed_rows,
    warn_pct
FROM (
    SELECT c.*,
           CASE WHEN c.total_rows <= 1                           THEN 0
                WHEN c.check_name IN ('one_row_per_trip_key',
                                      'trip_key_not_null')       THEN 0
                ELSE v_min_rows END AS min_failed_rows,
           0.0 AS warn_pct
    FROM checks c
);


-- # 5. Reconciliation with Silver, and coverage
-- ## What the fact table is built from
--
-- The Gold MERGE reads `nyc_silver.green_taxi_clean` — the whole table, FAIL
-- rows included — and carries `qc_error_descriptions` across. That is a
-- deliberate design: keep everything in the fact and let the consumer filter.
-- The reconciliation is written against the same source, because a check that
-- compares against `vw_green_taxi_valid` fails by exactly the number of rows
-- the pipeline is correctly keeping, and gets worse the better the pipeline
-- works.
--
-- The consequence is that `quarantined_rows_in_gold` is **advisory**, not a
-- defect. Those rows belong there. What matters is that nothing consumes the
-- fact table without filtering — see the note in section 7.
--
-- ## The dedup expectation
-- Silver's merge key is finer than the fact's `trip_key`, which uses seven
-- columns. So two distinct Silver rows can collapse into one fact row, and
-- the expected count is `COUNT(DISTINCT trip_key)` over Silver — recomputed
-- with the build's expression, not a corrected one. `dedup_collapse_rate`
-- reports how many rows that absorbs, because a number that climbs is the
-- early warning for the concat_ws problem in 4a.
--
-- ## Coverage — the all-batches check
-- `every_silver_month_present` is what makes "Gold covers all batches" an
-- assertion rather than an assumption. It counts the months present in Silver
-- and absent from the fact. One missing month is a Gold task that did not run
-- after a load, which nothing else in the pipeline would notice: every
-- per-batch check upstream passed, and Gold's own totals look smaller but
-- perfectly self-consistent.

INSERT INTO nyc_quality.dq_results (
    run_id, run_ts, layer, table_name, check_category, check_name,
    failed_rows, total_rows, failed_pct, threshold_pct, status,
    batch_month, min_failed_rows, warn_pct
)
WITH silver_keyed AS (
    SELECT
        MD5(CONCAT_WS('|',
            CAST(vendor_id AS STRING),
            CAST(lpep_pickup_datetime AS STRING),
            CAST(lpep_dropoff_datetime AS STRING),
            CAST(pu_location_id AS STRING),
            CAST(do_location_id AS STRING),
            CAST(trip_distance AS STRING),
            CAST(total_amount AS STRING)))       AS trip_key,
        total_amount,
        date_format(lpep_pickup_datetime, 'yyyy-MM') AS trip_month
    FROM nyc_silver.green_taxi_clean
),
-- One row per key, so the revenue comparison is against the deduplicated
-- total the fact table actually holds rather than the raw Silver sum.
silver_dedup AS (
    SELECT trip_key, total_amount, trip_month
    FROM   silver_keyed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY trip_key ORDER BY total_amount DESC NULLS LAST) = 1
),
silver AS (
    SELECT COUNT(*)                    AS n_expected,
           ROUND(SUM(total_amount), 2) AS revenue,
           COUNT(DISTINCT trip_month)  AS n_months
    FROM   silver_dedup
),
silver_raw AS (
    SELECT COUNT(*) AS n_raw FROM silver_keyed
),
fact AS (
    SELECT COUNT(*)                    AS n_fact,
           ROUND(SUM(total_amount), 2) AS revenue,
           SUM(CASE WHEN qc_error_descriptions IS NOT NULL
                     AND exists(qc_error_descriptions, x -> startswith(x, 'FAIL:'))
                    THEN 1 ELSE 0 END) AS n_quarantined,
           SUM(CASE WHEN size(qc_error_descriptions) > 0
                    THEN 1 ELSE 0 END) AS n_warned
    FROM nyc_gold.fact_taxi_trip
),
-- Months in Silver with no trips in the fact. An anti-join, not a count
-- comparison: two months can have equal counts and still be the wrong two.
missing_months AS (
    SELECT COUNT(*) AS n_missing
    FROM (
        SELECT DISTINCT s.trip_month
        FROM       silver_dedup s
        LEFT  JOIN (SELECT DISTINCT date_format(pickup_date, 'yyyy-MM') AS trip_month
                    FROM   nyc_gold.fact_taxi_trip
                    WHERE  pickup_date IS NOT NULL) g
               ON  g.trip_month = s.trip_month
        WHERE  s.trip_month IS NOT NULL AND g.trip_month IS NULL
    )
),
checks AS (
    SELECT 'consistency' AS check_category, 'rows_reconcile_with_silver' AS check_name,
           v_strict_pct AS threshold_pct,
           CASE WHEN (SELECT n_expected FROM silver) = (SELECT n_fact FROM fact)
                THEN 0 ELSE 1 END AS failed_rows,
           1 AS total_rows
    UNION ALL SELECT 'consistency', 'revenue_preserved', v_strict_pct,
           CASE WHEN ABS(COALESCE((SELECT revenue FROM silver), 0)
                       - COALESCE((SELECT revenue FROM fact), 0))
                     <= GREATEST(1.0, 0.001 * ABS(COALESCE((SELECT revenue FROM silver), 0)))
                THEN 0 ELSE 1 END, 1
    UNION ALL SELECT 'completeness', 'every_silver_month_present', v_strict_pct,
           (SELECT n_missing FROM missing_months), (SELECT n_months FROM silver)
    -- Advisory: Silver rows absorbed by the coarser fact key. A number that
    -- climbs run over run is the concat_ws weakness showing itself.
    UNION ALL SELECT 'business', 'dedup_collapse_rate', v_advisory_pct,
           (SELECT n_raw FROM silver_raw) - (SELECT n_expected FROM silver),
           (SELECT n_raw FROM silver_raw)
    -- Advisory by design: the fact deliberately keeps quarantined rows.
    UNION ALL SELECT 'business', 'quarantined_rows_in_gold', v_advisory_pct,
           (SELECT n_quarantined FROM fact), (SELECT n_fact FROM fact)
    UNION ALL SELECT 'business', 'trips_carrying_silver_warnings', v_advisory_pct,
           (SELECT n_warned FROM fact), (SELECT n_fact FROM fact)
)
SELECT
    v_run_id, v_run_ts, 'gold', 'fact_taxi_trip',
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    threshold_pct,
    CASE WHEN total_rows = 0 AND check_name <> 'table_not_empty'           THEN 'SKIP'
         WHEN failed_rows = 0                                              THEN 'PASS'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= warn_pct      THEN 'PASS'
         WHEN failed_rows <= min_failed_rows                               THEN 'WARN'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= threshold_pct THEN 'WARN'
         ELSE 'FAIL' END,
    v_as_of,
    min_failed_rows,
    warn_pct
FROM (
    -- The ladder above reads warn_pct and min_failed_rows off its input, the
    -- same way sections 1-4 do, so this section has to supply them too. It
    -- carried literals in the final SELECT list instead, which writes the right
    -- values into the table and still leaves the CASE with nothing to resolve.
    --
    -- No floor on any of these: four are scalar, and a missing month is one row
    -- in the count and a third of the warehouse in practice.
    SELECT c.*, 0 AS min_failed_rows, 0.0 AS warn_pct
    FROM   checks c
);
-- # 6. Audit log
-- One row per run. `batch_month` carries `v_as_of`, so the log reads as a
-- history of the warehouse's state after each load rather than a single row
-- that keeps being overwritten
INSERT INTO nyc_quality.dq_run_log (
    run_id, run_ts, layer, tables_checked, checks_run,
    checks_passed, checks_warned, checks_failed, overall_status, finished_at,
    batch_month, checks_skipped
)
SELECT
    v_run_id,
    v_run_ts,
    'gold',
    COUNT(DISTINCT d.table_name),
    COUNT(*),
    SUM(CASE WHEN d.status = 'PASS' THEN 1 ELSE 0 END),
    SUM(CASE WHEN d.status = 'WARN' THEN 1 ELSE 0 END),
    SUM(CASE WHEN d.status = 'FAIL' THEN 1 ELSE 0 END),
    -- FAIL means THIS LAYER IS NOT USABLE -- a blocking check failed, and the
    -- gate below raises on the same condition. A non-blocking failure is a
    -- recorded defect, not a reason to stop, so it lands on WARN. SKIP is
    -- excluded: a skipped check did not find anything wrong, it did not run.
    --
    -- `b.check_name IS NOT NULL` is the LEFT JOIN's way of saying "this check
    -- is on the blocking list". A correlated EXISTS inside the aggregate is
    -- what Spark will not resolve, which is how the Bronze version first
    -- failed with UNRESOLVED_COLUMN on a column that plainly existed.
    CASE WHEN SUM(CASE WHEN d.status = 'FAIL' AND b.check_name IS NOT NULL
                       THEN 1 ELSE 0 END) > 0                          THEN 'FAIL'
         WHEN SUM(CASE WHEN d.status IN ('FAIL','WARN') THEN 1 ELSE 0 END) > 0 THEN 'WARN'
         ELSE 'PASS' END,
    current_timestamp(),
    v_as_of,
    SUM(CASE WHEN d.status = 'SKIP' THEN 1 ELSE 0 END)
FROM       nyc_quality.dq_results d
LEFT  JOIN vw_gold_blocking_checks b
       ON  b.table_name = d.table_name AND b.check_name = d.check_name
WHERE  d.layer = 'gold'
  AND  d.batch_month = v_as_of
  AND  d.check_category <> 'gate';


-- # 7. Results
-- ## A note on consuming fact_taxi_trip
--
-- The fact table holds every Silver row, including the ones Silver classified
-- FAIL. `quarantined_rows_in_gold` reports how many. Nothing here stops a
-- dashboard from counting them, because that is a query-time decision, not a
-- load-time one. If Gold should only ever expose valid trips, the fix is a
-- view beside the fact table rather than a WHERE clause in the MERGE:
--
--     CREATE OR REPLACE VIEW nyc_gold.vw_fact_taxi_trip_valid AS
--     SELECT * FROM nyc_gold.fact_taxi_trip
--     WHERE  qc_error_descriptions IS NULL
--        OR  NOT exists(qc_error_descriptions, x -> startswith(x, 'FAIL:'));
--
-- Same pattern as Silver's `vw_green_taxi_valid`: the rows stay auditable and
-- the default path is the safe one.

SELECT * FROM nyc_quality.dq_run_log
WHERE  layer = 'gold' AND batch_month = v_as_of;

SELECT table_name, check_category, check_name,
       failed_rows, total_rows, failed_pct, threshold_pct,
       min_failed_rows, status
FROM   nyc_quality.dq_results
WHERE  layer = 'gold' AND batch_month = v_as_of AND status <> 'PASS'
ORDER  BY CASE status WHEN 'FAIL' THEN 0 WHEN 'WARN' THEN 1 ELSE 2 END,
          failed_pct DESC;


-- # 8. Gate — per table
-- A table is STOPPED when either is true of it:
--
-- | Trigger | Meaning |
-- |---|---|
-- | a blocking check for that table FAILED | the build is wrong, not the data |
-- | `v_max_total_failures` or more FAILs on that table | no single thing is fatal, but it is broadly wrong |
--
-- Counted per table, not across the run.
--
-- ### How a consumer reads the verdict
-- Written back into dq_results as one synthetic check per table,
-- `warehouse_cleared`, under check_category `gate`. No new table.
--
--     SELECT status
--     FROM   nyc_mobility.nyc_quality.dq_results
--     WHERE  layer = 'gold' AND check_category = 'gate'
--       AND  table_name = 'fact_taxi_trip'
--     ORDER  BY run_ts DESC LIMIT 1;

-- Enforcement switch.
--
-- FALSE: verdicts are still computed, written and displayed, but nothing
-- raises. TRUE: a stopped required table raises as designed.
--
-- One line so that suspending enforcement during a migration is one edit and a
-- grep for v_gate_enforce finds it. Leave it TRUE: a gate parked on FALSE
-- indefinitely is not a gate, it is a report.
DECLARE OR REPLACE VARIABLE v_gate_enforce BOOLEAN;
SET VAR v_gate_enforce = TRUE;

DECLARE OR REPLACE VARIABLE v_max_total_failures INT;
SET VAR v_max_total_failures = 5;


-- ### The blocking list has to be checked against reality
--
-- A pair in that list that no check ever emits is inert: it matches nothing,
-- so the gate neither blocks nor complains. That is what makes it dangerous
-- -- the list reads like a guarantee, and the next person to rename a check
-- turns one of those guarantees off without touching the gate.
--
-- Expect zero rows.
SELECT b.table_name, b.check_name AS blocking_pair_never_produced
FROM   vw_gold_blocking_checks b
LEFT   JOIN (SELECT DISTINCT table_name, check_name
             FROM   nyc_quality.dq_results
             WHERE  layer = 'gold' AND batch_month = v_as_of) r
       ON  b.table_name = r.table_name AND b.check_name = r.check_name
WHERE  r.check_name IS NULL;


-- ### Per-table verdicts
CREATE OR REPLACE TEMPORARY VIEW vw_gold_gate AS
WITH scored AS (
    SELECT d.table_name,
           SUM(CASE WHEN d.status = 'FAIL' THEN 1 ELSE 0 END)  AS failures,
           SUM(CASE WHEN d.status = 'FAIL' AND b.check_name IS NOT NULL
                    THEN 1 ELSE 0 END)                         AS blocking_failures,
           COALESCE(concat_ws(', ', collect_list(
               CASE WHEN d.status = 'FAIL' AND b.check_name IS NOT NULL
                    THEN d.check_name END)), 'none')           AS blocking_names,
           SUM(CASE WHEN d.status = 'SKIP' THEN 1 ELSE 0 END)  AS skipped
    FROM       nyc_quality.dq_results d
    LEFT  JOIN vw_gold_blocking_checks b
           ON  b.table_name = d.table_name AND b.check_name = d.check_name
    WHERE  d.layer = 'gold' AND d.batch_month = v_as_of
      AND  d.check_category <> 'gate'
    GROUP  BY d.table_name
)
SELECT table_name, failures, blocking_failures, blocking_names, skipped,
       CASE WHEN blocking_failures > 0
              OR failures >= v_max_total_failures THEN 'STOP' ELSE 'GO' END AS verdict,
       array_contains(v_required_tables, table_name)                        AS is_required
FROM   scored;

SELECT table_name, verdict, is_required,
       blocking_failures, failures, skipped, blocking_names
FROM   vw_gold_gate
ORDER  BY CASE verdict WHEN 'STOP' THEN 0 ELSE 1 END, table_name;


-- ### Record the verdicts where a consumer can read them
INSERT INTO nyc_quality.dq_results (
    run_id, run_ts, layer, table_name, check_category, check_name,
    failed_rows, total_rows, failed_pct, threshold_pct, status,
    batch_month, min_failed_rows, warn_pct
)
SELECT v_run_id, v_run_ts, 'gold', table_name,
       'gate', 'warehouse_cleared',
       blocking_failures, 1,
       CASE WHEN verdict = 'STOP' THEN 100.0 ELSE 0.0 END,
       0.0,
       CASE WHEN verdict = 'STOP' THEN 'FAIL' ELSE 'PASS' END,
       v_as_of, 0, 0.0
FROM   vw_gold_gate;


-- ### Raise, but only for a required table
DECLARE OR REPLACE VARIABLE v_stopped_required STRING;
DECLARE OR REPLACE VARIABLE v_stopped_optional STRING;

SET VAR v_stopped_required = (
    SELECT COALESCE(concat_ws(', ', collect_list(table_name)), '')
    FROM   vw_gold_gate WHERE verdict = 'STOP' AND is_required);

SET VAR v_stopped_optional = (
    SELECT COALESCE(concat_ws(', ', collect_list(table_name)), '')
    FROM   vw_gold_gate WHERE verdict = 'STOP' AND NOT is_required);

SELECT v_as_of                                          AS as_of,
       COALESCE(NULLIF(v_stopped_required, ''), 'none') AS stopped_required,
       COALESCE(NULLIF(v_stopped_optional, ''), 'none') AS stopped_optional,
       CASE WHEN v_stopped_required <> ''
              THEN 'will stop the run'
            WHEN v_stopped_optional <> ''
              THEN 'will continue; the named tables are not fit to publish'
            ELSE 'will continue' END                    AS verdict;

DECLARE OR REPLACE VARIABLE v_stopped_detail STRING;

-- The reason, not just the table. Without this the raise names a table and
-- the query that would explain it sits BELOW the raise, so it never runs --
-- the most useful output in the notebook is unreachable exactly when it is
-- needed. `blocking_names` is already computed in the gate view; this carries
-- it into the message.
SET VAR v_stopped_detail = (
    SELECT COALESCE(concat_ws(' | ', collect_list(
               concat(table_name, ' -> ',
                      CASE WHEN blocking_failures > 0 THEN blocking_names
                           ELSE concat(CAST(failures AS STRING),
                                       ' non-blocking failures, at or over the limit of ',
                                       CAST(v_max_total_failures AS STRING)) END))), '')
    FROM   vw_gold_gate WHERE verdict = 'STOP' AND is_required);

SELECT CASE
    WHEN v_gate_enforce AND v_stopped_required <> ''
      THEN raise_error(CONCAT('Gold DQ gate FAILED for ', v_as_of,
                              ': ', v_stopped_detail,
                              '. See nyc_quality.dq_results for run ', v_run_id))
    WHEN v_stopped_optional <> ''
      THEN CONCAT('Gold DQ gate PASSED as of ', v_as_of,
                  '; not fit to publish: ', v_stopped_optional)
    ELSE CONCAT('Gold DQ gate PASSED as of ', v_as_of,
                ' -- every table cleared')
END AS gate;
