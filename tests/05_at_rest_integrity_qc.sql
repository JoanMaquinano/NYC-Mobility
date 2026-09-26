
-- At-rest integrity — Gold warehouse
-- **Does every foreign key in the fact table find its dimension row?**
--
-- This is the section that justifies a Gold DQ existing at all. Every other
-- check in `gold_qc` has an analogue upstream; this one does not, because the
-- keys did not exist until Gold built them.
--
-- | | `gold_qc` | this notebook |
-- |---|---|---|
-- | Compares | Gold against Silver | Gold against itself |
-- | Reads Silver | yes | **no** |
-- | Trigger | after a Gold load | a schedule, or on demand |
-- | Fails when | the build is wrong | the warehouse has drifted |
--
-- The "reads Silver: no" row is the real test of the split. Nothing below
-- touches `nyc_silver`. If a check here needed Silver it would belong in
-- `gold_qc`, because it would be checking a load rather than a state.
--
-- Recorded under `layer = 'at_rest'`, not `'gold'`, so a dashboard can show
-- build quality and warehouse integrity as two different things, and so the
-- run log counts do not mix a load-time verdict with a standing one.
--
-- ## Why an unresolvable key is worse than a missing row
-- A row that fails to load is absent, and a count notices. A row whose
-- foreign key resolves to nothing is **present**: `COUNT(*)` includes it,
-- `SUM(total_amount)` includes it, and then an INNER JOIN to the dimension
-- silently drops it while a LEFT JOIN quietly buckets it under NULL. The
-- totals and the breakdown stop agreeing, and nothing anywhere failed.
--
-- ## Measured in distinct keys, not rows
-- One unmatched zone id affecting forty thousand trips is **one** thing to
-- fix. Reporting it as forty thousand failures buries it. The `trips_*` rows
-- are the same defects counted in trips, advisory, because "3 zones" and
-- "8,400 trips" are different sentences and a reviewer wants both.
--
-- ## Sections
-- | § | Covers | Checks |
-- |---|---|---|
-- | 1 | outbound — every fact key resolves | 5 |
-- | 2 | the same defects counted in trips | 4 |
-- | 3 | inbound — is each dimension reachable | 3 |
--
-- Then 4. Audit log · 5. Results · 6. Gate.


SET TIME ZONE 'UTC';
USE CATALOG nyc_mobility;

DECLARE OR REPLACE VARIABLE v_run_id STRING;
DECLARE OR REPLACE VARIABLE v_run_ts TIMESTAMP;
DECLARE OR REPLACE VARIABLE v_as_of  STRING;

SET VAR v_run_id = uuid();
SET VAR v_run_ts = current_timestamp();

-- Same as-of label as gold_qc, derived the same way, so the two notebooks'
-- rows line up in the dashboard even when they run hours apart.
SET VAR v_as_of = COALESCE(
    (SELECT date_format(MAX(pickup_date), 'yyyy-MM')
     FROM   nyc_gold.fact_taxi_trip
     WHERE  pickup_date IS NOT NULL),
    'none');

SELECT v_run_id AS run_id, v_run_ts AS run_ts, v_as_of AS as_of;

DECLARE OR REPLACE VARIABLE v_strict_pct   DOUBLE;
DECLARE OR REPLACE VARIABLE v_tol_pct      DOUBLE;
DECLARE OR REPLACE VARIABLE v_advisory_pct DOUBLE;

SET VAR v_strict_pct   = 0.0;
SET VAR v_tol_pct      = 10.0;
SET VAR v_advisory_pct = 100.0;


-- ## The blocking list
--
-- Zones block. A zone id with no dimension row breaks the borough breakdown,
-- which is the main thing the dashboard does, and the fix is upstream.
--
-- `weather_key_resolves` is NOT here, for a specific reason: the cause is a
-- stale fact table, not corrupt data. dim_weather is correct and a rebuild of
-- fact_taxi_trip clears it. Blocking does not rebuild anything, and a gate
-- that raises on a known open item stops being read. Move it here once a
-- rebuild has been confirmed at zero.
--
-- The two date checks are not here either — see the note on §1 below.

CREATE OR REPLACE TEMPORARY VIEW vw_at_rest_blocking_checks AS
SELECT * FROM VALUES
    ('fact_taxi_trip', 'pickup_zone_resolves'),
    ('fact_taxi_trip', 'dropoff_zone_resolves')
AS blocking(table_name, check_name);

DECLARE OR REPLACE VARIABLE v_required_tables ARRAY<STRING>;
SET VAR v_required_tables = array('fact_taxi_trip');


-- ## Clear-down
DELETE FROM nyc_quality.dq_results
WHERE layer = 'at_rest' AND batch_month = v_as_of;

DELETE FROM nyc_quality.dq_run_log
WHERE layer = 'at_rest' AND batch_month = v_as_of;


-- # 1. Outbound — every fact key resolves
--
-- Five anti-joins against dimensions of 265, ~2,200 and ~92 rows. Nothing is
-- multiplied.
--
-- ## The two date checks, and why they are tolerated
--
-- `fact.pickup_date` is a DATE; dim_date's key is `date_key INT`, so the join
-- is on `full_date`. Worth stating, because the column comment calls
-- pickup_date a foreign key to dim_date — and it is, just not to the column
-- named `_key`.
--
-- These two are counted in TRIPS, not in distinct dates, and that is the
-- whole reason they are tolerated rather than strict. dim_date is derived
-- from the span of the weather feed and the trips, by design. TLC files
-- reliably carry a handful of trips dated years outside the file month --
-- Silver flags them WARN and keeps them -- so those dates sit outside the
-- calendar and always will.
--
-- Over ~100 distinct dates, that handful is about 8 percent: a permanent FAIL
-- at any tolerance, purely because the denominator is small. The same defect
-- over 133,367 trips is 0.008 percent. A month genuinely missing from the
-- calendar is about a third of the rows either way, so the row denominator
-- still catches the regression while tolerating the convention.

INSERT INTO nyc_quality.dq_results (
    run_id, run_ts, layer, table_name, check_category, check_name,
    failed_rows, total_rows, failed_pct, threshold_pct, status,
    batch_month, min_failed_rows, warn_pct
)
WITH pu AS (
    SELECT COUNT(*) AS unmatched,
           (SELECT COUNT(DISTINCT pickup_location_id)
            FROM nyc_gold.fact_taxi_trip) AS total
    FROM (
        SELECT DISTINCT f.pickup_location_id
        FROM       nyc_gold.fact_taxi_trip f
        LEFT  JOIN nyc_gold.dim_taxi_zone  z ON f.pickup_location_id = z.location_id
        WHERE  f.pickup_location_id IS NOT NULL AND z.location_id IS NULL
    )
),
do_ AS (
    SELECT COUNT(*) AS unmatched,
           (SELECT COUNT(DISTINCT dropoff_location_id)
            FROM nyc_gold.fact_taxi_trip) AS total
    FROM (
        SELECT DISTINCT f.dropoff_location_id
        FROM       nyc_gold.fact_taxi_trip f
        LEFT  JOIN nyc_gold.dim_taxi_zone  z ON f.dropoff_location_id = z.location_id
        WHERE  f.dropoff_location_id IS NOT NULL AND z.location_id IS NULL
    )
),
pd AS (
    SELECT SUM(CASE WHEN f.pickup_date IS NOT NULL AND d.full_date IS NULL
                    THEN 1 ELSE 0 END)  AS unmatched,
           COUNT(*)                     AS total
    FROM       nyc_gold.fact_taxi_trip f
    LEFT  JOIN nyc_gold.dim_date       d ON f.pickup_date = d.full_date
),
dd AS (
    SELECT SUM(CASE WHEN f.dropoff_date IS NOT NULL AND d.full_date IS NULL
                    THEN 1 ELSE 0 END)  AS unmatched,
           COUNT(*)                     AS total
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
        WHERE  f.weather_key IS NOT NULL AND w.weather_key IS NULL
    )
),
checks AS (
    SELECT 'at_rest_integrity' AS check_category, 'pickup_zone_resolves' AS check_name,
           v_strict_pct AS threshold_pct,
           (SELECT unmatched FROM pu) AS failed_rows,
           (SELECT total FROM pu)     AS total_rows
    UNION ALL SELECT 'at_rest_integrity', 'dropoff_zone_resolves', v_strict_pct,
           (SELECT unmatched FROM do_), (SELECT total FROM do_)
    -- Tolerated, and not blocking: see the note above.
    UNION ALL SELECT 'at_rest_integrity', 'pickup_date_resolves', v_tol_pct,
           (SELECT unmatched FROM pd), (SELECT total FROM pd)
    UNION ALL SELECT 'at_rest_integrity', 'dropoff_date_resolves', v_tol_pct,
           (SELECT unmatched FROM dd), (SELECT total FROM dd)
    -- Only keys that were actually set. A NULL weather_key is "no weather
    -- matched", which is the advisory in §2, not a broken reference.
    UNION ALL SELECT 'at_rest_integrity', 'weather_key_resolves', v_strict_pct,
           (SELECT unmatched FROM wk), (SELECT total FROM wk)
)
SELECT
    v_run_id, v_run_ts, 'at_rest', 'fact_taxi_trip',
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    threshold_pct,
    CASE WHEN total_rows = 0                                               THEN 'SKIP'
         WHEN failed_rows = 0                                              THEN 'PASS'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= threshold_pct THEN 'WARN'
         ELSE 'FAIL' END,
    v_as_of,
    -- No row floor anywhere in this notebook. These are counted in DISTINCT
    -- KEYS: the counts are already small by construction, and a floor of 5
    -- would switch off "five zones have no dimension row", which is the exact
    -- defect the section exists to find.
    0,
    0.0
FROM checks;


-- # 2. The same defects, counted in trips
--
-- Advisory at 100%: one defect should not stop the pipeline twice. These
-- exist so a reviewer can see the blast radius next to the cause.

INSERT INTO nyc_quality.dq_results (
    run_id, run_ts, layer, table_name, check_category, check_name,
    failed_rows, total_rows, failed_pct, threshold_pct, status,
    batch_month, min_failed_rows, warn_pct
)
WITH rows_hit AS (
    SELECT
        COUNT(*)                                                   AS n_trips,
        SUM(CASE WHEN f.pickup_location_id IS NOT NULL
                  AND zp.location_id IS NULL THEN 1 ELSE 0 END)    AS t_pu,
        SUM(CASE WHEN f.dropoff_location_id IS NOT NULL
                  AND zd.location_id IS NULL THEN 1 ELSE 0 END)    AS t_do,
        SUM(CASE WHEN f.weather_key IS NULL THEN 1 ELSE 0 END)     AS t_no_weather,
        SUM(CASE WHEN f.weather_key IS NOT NULL
                  AND w.weather_key IS NULL THEN 1 ELSE 0 END)     AS t_bad_weather
    FROM       nyc_gold.fact_taxi_trip f
    LEFT  JOIN nyc_gold.dim_taxi_zone  zp ON f.pickup_location_id  = zp.location_id
    LEFT  JOIN nyc_gold.dim_taxi_zone  zd ON f.dropoff_location_id = zd.location_id
    LEFT  JOIN nyc_gold.dim_weather    w  ON f.weather_key         = w.weather_key
),
checks AS (
    SELECT 'at_rest_integrity' AS check_category,
           'trips_with_unmatched_pickup_zone' AS check_name,
           t_pu AS failed_rows, n_trips AS total_rows FROM rows_hit
    UNION ALL SELECT 'at_rest_integrity', 'trips_with_unmatched_dropoff_zone',
           t_do,          n_trips FROM rows_hit
    UNION ALL SELECT 'at_rest_integrity', 'trips_with_unmatched_weather_key',
           t_bad_weather, n_trips FROM rows_hit
    -- Not a broken reference: the LEFT JOIN in the fact build found no weather
    -- hour for this trip. Expected at the edges of a month, and the number to
    -- watch when the UTC conversion is in question.
    UNION ALL SELECT 'at_rest_integrity', 'trips_without_weather',
           t_no_weather,  n_trips FROM rows_hit
)
SELECT
    v_run_id, v_run_ts, 'at_rest', 'fact_taxi_trip',
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    v_advisory_pct,
    CASE WHEN total_rows = 0   THEN 'SKIP'
         WHEN failed_rows = 0  THEN 'PASS'
         ELSE 'WARN' END,
    v_as_of, 0, 0.0
FROM checks;


-- # 3. Inbound — is each dimension reachable
--
-- The other direction. An unused dimension row is not an error: a zone with
-- no trips is normal, and a calendar day with no trips is normal. But a
-- dimension where MOST rows are unused usually means the key convention
-- drifted between the dimension and the fact — the join is not failing, it is
-- succeeding on a shrinking subset, and nothing in §1 can see that.
--
-- Advisory throughout. The number is the signal, not the status.

INSERT INTO nyc_quality.dq_results (
    run_id, run_ts, layer, table_name, check_category, check_name,
    failed_rows, total_rows, failed_pct, threshold_pct, status,
    batch_month, min_failed_rows, warn_pct
)
WITH checks AS (
    SELECT 'at_rest_integrity' AS check_category,
           'weather_hours_used_by_a_trip' AS check_name,
           (SELECT COUNT(*) FROM (
               SELECT w.weather_key FROM nyc_gold.dim_weather w
               LEFT JOIN (SELECT DISTINCT weather_key FROM nyc_gold.fact_taxi_trip) f
                      ON w.weather_key = f.weather_key
               WHERE f.weather_key IS NULL)) AS failed_rows,
           (SELECT COUNT(*) FROM nyc_gold.dim_weather) AS total_rows
    UNION ALL SELECT 'at_rest_integrity', 'zones_used_by_a_trip',
           (SELECT COUNT(*) FROM (
               SELECT z.location_id FROM nyc_gold.dim_taxi_zone z
               LEFT JOIN (SELECT DISTINCT pickup_location_id AS location_id
                          FROM nyc_gold.fact_taxi_trip
                          UNION
                          SELECT DISTINCT dropoff_location_id
                          FROM nyc_gold.fact_taxi_trip) f
                      ON z.location_id = f.location_id
               WHERE f.location_id IS NULL)),
           (SELECT COUNT(*) FROM nyc_gold.dim_taxi_zone)
    UNION ALL SELECT 'at_rest_integrity', 'calendar_days_used_by_a_trip',
           (SELECT COUNT(*) FROM (
               SELECT d.full_date FROM nyc_gold.dim_date d
               LEFT JOIN (SELECT DISTINCT pickup_date AS full_date
                          FROM nyc_gold.fact_taxi_trip) f
                      ON d.full_date = f.full_date
               WHERE f.full_date IS NULL)),
           (SELECT COUNT(*) FROM nyc_gold.dim_date)
)
SELECT
    v_run_id, v_run_ts, 'at_rest', 'fact_taxi_trip',
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    v_advisory_pct,
    CASE WHEN total_rows = 0   THEN 'SKIP'
         WHEN failed_rows = 0  THEN 'PASS'
         ELSE 'WARN' END,
    v_as_of, 0, 0.0
FROM checks;


-- # 4. Audit log

INSERT INTO nyc_quality.dq_run_log (
    run_id, run_ts, layer, tables_checked, checks_run,
    checks_passed, checks_warned, checks_failed, overall_status, finished_at,
    batch_month, checks_skipped
)
SELECT
    v_run_id, v_run_ts, 'at_rest',
    COUNT(DISTINCT d.table_name),
    COUNT(*),
    SUM(CASE WHEN d.status = 'PASS' THEN 1 ELSE 0 END),
    SUM(CASE WHEN d.status = 'WARN' THEN 1 ELSE 0 END),
    SUM(CASE WHEN d.status = 'FAIL' THEN 1 ELSE 0 END),
    CASE WHEN SUM(CASE WHEN d.status = 'FAIL' AND b.check_name IS NOT NULL
                       THEN 1 ELSE 0 END) > 0                          THEN 'FAIL'
         WHEN SUM(CASE WHEN d.status IN ('FAIL','WARN') THEN 1 ELSE 0 END) > 0 THEN 'WARN'
         ELSE 'PASS' END,
    current_timestamp(),
    v_as_of,
    SUM(CASE WHEN d.status = 'SKIP' THEN 1 ELSE 0 END)
FROM       nyc_quality.dq_results d
LEFT  JOIN vw_at_rest_blocking_checks b
       ON  b.table_name = d.table_name AND b.check_name = d.check_name
WHERE  d.layer = 'at_rest'
  AND  d.batch_month = v_as_of
  AND  d.check_category <> 'gate';


-- # 5. Results

SELECT * FROM nyc_quality.dq_run_log
WHERE  layer = 'at_rest' AND batch_month = v_as_of;

-- The referential report on its own, ordered by size of the defect.
SELECT check_name, failed_rows, total_rows, failed_pct, threshold_pct, status
FROM   nyc_quality.dq_results
WHERE  layer = 'at_rest' AND batch_month = v_as_of
  AND  check_category = 'at_rest_integrity'
ORDER  BY CASE status WHEN 'FAIL' THEN 0 WHEN 'WARN' THEN 1 ELSE 2 END,
          failed_rows DESC;

-- The zone ids that do not resolve, named. Five rows here is five things to
-- fix; the count alone does not tell you where to look.
SELECT 'pickup' AS side, f.pickup_location_id AS location_id, COUNT(*) AS trips
FROM       nyc_gold.fact_taxi_trip f
LEFT  JOIN nyc_gold.dim_taxi_zone  z ON f.pickup_location_id = z.location_id
WHERE  f.pickup_location_id IS NOT NULL AND z.location_id IS NULL
GROUP  BY f.pickup_location_id
UNION ALL
SELECT 'dropoff', f.dropoff_location_id, COUNT(*)
FROM       nyc_gold.fact_taxi_trip f
LEFT  JOIN nyc_gold.dim_taxi_zone  z ON f.dropoff_location_id = z.location_id
WHERE  f.dropoff_location_id IS NOT NULL AND z.location_id IS NULL
GROUP  BY f.dropoff_location_id
ORDER  BY trips DESC;


-- # 6. Gate

DECLARE OR REPLACE VARIABLE v_gate_enforce BOOLEAN;
SET VAR v_gate_enforce = TRUE;

DECLARE OR REPLACE VARIABLE v_max_total_failures INT;
SET VAR v_max_total_failures = 5;

-- Inert blocking pairs. Expect zero rows.
SELECT b.table_name, b.check_name AS blocking_pair_never_produced
FROM   vw_at_rest_blocking_checks b
LEFT   JOIN (SELECT DISTINCT table_name, check_name
             FROM   nyc_quality.dq_results
             WHERE  layer = 'at_rest' AND batch_month = v_as_of) r
       ON  b.table_name = r.table_name AND b.check_name = r.check_name
WHERE  r.check_name IS NULL;

CREATE OR REPLACE TEMPORARY VIEW vw_at_rest_gate AS
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
    LEFT  JOIN vw_at_rest_blocking_checks b
           ON  b.table_name = d.table_name AND b.check_name = d.check_name
    WHERE  d.layer = 'at_rest' AND d.batch_month = v_as_of
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
FROM   vw_at_rest_gate;

INSERT INTO nyc_quality.dq_results (
    run_id, run_ts, layer, table_name, check_category, check_name,
    failed_rows, total_rows, failed_pct, threshold_pct, status,
    batch_month, min_failed_rows, warn_pct
)
SELECT v_run_id, v_run_ts, 'at_rest', table_name,
       'gate', 'referential_integrity_holds',
       blocking_failures, 1,
       CASE WHEN verdict = 'STOP' THEN 100.0 ELSE 0.0 END,
       0.0,
       CASE WHEN verdict = 'STOP' THEN 'FAIL' ELSE 'PASS' END,
       v_as_of, 0, 0.0
FROM   vw_at_rest_gate;

DECLARE OR REPLACE VARIABLE v_stopped_required STRING;

SET VAR v_stopped_required = (
    SELECT COALESCE(concat_ws(', ', collect_list(table_name)), '')
    FROM   vw_at_rest_gate WHERE verdict = 'STOP' AND is_required);

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
    FROM   vw_at_rest_gate WHERE verdict = 'STOP' AND is_required);

SELECT CASE
    WHEN v_gate_enforce AND v_stopped_required <> ''
      THEN raise_error(CONCAT('At-rest integrity FAILED as of ', v_as_of,
                              ' -- ', v_stopped_detail,
                              '. Foreign keys that do not resolve. '
                              'See nyc_quality.dq_results for run ', v_run_id))
    ELSE CONCAT('At-rest integrity PASSED as of ', v_as_of)
END AS gate;
