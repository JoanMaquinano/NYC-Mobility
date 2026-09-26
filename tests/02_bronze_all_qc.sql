-- Data Quality- Bronze
-- Sections
-- | § | Covers | Checks |
-- |---|---|---|
-- | 1 | `green_taxi`, lineage | 3 |
-- | 2 | `taxi_zones`, lineage and MERGE key | 5 |
-- | 3 | `weather`, lineage, MERGE key, coverage | 6 |
-- | 4 | referential integrity, trips → zones | 4 |
-- | 5 | load fidelity, `green_taxi` vs landed Parquet | 21 |
-- | 5b | load fidelity, `weather` vs landed CSV | 3 |
-- | | **total** | **42** |
-- Then 6. Audit log · 7. Results · 8. Gate · 9. Afterwards.

SET TIME ZONE 'UTC';
USE CATALOG nyc_mobility;

-- ## Parameters — the same two the ingestion notebook takes
-- | Parameter | Example | Also used by |
-- |---|---|---|
-- | `year_month`   | `2026-03`                | the green_taxi MERGE |
-- | `weather_file` | `weather_march_2026.csv` | the weather MERGE |
--
-- ## Both are optional
-- If Left blank, the month falls back to whatever the preload notebook decided
-- and recorded in `dq_run_log`. Preload picks the oldest month that has
-- landed and is not yet in Bronze, so a run with no parameters at all still
-- describes the batch that was actually loaded -- and every layer of that run
-- agrees on which month it was.
-- `weather_file` falls back to deriving the name from the month:
-- `2026-03` -> `weather_march_2026.csv`.

DECLARE OR REPLACE VARIABLE v_batch_month  STRING;
DECLARE OR REPLACE VARIABLE v_weather_file STRING;
DECLARE OR REPLACE VARIABLE v_month_source STRING;
DECLARE OR REPLACE VARIABLE v_run_id       STRING;
DECLARE OR REPLACE VARIABLE v_run_ts       TIMESTAMP;

SET VAR v_run_id = uuid();
SET VAR v_run_ts = current_timestamp();

SET VAR v_batch_month = COALESCE(
    NULLIF(:year_month, ''),
    (SELECT batch_month
     FROM   nyc_quality.dq_run_log
     WHERE  layer = 'preload' AND batch_month IS NOT NULL
     ORDER  BY run_ts DESC
     LIMIT  1));

SET VAR v_month_source = CASE
    WHEN COALESCE(:year_month, '') <> '' THEN 'parameter'
    ELSE 'preload run log' END;

SET VAR v_weather_file = COALESCE(
    NULLIF(:weather_file, ''),
    concat('weather_',
           lower(date_format(to_date(concat(v_batch_month, '-01')), 'MMMM')),
           '_', substr(v_batch_month, 1, 4), '.csv'));


-- ## Validate before checking anything
-- A typo like `2026-3` does not error on its own: it matches zero rows, every
-- check SKIPs, and the run reads as almost clean. 
-- Note what is NOT an error below: a table with rows in Bronze but none for
-- this month. That is a real finding, and `table_not_empty` reports it. Only
-- an unusable parameter stops the run this early.
SELECT CASE
    WHEN v_batch_month IS NULL
      THEN raise_error('No year_month given and no preload run found in '
                    || 'nyc_quality.dq_run_log. Pass year_month, or run the '
                    || 'preload notebook first.')
    WHEN NOT (v_batch_month RLIKE '^[0-9]{4}-[0-9]{2}$')
      THEN raise_error(CONCAT('year_month must be YYYY-MM (e.g. 2026-03), got: ',
                              v_batch_month))
    WHEN try_to_timestamp(concat(v_batch_month, '-01')) IS NULL
      THEN raise_error(CONCAT('year_month is not a real month: ', v_batch_month))
    ELSE CONCAT('Checking ', v_batch_month) END AS validation;

-- ## taxi_zones has no month in it
-- It is a 265-row reference file that does not change month to month, so
-- stamping its results with the trip month would claim it was re-validated
-- each time and fill the dashboard with identical rows.
--
-- Preload keys it on the lookup file's modification date instead --
-- `static-2026-09-16` -- so an unchanged file replaces its own rows and a
-- REPLACED file starts a new set, keeping the record of the old one. 
DECLARE OR REPLACE VARIABLE v_zones_batch STRING;

SET VAR v_zones_batch = COALESCE(
    (SELECT batch_month
     FROM   nyc_quality.dq_results
     WHERE  layer = 'preload' AND table_name = 'taxi_zones'
       AND  batch_month IS NOT NULL
     ORDER  BY run_ts DESC
     LIMIT  1),
    'static-unknown');
-- One row per table saying which batch its results belong to. Everything
-- below joins to this rather than assuming a single key for the run.
CREATE OR REPLACE TEMPORARY VIEW vw_batch_scope AS
          SELECT 'green_taxi' AS table_name, v_batch_month AS batch_month
UNION ALL SELECT 'weather',                  v_batch_month
UNION ALL SELECT 'taxi_zones',               v_zones_batch;

SELECT v_run_id       AS run_id,
       v_run_ts       AS run_ts,
       v_batch_month  AS batch_month,
       v_month_source AS month_from,
       v_weather_file AS weather_file,
       v_zones_batch  AS zones_version;


-- ## Threshold policy
-- | Variable | Value | Meaning |
-- |---|---|---|
-- | `v_strict_pct`   | `0.0`   | must never happen: one occurrence corrupts an aggregate, a join or the grain -- or the check is scalar, where a percentage is 0 or 100 and nothing between |
-- | `v_tol_pct`      | `10.0`  | the source is known to be imperfect and this much is tolerated |
-- | `v_advisory_pct` | `100.0` | reported every run, can only ever WARN, never gates |
--
-- ### Why a row floor as well as a percentage
-- A percentage alone cannot express "this is serious but one row of it is
-- not worth stopping a pipeline for". `dropoff_after_pickup` found exactly
-- 1 row in 44,208. At 0.0% that is a FAIL; at 10.0% the rule stops meaning
-- anything. `v_min_rows` is the third option: under 5 rows the check WARNs
-- whatever the percentage says, at 5 or more the percentage decides. Strict
-- about the rule, quiet about the singleton.
--
-- Two checks are exempt from the floor and stay absolute:
-- `location_id_unique` and `one_row_per_hour`. Both are primary keys. One
-- duplicate key multiplies rows through every downstream join, which is not
-- the same kind of damage as one implausible fare.
--
-- ### A second threshold, for the cast checks only
-- One threshold can only draw one line, so it cannot say "5% is worth
-- looking at, 10% is worth stopping for". `warn_pct` is the lower line:
--
-- | Band | Status |
-- |---|---|
-- | no failures | PASS |
-- | at or under `warn_pct` | PASS -- within tolerance, still recorded |
-- | over `warn_pct`, under `threshold_pct` | WARN |
-- | over `threshold_pct` | FAIL |
--
-- Every check in sections 1-4 has `warn_pct = 0`, which means any failure at
-- all is at least a WARN -- their behaviour is unchanged. Only the 19
-- `no_nulls_added_*` checks use the 5/10 pair.
--
-- Because a sub-5% cast failure now reads PASS rather than WARN, section 7
-- has a query for PASSes that are not clean. Nothing disappears; it just
-- stops competing for attention with the real warnings.

DECLARE OR REPLACE VARIABLE v_strict_pct   DOUBLE;
DECLARE OR REPLACE VARIABLE v_tol_pct      DOUBLE;
DECLARE OR REPLACE VARIABLE v_advisory_pct DOUBLE;
DECLARE OR REPLACE VARIABLE v_min_rows     BIGINT;

-- Load fidelity splits into two kinds of finding, so it gets its own pair of
-- thresholds. See the note above section 5.
DECLARE OR REPLACE VARIABLE v_cast_warn_pct DOUBLE;
DECLARE OR REPLACE VARIABLE v_cast_fail_pct DOUBLE;

SET VAR v_strict_pct   = 0.0;
SET VAR v_tol_pct      = 10.0;
SET VAR v_advisory_pct = 100.0;
SET VAR v_min_rows     = 5;
SET VAR v_cast_warn_pct = 5.0;
SET VAR v_cast_fail_pct = 10.0;


-- ## Batch scope
CREATE OR REPLACE TEMPORARY VIEW vw_batch_green_taxi AS
SELECT *
FROM   nyc_bronze.green_taxi
-- regexp_extract rather than an equality on the filename: source_file is
-- written as a bare name today and could be written as a full path
-- tomorrow, and the month is what identifies the batch either way.
WHERE  regexp_extract(source_file, '([0-9]{4}-[0-9]{2})', 1) = v_batch_month;

CREATE OR REPLACE TEMPORARY VIEW vw_batch_weather AS
SELECT *
FROM   nyc_bronze.weather
WHERE  (v_weather_file <> '' AND source_file_month = v_weather_file)
   OR  (v_weather_file =  '' AND date_format(try_cast(`date` AS TIMESTAMP), 'yyyy-MM') = v_batch_month);

-- taxi_zones is deliberately NOT scoped. It is a static 265-row dimension
-- with no month in it, so every batch re-checks the whole thing. Its rows
-- are stamped with the current batch_month so the batch view is complete;
-- identical results across months are expected, not duplication.
SELECT (SELECT COUNT(*) FROM vw_batch_green_taxi) AS taxi_rows_in_batch,
       (SELECT COUNT(*) FROM vw_batch_weather)    AS weather_rows_in_batch,
       (SELECT COUNT(*) FROM nyc_bronze.taxi_zones) AS zone_rows;


-- ## The blocking list
-- A gate is a claim that the data is unusable, not that it is imperfect. Two
-- things decide what belongs here:
--   1. **Can Silver fix it?** A NULL pickup timestamp, a negative distance, a
--      zone id with no match -- Silver quarantines or flags those row by row.
--      Stopping the load instead throws away the 99.9% that was fine.
--   2. **Is the damage confined to the row?** A duplicated primary key is
--      not: it multiplies through every join downstream, so one bad row
--      becomes thousands of wrong ones. A loader that silently cast a real
--      value to NULL is not either: the original value is gone and nothing
--      downstream can recover it.


CREATE OR REPLACE TEMPORARY VIEW vw_bronze_blocking_checks AS
SELECT * FROM VALUES
    -- green_taxi -- 22 pairs, 21 of them loader fidelity
    ('green_taxi', 'table_not_empty'),
    ('green_taxi', 'row_count_matches_source'),
    ('green_taxi', 'every_landed_file_is_loaded'),
    ('green_taxi', 'no_nulls_added_vendor_id'),
    ('green_taxi', 'no_nulls_added_pu_location_id'),
    ('green_taxi', 'no_nulls_added_do_location_id'),
    ('green_taxi', 'no_nulls_added_ratecode_id'),
    ('green_taxi', 'no_nulls_added_payment_type'),
    ('green_taxi', 'no_nulls_added_trip_type'),
    ('green_taxi', 'no_nulls_added_passenger_count'),
    ('green_taxi', 'no_nulls_added_store_and_fwd_flag'),
    ('green_taxi', 'no_nulls_added_pickup_datetime'),
    ('green_taxi', 'no_nulls_added_dropoff_datetime'),
    ('green_taxi', 'no_nulls_added_trip_distance'),
    ('green_taxi', 'no_nulls_added_fare_amount'),
    ('green_taxi', 'no_nulls_added_total_amount'),
    ('green_taxi', 'no_nulls_added_extra'),
    ('green_taxi', 'no_nulls_added_mta_tax'),
    ('green_taxi', 'no_nulls_added_tip_amount'),
    ('green_taxi', 'no_nulls_added_tolls_amount'),
    ('green_taxi', 'no_nulls_added_improvement_surcharge'),
    ('green_taxi', 'no_nulls_added_congestion_surcharge'),

    -- taxi_zones -- the lookup is a primary key or it is nothing
    ('taxi_zones', 'table_not_empty'),
    ('taxi_zones', 'location_id_unique'),

    -- weather -- date is the MERGE key
    ('weather', 'table_not_empty'),
    ('weather', 'row_count_matches_source'),
    ('weather', 'every_landed_file_is_loaded'),
    ('weather', 'one_row_per_hour')
AS blocking(table_name, check_name);


-- ## Which tables must be healthy for the run to continue
--
-- A per-table gate needs to know which tables the run cannot proceed
-- without. green_taxi is the fact table: no trips, no pipeline. Weather and
-- zones enrich it, so a bad batch of either stops its own path into Silver
-- and leaves the trip path alone.
--
-- Add a table name here to make its failure fatal to the whole run.
DECLARE OR REPLACE VARIABLE v_required_tables ARRAY<STRING>;
SET VAR v_required_tables = array('green_taxi');


-- ## Re-running a month replaces that month
-- v1 deleted on run_id, which is a fresh uuid every run and therefore
-- matched nothing -- the DELETE was decoration and the table accumulated a
-- new set of rows per execution. The key that actually identifies "the same
-- work done again" is (layer, batch_month), so that is what is deleted.
--
-- This matters for the idempotency requirement. Running May twice has to
-- leave the dataset unchanged; it should leave the QC record unchanged
-- too, not two conflicting verdicts for May. The history that is kept is
-- one authoritative row set per month, which is exactly what the per-month
-- dashboard reads.
-- Per table, because taxi_zones lives in a batch of its own. One DELETE on
-- the month alone would leave the previous zone rows behind and the INSERT
-- would double them.
DELETE FROM nyc_quality.dq_results
WHERE layer = 'bronze' AND check_category <> 'gate'
  AND ( (table_name IN ('green_taxi','weather') AND batch_month = v_batch_month)
     OR (table_name = 'taxi_zones'              AND batch_month = v_zones_batch) );
-- Gate verdicts are cleared separately because they are written later
DELETE FROM nyc_quality.dq_results
WHERE layer = 'bronze' AND check_category = 'gate'
  AND ( (table_name IN ('green_taxi','weather') AND batch_month = v_batch_month)
     OR (table_name = 'taxi_zones'              AND batch_month = v_zones_batch) );

DELETE FROM nyc_quality.dq_run_log
WHERE layer = 'bronze' AND batch_month = v_batch_month;



INSERT INTO nyc_quality.dq_results (
    run_id, run_ts, layer, table_name, check_category, check_name,
    failed_rows, total_rows, failed_pct, threshold_pct, status,
    batch_month, min_failed_rows, warn_pct
)
WITH metrics AS (
    SELECT
        COUNT(*)                                                    AS total_rows,
        -- Explicitly fail an empty load once, instead of producing many
        -- misleading downstream failures from NULL aggregate results.
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END                    AS t_empty,
        SUM(CASE WHEN source_file    IS NULL THEN 1 ELSE 0 END)     AS c_lineage,
        SUM(CASE WHEN ingestion_time IS NULL THEN 1 ELSE 0 END)     AS c_ingested
    FROM vw_batch_green_taxi
),
checks AS (
    SELECT 'completeness' AS check_category, 'table_not_empty' AS check_name,
           v_strict_pct AS threshold_pct, t_empty AS failed_rows, 1 AS total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'source_file_recorded',    v_strict_pct, c_lineage,  total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'ingestion_time_recorded', v_tol_pct,    c_ingested, total_rows FROM metrics
)
SELECT
    v_run_id, v_run_ts, 'bronze', 'green_taxi',
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    threshold_pct,
    -- Status, in the order the tests are applied.
    --   SKIP  the batch had no rows. Only table_not_empty reports that;
    --         the other checks stay quiet instead of each inventing its own
    --         failure out of a NULL aggregate. The empty weather table
    --         produced 35 FAIL rows in the first March run, all of them
    --         restating one fact.
    --   PASS  nothing broke the rule.
    --   WARN  broken, but under the row floor OR under the rate threshold.
    --   FAIL  over both.
    CASE WHEN total_rows = 0 AND check_name <> 'table_not_empty'           THEN 'SKIP'
         WHEN failed_rows = 0                                              THEN 'PASS'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= warn_pct      THEN 'PASS'
         WHEN failed_rows <= min_failed_rows                               THEN 'WARN'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= threshold_pct THEN 'WARN'
         ELSE 'FAIL' END,
    v_batch_month,
    min_failed_rows,
    warn_pct
FROM (
    SELECT c.*,
           -- Scalar checks (total_rows = 1) get no floor: their percentage is
           -- 0 or 100 and nothing between, so a floor would switch them off.
           -- Primary keys get no floor either -- see the policy note above.
           CASE WHEN c.total_rows <= 1                                THEN 0
                -- Primary keys, and the not-null checks that guard them. A
                -- NULL key does not join, so those rows do not become wrong
                -- in Gold -- they silently are not there. Same damage as a
                -- duplicate key, so the same absolute treatment.
                WHEN c.check_name IN ('location_id_unique',
                                      'one_row_per_hour',
                                      'location_id_not_null',
                                      'date_not_null')                THEN 0
                ELSE v_min_rows END AS min_failed_rows,
           -- 0 means "any failure is at least a WARN" -- the behaviour these
           -- sections already had.
           0.0 AS warn_pct
    FROM checks c
);

-- # 2. taxi_zones — lineage and the MERGE key
-- `location_id_unique` stays and is blocking. The zones MERGE matches on
-- `location_id`, so a duplicate here is not a duplicate in the CSV -- preload
-- checks that -- it is the MERGE having failed to deduplicate. One duplicate
-- key fans the join out in Gold and inflates every trip count touching that
-- zone.
-- `zone_names_shared_is_3` stays as an advisory: LocationIDs 103/104/105
-- share a zone name, so grouping by id and grouping by name give different
-- counts. Reported so the difference is explicit rather than surprising.

INSERT INTO nyc_quality.dq_results (
    run_id, run_ts, layer, table_name, check_category, check_name,
    failed_rows, total_rows, failed_pct, threshold_pct, status,
    batch_month, min_failed_rows, warn_pct
)
WITH metrics AS (
    SELECT
        COUNT(*)                                                    AS total_rows,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END                    AS t_empty,
        SUM(CASE WHEN source_file    IS NULL THEN 1 ELSE 0 END)     AS c_lineage,
        SUM(CASE WHEN ingestion_time IS NULL THEN 1 ELSE 0 END)     AS c_ingested,
        COUNT(location_id) - COUNT(DISTINCT location_id)            AS u_id_dupes
    FROM nyc_bronze.taxi_zones
),
-- Zone names shared by more than one id. 264 and 265 are both "Unknown" by
-- design and excluded; try_cast so a malformed id cannot take the cell down.
shared_names AS (
    SELECT COALESCE(SUM(n), 0) AS ids_sharing_a_name
    FROM (
        SELECT COUNT(*) AS n
        FROM   nyc_bronze.taxi_zones
        WHERE  COALESCE(try_cast(location_id AS INT), -1) NOT IN (264, 265)
        GROUP  BY `zone`
        HAVING COUNT(*) > 1
    )
),
checks AS (
    SELECT 'completeness' AS check_category, 'table_not_empty' AS check_name,
           v_strict_pct AS threshold_pct, t_empty AS failed_rows, 1 AS total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'source_file_recorded',    v_strict_pct, c_lineage,  total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'ingestion_time_recorded', v_tol_pct,    c_ingested, total_rows FROM metrics
    UNION ALL SELECT 'uniqueness',   'location_id_unique',      v_strict_pct, u_id_dupes, total_rows FROM metrics
    UNION ALL SELECT 'business',     'zone_names_shared_is_3',  v_advisory_pct,
                     CASE WHEN (SELECT ids_sharing_a_name FROM shared_names) = 3
                          THEN 0 ELSE 1 END, 1 FROM metrics
)
SELECT
    v_run_id, v_run_ts, 'bronze', 'taxi_zones',
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    threshold_pct,
    -- Status, in the order the tests are applied.
    --   SKIP  the batch had no rows. Only table_not_empty reports that;
    --         the other checks stay quiet instead of each inventing its own
    --         failure out of a NULL aggregate. The empty weather table
    --         produced 35 FAIL rows in the first March run, all of them
    --         restating one fact.
    --   PASS  nothing broke the rule.
    --   WARN  broken, but under the row floor OR under the rate threshold.
    --   FAIL  over both.
    CASE WHEN total_rows = 0 AND check_name <> 'table_not_empty'           THEN 'SKIP'
         WHEN failed_rows = 0                                              THEN 'PASS'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= warn_pct      THEN 'PASS'
         WHEN failed_rows <= min_failed_rows                               THEN 'WARN'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= threshold_pct THEN 'WARN'
         ELSE 'FAIL' END,
    v_zones_batch,
    min_failed_rows,
    warn_pct
FROM (
    SELECT c.*,
           -- Scalar checks (total_rows = 1) get no floor: their percentage is
           -- 0 or 100 and nothing between, so a floor would switch them off.
           -- Primary keys get no floor either -- see the policy note above.
           CASE WHEN c.total_rows <= 1                                THEN 0
                -- Primary keys, and the not-null checks that guard them. A
                -- NULL key does not join, so those rows do not become wrong
                -- in Gold -- they silently are not there. Same damage as a
                -- duplicate key, so the same absolute treatment.
                WHEN c.check_name IN ('location_id_unique',
                                      'one_row_per_hour',
                                      'location_id_not_null',
                                      'date_not_null')                THEN 0
                ELSE v_min_rows END AS min_failed_rows,
           -- 0 means "any failure is at least a WARN" -- the behaviour these
           -- sections already had.
           0.0 AS warn_pct
    FROM checks c
);

-- # 3. weather — lineage, the MERGE key, and coverage
-- `one_row_per_hour` is the counterpart to section 2's uniqueness check. The
-- weather MERGE matches on `date`, so a duplicate hour in Bronze means the
-- dedupe did not hold -- different from the duplicates preload counts in the
-- CSV, which the MERGE is expected to collapse.
--
-- `source_file_is_not_placeholder` catches the MERGE writing the parameter
-- name literally, which happens if the notebook runs as plain SQL rather than
-- through parameter substitution. Lineage is gone and nothing else notices.
--
-- `hour_within_covered_months` reports hours outside the months this table
-- actually covers. With UTC timestamps and a New York project the boundary
-- hours legitimately land either side, so it is worth seeing, not gating on.

INSERT INTO nyc_quality.dq_results (
    run_id, run_ts, layer, table_name, check_category, check_name,
    failed_rows, total_rows, failed_pct, threshold_pct, status,
    batch_month, min_failed_rows, warn_pct
)
WITH w AS (
    SELECT NULLIF(trim(`date`),            '') AS `date`,
           NULLIF(trim(`month`),           '') AS `month`,
           NULLIF(trim(source_file_month), '') AS source_file_month,
           ingestion_timestamp
    FROM   vw_batch_weather
),
-- Covered months are derived from the data, so no hardcoded month list is
-- needed. A month must contain at least two distinct days to exclude a single
-- boundary hour carried over from the previous month.
covered_months AS (
    SELECT date_trunc('MONTH', try_cast(`date` AS TIMESTAMP)) AS month_start
    FROM   w
    WHERE  try_cast(`date` AS TIMESTAMP) IS NOT NULL
    GROUP  BY date_trunc('MONTH', try_cast(`date` AS TIMESTAMP))
    HAVING COUNT(DISTINCT to_date(try_cast(`date` AS TIMESTAMP))) >= 2
),
flagged AS (
    SELECT w.*, c.month_start IS NOT NULL AS month_is_covered
    FROM       w
    LEFT JOIN  covered_months c
           ON  c.month_start = date_trunc('MONTH', try_cast(w.`date` AS TIMESTAMP))
),
metrics AS (
    SELECT
        COUNT(*)                                                     AS total_rows,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END                     AS t_empty,
        SUM(CASE WHEN source_file_month   IS NULL THEN 1 ELSE 0 END) AS c_lineage,
        SUM(CASE WHEN ingestion_timestamp IS NULL THEN 1 ELSE 0 END) AS c_ingested,
        COUNT(try_cast(`date` AS TIMESTAMP))
          - COUNT(DISTINCT try_cast(`date` AS TIMESTAMP))            AS u_date_dupes,
        SUM(CASE WHEN source_file_month LIKE '%{%}%' THEN 1 ELSE 0 END) AS x_placeholder,
        SUM(CASE WHEN try_cast(`date` AS TIMESTAMP) IS NOT NULL
                  AND NOT month_is_covered THEN 1 ELSE 0 END)        AS b_window
    FROM flagged
),
checks AS (
    SELECT 'completeness' AS check_category, 'table_not_empty' AS check_name,
           v_strict_pct AS threshold_pct, t_empty AS failed_rows, 1 AS total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'source_file_recorded',    v_strict_pct,   c_lineage,     total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'ingestion_time_recorded', v_tol_pct,      c_ingested,    total_rows FROM metrics
    UNION ALL SELECT 'uniqueness',   'one_row_per_hour',        v_strict_pct,   u_date_dupes,  total_rows FROM metrics
    UNION ALL SELECT 'consistency',  'source_file_is_not_placeholder', v_advisory_pct, x_placeholder, total_rows FROM metrics
    UNION ALL SELECT 'validity',     'hour_within_covered_months', v_strict_pct, b_window,     total_rows FROM metrics
)
SELECT
    v_run_id, v_run_ts, 'bronze', 'weather',
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    threshold_pct,
    -- Status, in the order the tests are applied.
    --   SKIP  the batch had no rows. Only table_not_empty reports that;
    --         the other checks stay quiet instead of each inventing its own
    --         failure out of a NULL aggregate. The empty weather table
    --         produced 35 FAIL rows in the first March run, all of them
    --         restating one fact.
    --   PASS  nothing broke the rule.
    --   WARN  broken, but under the row floor OR under the rate threshold.
    --   FAIL  over both.
    CASE WHEN total_rows = 0 AND check_name <> 'table_not_empty'           THEN 'SKIP'
         WHEN failed_rows = 0                                              THEN 'PASS'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= warn_pct      THEN 'PASS'
         WHEN failed_rows <= min_failed_rows                               THEN 'WARN'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= threshold_pct THEN 'WARN'
         ELSE 'FAIL' END,
    v_batch_month,
    min_failed_rows,
    warn_pct
FROM (
    SELECT c.*,
           -- Scalar checks (total_rows = 1) get no floor: their percentage is
           -- 0 or 100 and nothing between, so a floor would switch them off.
           -- Primary keys get no floor either -- see the policy note above.
           CASE WHEN c.total_rows <= 1                                THEN 0
                -- Primary keys, and the not-null checks that guard them. A
                -- NULL key does not join, so those rows do not become wrong
                -- in Gold -- they silently are not there. Same damage as a
                -- duplicate key, so the same absolute treatment.
                WHEN c.check_name IN ('location_id_unique',
                                      'one_row_per_hour',
                                      'location_id_not_null',
                                      'date_not_null')                THEN 0
                ELSE v_min_rows END AS min_failed_rows,
           -- 0 means "any failure is at least a WARN" -- the behaviour these
           -- sections already had.
           0.0 AS warn_pct
    FROM checks c
);

-- Check the units before trusting the weather ranges.
-- Scoped to the batch, so first_hour/last_hour also serve as a coverage
-- read-out for the month just loaded.
SELECT
    COUNT(*)                                          AS rows,
    MIN(try_cast(`date` AS TIMESTAMP))                AS first_hour,
    MAX(try_cast(`date` AS TIMESTAMP))                AS last_hour,
    ROUND(MIN(try_cast(temperature_2m AS DOUBLE)), 1) AS min_temp,
    ROUND(MAX(try_cast(temperature_2m AS DOUBLE)), 1) AS max_temp,
    CASE WHEN MAX(try_cast(temperature_2m AS DOUBLE)) > 45
         THEN 'LOOKS LIKE FAHRENHEIT — temperature_plausible is wrong'
         ELSE 'Looks like Celsius — ranges are right' END AS verdict,
    ROUND(MAX(try_cast(rain AS DOUBLE)), 2)           AS max_rain,
    ROUND(MAX(try_cast(visibility AS DOUBLE)), 0)     AS max_visibility,
    ROUND(MAX(try_cast(wind_speed_10m AS DOUBLE)), 1) AS max_wind
FROM vw_batch_weather;

-- 4. Referential integrity — trips to zones
-- Every pickup and drop-off location_id in green_taxi must exist in taxi_zones.
-- Missing matches would become Unknown in Gold, so this check catches lookup issues early.

-- TRY_CAST prevents malformed taxi_zones.location_id values from failing the join.
-- Invalid IDs simply do not match and are reported by location_id_in_range.

-- The gate counts distinct unmatched IDs, not affected trips.
-- One bad lookup ID is one issue to fix, even if it affects many trips.
-- But the key count alone does not say how much it costs. One unmatched
-- id can be a single test row or a fifth of the load, and those call for
-- very different reactions. So both are reported:
--
-- | Check | Answers |
-- |---|---|
-- | `pickup_zone_exists_in_lookup` | how many lookup keys need fixing — **blocking** |
-- | `trips_with_unmatched_pickup_zone` | how many trip rows are affected — advisory |

INSERT INTO nyc_quality.dq_results (
    run_id, run_ts, layer, table_name, check_category, check_name,
    failed_rows, total_rows, failed_pct, threshold_pct, status,
    batch_month, min_failed_rows, warn_pct
)
WITH pu AS (
    SELECT COUNT(*) AS unmatched, (SELECT COUNT(DISTINCT pu_location_id)
                                   FROM vw_batch_green_taxi) AS total
    FROM (
        SELECT DISTINCT t.pu_location_id
        FROM   vw_batch_green_taxi t
        LEFT   JOIN nyc_bronze.taxi_zones z
               ON t.pu_location_id = try_cast(z.location_id AS INT)
        WHERE  t.pu_location_id IS NOT NULL AND z.location_id IS NULL
    )
),
do_ AS (
    SELECT COUNT(*) AS unmatched, (SELECT COUNT(DISTINCT do_location_id)
                                   FROM vw_batch_green_taxi) AS total
    FROM (
        SELECT DISTINCT t.do_location_id
        FROM   vw_batch_green_taxi t
        LEFT   JOIN nyc_bronze.taxi_zones z
               ON t.do_location_id = try_cast(z.location_id AS INT)
        WHERE  t.do_location_id IS NOT NULL AND z.location_id IS NULL
    )
),
rows_hit AS (
    SELECT
        SUM(CASE WHEN t.pu_location_id IS NOT NULL AND zp.location_id IS NULL
                 THEN 1 ELSE 0 END)                             AS pu_rows,
        SUM(CASE WHEN t.do_location_id IS NOT NULL AND zd.location_id IS NULL
                 THEN 1 ELSE 0 END)                             AS do_rows,
        COUNT(*)                                                AS n_trips
    FROM   vw_batch_green_taxi t
    LEFT   JOIN nyc_bronze.taxi_zones zp
           ON t.pu_location_id = try_cast(zp.location_id AS INT)
    LEFT   JOIN nyc_bronze.taxi_zones zd
           ON t.do_location_id = try_cast(zd.location_id AS INT)
),
checks AS (
    SELECT 'consistency' AS check_category, 'pickup_zone_exists_in_lookup' AS check_name,
           v_strict_pct AS threshold_pct,
           (SELECT unmatched FROM pu) AS failed_rows,
           (SELECT total FROM pu)     AS total_rows
    UNION ALL
    SELECT 'consistency', 'dropoff_zone_exists_in_lookup', v_strict_pct,
           (SELECT unmatched FROM do_), (SELECT total FROM do_)
    UNION ALL
    SELECT 'consistency', 'trips_with_unmatched_pickup_zone', v_advisory_pct,
           (SELECT pu_rows FROM rows_hit), (SELECT n_trips FROM rows_hit)
    UNION ALL
    SELECT 'consistency', 'trips_with_unmatched_dropoff_zone', v_advisory_pct,
           (SELECT do_rows FROM rows_hit), (SELECT n_trips FROM rows_hit)
)

SELECT
    v_run_id, v_run_ts, 'bronze', 'green_taxi',
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    threshold_pct,
    -- Status, in the order the tests are applied.
    --   SKIP  the batch had no rows. Only table_not_empty reports that;
    --         the other checks stay quiet instead of each inventing its own
    --         failure out of a NULL aggregate. The empty weather table
    --         produced 35 FAIL rows in the first March run, all of them
    --         restating one fact.
    --   PASS  nothing broke the rule.
    --   WARN  broken, but under the row floor OR under the rate threshold.
    --   FAIL  over both.
    CASE WHEN total_rows = 0 AND check_name <> 'table_not_empty'           THEN 'SKIP'
         WHEN failed_rows = 0                                              THEN 'PASS'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= warn_pct      THEN 'PASS'
         WHEN failed_rows <= min_failed_rows                               THEN 'WARN'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= threshold_pct THEN 'WARN'
         ELSE 'FAIL' END,
    v_batch_month,
    min_failed_rows,
    warn_pct
FROM (
    SELECT c.*,
           -- Scalar checks (total_rows = 1) get no floor: their percentage is
           -- 0 or 100 and nothing between, so a floor would switch them off.
           -- Primary keys get no floor either -- see the policy note above.
           CASE WHEN c.total_rows <= 1                                THEN 0
                -- Primary keys, and the not-null checks that guard them. A
                -- NULL key does not join, so those rows do not become wrong
                -- in Gold -- they silently are not there. Same damage as a
                -- duplicate key, so the same absolute treatment.
                WHEN c.check_name IN ('location_id_unique',
                                      'one_row_per_hour',
                                      'location_id_not_null',
                                      'date_not_null')                THEN 0
                ELSE v_min_rows END AS min_failed_rows,
           -- 0 means "any failure is at least a WARN" -- the behaviour these
           -- sections already had.
           0.0 AS warn_pct
    FROM checks c

);

-- # 5. Load fidelity — did the cast lose anything?
-- **This section exists because `green_taxi` declares its schema and
-- casts.** It has no counterpart in the personal pipeline.
--
-- The problem it solves
-- After `CAST(passenger_count AS INT)` runs, a value that failed to
-- convert and a value that was never there are **both NULL**. Nothing in
-- the Bronze table distinguishes them.
-- `read_files` normally adds `_rescued_data` to catch exactly this, but it
-- only appears when the schema is *inferred*. A declared schema skips
-- inference, so a value that does not fit has nowhere to go.


INSERT INTO nyc_quality.dq_results (
    run_id, run_ts, layer, table_name, check_category, check_name,
    failed_rows, total_rows, failed_pct, threshold_pct, status,
    batch_month, min_failed_rows, warn_pct
)
WITH raw AS (
    -- The landed files, original column names, original Parquet types.
    -- All loader-cast columns are checked, not just likely failures.
    -- Silent cast-to-NULL issues are unpredictable, and checking all columns adds little cost in the same file scan.
    SELECT
        COUNT(*)                                                       AS n_rows,
        SUM(CASE WHEN VendorID              IS NULL THEN 1 ELSE 0 END) AS n_vendor_id,
        SUM(CASE WHEN PULocationID          IS NULL THEN 1 ELSE 0 END) AS n_pu_location_id,
        SUM(CASE WHEN DOLocationID          IS NULL THEN 1 ELSE 0 END) AS n_do_location_id,
        SUM(CASE WHEN RatecodeID            IS NULL THEN 1 ELSE 0 END) AS n_ratecode_id,
        SUM(CASE WHEN payment_type          IS NULL THEN 1 ELSE 0 END) AS n_payment_type,
        SUM(CASE WHEN trip_type             IS NULL THEN 1 ELSE 0 END) AS n_trip_type,
        SUM(CASE WHEN passenger_count       IS NULL THEN 1 ELSE 0 END) AS n_passenger_count,
        SUM(CASE WHEN store_and_fwd_flag    IS NULL THEN 1 ELSE 0 END) AS n_store_and_fwd_flag,
        SUM(CASE WHEN lpep_pickup_datetime  IS NULL THEN 1 ELSE 0 END) AS n_pickup_datetime,
        SUM(CASE WHEN lpep_dropoff_datetime IS NULL THEN 1 ELSE 0 END) AS n_dropoff_datetime,
        SUM(CASE WHEN trip_distance         IS NULL THEN 1 ELSE 0 END) AS n_trip_distance,
        SUM(CASE WHEN fare_amount           IS NULL THEN 1 ELSE 0 END) AS n_fare_amount,
        SUM(CASE WHEN total_amount          IS NULL THEN 1 ELSE 0 END) AS n_total_amount,
        SUM(CASE WHEN extra                 IS NULL THEN 1 ELSE 0 END) AS n_extra,
        SUM(CASE WHEN mta_tax               IS NULL THEN 1 ELSE 0 END) AS n_mta_tax,
        SUM(CASE WHEN tip_amount            IS NULL THEN 1 ELSE 0 END) AS n_tip_amount,
        SUM(CASE WHEN tolls_amount          IS NULL THEN 1 ELSE 0 END) AS n_tolls_amount,
        SUM(CASE WHEN improvement_surcharge IS NULL THEN 1 ELSE 0 END) AS n_improvement_surcharge,
        SUM(CASE WHEN congestion_surcharge  IS NULL THEN 1 ELSE 0 END) AS n_congestion_surcharge
    -- The folder path is a literal and the month is a predicate, rather than
    -- the month being spliced into the path. Whether a SQL variable
    -- constant-folds inside read_files is runtime-dependent; a WHERE clause
    -- on _metadata.file_name is not, and Spark prunes to the one file.
    FROM read_files('/Volumes/workspace/default/ftw-b12-de/groups/week-08/group-d/green-taxi/', format => 'parquet')
    WHERE _metadata.file_name = concat('green_tripdata_', v_batch_month, '.parquet')
),
files AS (
    -- Name-level reconciliation, not a count. Scoped to this month's file:
    -- v1 read the whole landing folder and asked whether every file in it
    -- was loaded, which is only ever true on the final month. In the March
    -- run it reported 4 of 5 files unloaded. April and May were not
    -- missing, they had not been asked for yet.
    SELECT
        (SELECT COUNT(*) FROM (
            SELECT DISTINCT _metadata.file_name AS f
            FROM   read_files('/Volumes/workspace/default/ftw-b12-de/groups/week-08/group-d/green-taxi/', format => 'parquet')
            WHERE  _metadata.file_name = concat('green_tripdata_', v_batch_month, '.parquet')
            EXCEPT
            SELECT DISTINCT element_at(split(source_file, '/'), -1)
            FROM   vw_batch_green_taxi
        ))                                                             AS n_unloaded,
        (SELECT COUNT(DISTINCT _metadata.file_name)
         FROM read_files('/Volumes/workspace/default/ftw-b12-de/groups/week-08/group-d/green-taxi/', format => 'parquet')
         WHERE _metadata.file_name = concat('green_tripdata_', v_batch_month, '.parquet'))
                                                                       AS n_landed
),
loaded AS (
    -- Same list, this pipeline's column names.
    SELECT
        COUNT(*)                                                       AS n_rows,
        SUM(CASE WHEN vendor_id             IS NULL THEN 1 ELSE 0 END) AS n_vendor_id,
        SUM(CASE WHEN pu_location_id        IS NULL THEN 1 ELSE 0 END) AS n_pu_location_id,
        SUM(CASE WHEN do_location_id        IS NULL THEN 1 ELSE 0 END) AS n_do_location_id,
        SUM(CASE WHEN ratecode_id           IS NULL THEN 1 ELSE 0 END) AS n_ratecode_id,
        SUM(CASE WHEN payment_type          IS NULL THEN 1 ELSE 0 END) AS n_payment_type,
        SUM(CASE WHEN trip_type             IS NULL THEN 1 ELSE 0 END) AS n_trip_type,
        SUM(CASE WHEN passenger_count       IS NULL THEN 1 ELSE 0 END) AS n_passenger_count,
        SUM(CASE WHEN store_and_fwd_flag    IS NULL THEN 1 ELSE 0 END) AS n_store_and_fwd_flag,
        SUM(CASE WHEN lpep_pickup_datetime  IS NULL THEN 1 ELSE 0 END) AS n_pickup_datetime,
        SUM(CASE WHEN lpep_dropoff_datetime IS NULL THEN 1 ELSE 0 END) AS n_dropoff_datetime,
        SUM(CASE WHEN trip_distance         IS NULL THEN 1 ELSE 0 END) AS n_trip_distance,
        SUM(CASE WHEN fare_amount           IS NULL THEN 1 ELSE 0 END) AS n_fare_amount,
        SUM(CASE WHEN total_amount          IS NULL THEN 1 ELSE 0 END) AS n_total_amount,
        SUM(CASE WHEN extra                 IS NULL THEN 1 ELSE 0 END) AS n_extra,
        SUM(CASE WHEN mta_tax               IS NULL THEN 1 ELSE 0 END) AS n_mta_tax,
        SUM(CASE WHEN tip_amount            IS NULL THEN 1 ELSE 0 END) AS n_tip_amount,
        SUM(CASE WHEN tolls_amount          IS NULL THEN 1 ELSE 0 END) AS n_tolls_amount,
        SUM(CASE WHEN improvement_surcharge IS NULL THEN 1 ELSE 0 END) AS n_improvement_surcharge,
        SUM(CASE WHEN congestion_surcharge  IS NULL THEN 1 ELSE 0 END) AS n_congestion_surcharge
    FROM vw_batch_green_taxi
),
fidelity AS (
    -- GREATEST(..., 0) because only an INCREASE means loss. Fewer nulls than
    -- the source would be a stranger problem, caught by the row-count check.
    SELECT 'business' AS check_category, 'row_count_matches_source' AS check_name,
           v_strict_pct AS threshold_pct,
           ABS((SELECT n_rows FROM loaded) - (SELECT n_rows FROM raw)) AS failed_rows,
           (SELECT n_rows FROM raw)                                    AS total_rows
    UNION ALL SELECT 'business', 'every_landed_file_is_loaded', v_strict_pct,
           (SELECT n_unloaded FROM files), (SELECT n_landed FROM files)
    UNION ALL SELECT 'completeness', 'no_nulls_added_vendor_id', v_strict_pct,
        GREATEST((SELECT n_vendor_id FROM loaded) - (SELECT n_vendor_id FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_pu_location_id', v_strict_pct,
        GREATEST((SELECT n_pu_location_id FROM loaded) - (SELECT n_pu_location_id FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_do_location_id', v_strict_pct,
        GREATEST((SELECT n_do_location_id FROM loaded) - (SELECT n_do_location_id FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_ratecode_id', v_strict_pct,
        GREATEST((SELECT n_ratecode_id FROM loaded) - (SELECT n_ratecode_id FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_payment_type', v_strict_pct,
        GREATEST((SELECT n_payment_type FROM loaded) - (SELECT n_payment_type FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_trip_type', v_strict_pct,
        GREATEST((SELECT n_trip_type FROM loaded) - (SELECT n_trip_type FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_passenger_count', v_strict_pct,
        GREATEST((SELECT n_passenger_count FROM loaded) - (SELECT n_passenger_count FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_store_and_fwd_flag', v_strict_pct,
        GREATEST((SELECT n_store_and_fwd_flag FROM loaded) - (SELECT n_store_and_fwd_flag FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_pickup_datetime', v_strict_pct,
        GREATEST((SELECT n_pickup_datetime FROM loaded) - (SELECT n_pickup_datetime FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_dropoff_datetime', v_strict_pct,
        GREATEST((SELECT n_dropoff_datetime FROM loaded) - (SELECT n_dropoff_datetime FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_trip_distance', v_strict_pct,
        GREATEST((SELECT n_trip_distance FROM loaded) - (SELECT n_trip_distance FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_fare_amount', v_strict_pct,
        GREATEST((SELECT n_fare_amount FROM loaded) - (SELECT n_fare_amount FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_total_amount', v_strict_pct,
        GREATEST((SELECT n_total_amount FROM loaded) - (SELECT n_total_amount FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_extra', v_strict_pct,
        GREATEST((SELECT n_extra FROM loaded) - (SELECT n_extra FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_mta_tax', v_strict_pct,
        GREATEST((SELECT n_mta_tax FROM loaded) - (SELECT n_mta_tax FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_tip_amount', v_strict_pct,
        GREATEST((SELECT n_tip_amount FROM loaded) - (SELECT n_tip_amount FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_tolls_amount', v_strict_pct,
        GREATEST((SELECT n_tolls_amount FROM loaded) - (SELECT n_tolls_amount FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_improvement_surcharge', v_strict_pct,
        GREATEST((SELECT n_improvement_surcharge FROM loaded) - (SELECT n_improvement_surcharge FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_congestion_surcharge', v_strict_pct,
        GREATEST((SELECT n_congestion_surcharge FROM loaded) - (SELECT n_congestion_surcharge FROM raw), 0), (SELECT n_rows FROM raw)
)

SELECT
    v_run_id, v_run_ts, 'bronze', 'green_taxi',
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    threshold_pct,
    -- Status, in the order the tests are applied.
    --   SKIP  the batch had no rows. Only table_not_empty reports that;
    --         the other checks stay quiet instead of each inventing its own
    --         failure out of a NULL aggregate. The empty weather table
    --         produced 35 FAIL rows in the first March run, all of them
    --         restating one fact.
    --   PASS  nothing broke the rule.
    --   WARN  broken, but under the row floor OR under the rate threshold.
    --   FAIL  over both.
    CASE WHEN total_rows = 0 AND check_name <> 'table_not_empty'           THEN 'SKIP'
         WHEN failed_rows = 0                                              THEN 'PASS'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= warn_pct      THEN 'PASS'
         WHEN failed_rows <= min_failed_rows                               THEN 'WARN'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= threshold_pct THEN 'WARN'
         ELSE 'FAIL' END,
    v_batch_month,
    min_failed_rows,
    warn_pct
FROM (
    SELECT c.* EXCEPT (threshold_pct),

           -- ## Two kinds of finding in one section
           --
           -- **Did the load happen at all** -- `table_not_empty`,
           -- `row_count_matches_source`, `every_landed_file_is_loaded`. These
           -- are counted in rows and files, and there is no tolerable amount
           -- of either. One row short means the MERGE dropped something; one
           -- file short means a month never arrived. Both stay at 0% with no
           -- floor: one is a FAIL, and the pipeline stops.
           --
           -- **Did a CAST silently null a value** -- the 19
           -- `no_nulls_added_*`. Same evidence, different question. A handful
           -- of unparseable values in 44,000 rows is the source being the
           -- source; a tenth of a column turning to NULL is the declared type
           -- being wrong. One threshold cannot tell those apart, so these get
           -- the pair: 5% warns, 10% fails.
           --
           -- They stay on the blocking list. Over 10% of a column silently
           -- nulled is not a defect to record and move past -- the values are
           -- gone and nothing downstream can rebuild them.
           CASE WHEN c.check_name LIKE 'no_nulls_added_%'
                THEN v_cast_fail_pct ELSE c.threshold_pct END AS threshold_pct,
           CASE WHEN c.check_name LIKE 'no_nulls_added_%'
                THEN v_cast_warn_pct ELSE 0.0 END             AS warn_pct,
           0 AS min_failed_rows
    FROM fidelity c

);
-- If `row_count_matches_source` fails, `COPY INTO` either skipped a file it
-- had already loaded or loaded one twice. If `every_landed_file_is_loaded`
-- fails, a specific file landed and was never picked up — the `EXCEPT` in
-- the `files` CTE will name it if you run it on its own. If a
-- `no_nulls_added_*` check fails, that column's `CAST` is rejecting real
-- values — swap it for `try_cast` and add a flag, or widen the declared
-- type. All three are loader bugs, not data problems, and belong with
-- whoever owns the load.

-- # 5b. Load fidelity — weather
-- green_taxi had this section and weather did not, which left the strictest
-- rule in the notebook applying to one source out of two. If the weather
-- MERGE dropped rows or a monthly CSV was never picked up, nothing noticed
-- until the numbers looked odd in Gold.
--
-- ## Why the denominator is distinct dates, not rows
-- The weather MERGE deduplicates inside the CSV before it inserts:
--
--     QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY ...) = 1
--
-- So a CSV with 744 rows and 2 repeated hours legitimately loads 742. A
-- plain row-count comparison would call that a lost row every single run and
-- the check would be switched off inside a month. Counting DISTINCT dates in
-- the source is the number that should match, and the rows the MERGE
-- collapsed are reported separately as an advisory.
--
-- ## One case this will flag that is not a bug
--
-- The MERGE matches on date alone, with no month in the condition. If a
-- date in this file already arrived with an earlier file -- possible at a
-- month boundary once the timestamps are converted out of UTC -- the row is
-- not inserted, and it is not under this file's source_file_month either.
-- That reads as one missing row. Rare, and worth being told about.

INSERT INTO nyc_quality.dq_results (
    run_id, run_ts, layer, table_name, check_category, check_name,
    failed_rows, total_rows, failed_pct, threshold_pct, status,
    batch_month, min_failed_rows, warn_pct
)
WITH raw AS (
    -- The landed CSV for this month, read without the loader's FAILFAST so
    -- a malformed row is counted here rather than taking the cell down.
    SELECT COUNT(*)                  AS n_rows,
           COUNT(DISTINCT `date`)    AS n_keys
    FROM   read_files('/Volumes/workspace/default/ftw-b12-de/groups/week-08/group-d/weather/',
                      format => 'csv', header => true)
    WHERE  _metadata.file_name = v_weather_file
),
landed AS (
    -- Name-level reconciliation: did this month's file land, and is it in
    -- Bronze under its own name? Reads the folder rather than the file, so a
    -- missing file is reported instead of raising.
    SELECT
        (SELECT COUNT(*) FROM (
            SELECT DISTINCT _metadata.file_name AS f
            FROM   read_files('/Volumes/workspace/default/ftw-b12-de/groups/week-08/group-d/weather/',
                              format => 'csv', header => true)
            WHERE  _metadata.file_name = v_weather_file
            EXCEPT
            SELECT DISTINCT source_file_month
            FROM   nyc_bronze.weather
        ))                                                             AS n_unloaded,
        (SELECT COUNT(DISTINCT _metadata.file_name)
         FROM   read_files('/Volumes/workspace/default/ftw-b12-de/groups/week-08/group-d/weather/',
                           format => 'csv', header => true)
         WHERE  _metadata.file_name = v_weather_file)                   AS n_landed
),
loaded AS (
    SELECT COUNT(*) AS n_rows FROM vw_batch_weather
),
fidelity AS (
    SELECT 'business' AS check_category, 'row_count_matches_source' AS check_name,
           v_strict_pct AS threshold_pct,
           ABS((SELECT n_rows FROM loaded) - (SELECT n_keys FROM raw)) AS failed_rows,
           (SELECT n_keys FROM raw)                                    AS total_rows
    UNION ALL SELECT 'business', 'every_landed_file_is_loaded', v_strict_pct,
           (SELECT n_unloaded FROM landed), (SELECT n_landed FROM landed)
    -- Advisory: how many duplicate hours the MERGE collapsed on the way in.
    -- Expected to be 0 or a handful; a number that jumps is the source
    -- repeating itself, which is worth knowing before Silver aggregates it.
    UNION ALL SELECT 'uniqueness', 'source_rows_deduplicated', v_advisory_pct,
           GREATEST((SELECT n_rows FROM raw) - (SELECT n_keys FROM raw), 0),
           (SELECT n_rows FROM raw)
)
SELECT
    v_run_id, v_run_ts, 'bronze', 'weather',
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    threshold_pct,
    CASE WHEN total_rows = 0 AND check_name <> 'table_not_empty'           THEN 'SKIP'
         WHEN failed_rows = 0                                              THEN 'PASS'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= warn_pct      THEN 'PASS'
         WHEN failed_rows <= min_failed_rows                               THEN 'WARN'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= threshold_pct THEN 'WARN'
         ELSE 'FAIL' END,
    v_batch_month,
    min_failed_rows,
    warn_pct
FROM (
    SELECT c.*,
           -- Same reasoning as section 5: this measures the loader, and a
           -- loader is exact or broken. No floor, no warn band.
           0 AS min_failed_rows,
           0.0 AS warn_pct
    FROM fidelity c
);


-- 6. Audit log
INSERT INTO nyc_quality.dq_run_log (
    run_id, run_ts, layer, tables_checked, checks_run,
    checks_passed, checks_warned, checks_failed, overall_status, finished_at,
    batch_month, checks_skipped
)
SELECT
    v_run_id,
    v_run_ts,
    'bronze',
    COUNT(DISTINCT d.table_name)                                 AS tables_checked,
    COUNT(*)                                                     AS checks_run,
    SUM(CASE WHEN d.status = 'PASS' THEN 1 ELSE 0 END)           AS checks_passed,
    SUM(CASE WHEN d.status = 'WARN' THEN 1 ELSE 0 END)           AS checks_warned,
    SUM(CASE WHEN d.status = 'FAIL' THEN 1 ELSE 0 END)           AS checks_failed,
    -- FAIL means THIS LAYER IS NOT USABLE -- a blocking check failed, and the
    -- gate below raises on the same condition. A non-blocking failure is a
    -- recorded defect, not a reason to stop, so it lands on WARN: saying FAIL
    -- for both made the column disagree with the gate that read the same rows.
    --
    -- SKIP is excluded from the WARN test. A skipped check did not find
    -- anything wrong, it did not run, and counting it as a warning would put
    -- an empty batch permanently on WARN for reasons unrelated to quality.
    --
    -- `b.check_name IS NOT NULL` is the LEFT JOIN's way of saying "this check
    -- is on the blocking list". It replaces a correlated EXISTS that sat
    -- inside the aggregate, which Spark will not resolve there.
    CASE WHEN SUM(CASE WHEN d.status = 'FAIL' AND b.check_name IS NOT NULL
                       THEN 1 ELSE 0 END) > 0                    THEN 'FAIL'
         WHEN SUM(CASE WHEN d.status NOT IN ('PASS', 'SKIP')
                       THEN 1 ELSE 0 END) > 0                    THEN 'WARN'
         ELSE 'PASS' END                                         AS overall_status,
    current_timestamp()                                          AS finished_at,
    v_batch_month                                                AS batch_month,
    SUM(CASE WHEN d.status = 'SKIP' THEN 1 ELSE 0 END)           AS checks_skipped
-- Joined, not a correlated EXISTS. Spark does not support a correlated
-- subquery in every position -- inside an aggregate least of all -- and the
-- join says the same thing with no ambiguity about which alias is in scope.
--
-- Read by batch, not by run_id: one authoritative row per month, so a re-run
-- of the same month replaces its verdict instead of adding a second.
FROM       nyc_quality.dq_results d
JOIN       vw_batch_scope s
       ON  s.table_name = d.table_name AND s.batch_month = d.batch_month
-- LEFT, so a check that is not blocking still counts toward the totals.
-- vw_bronze_blocking_checks holds at most one row per (table, check), so this
-- cannot fan out.
LEFT  JOIN vw_bronze_blocking_checks b
       ON  b.table_name = d.table_name AND b.check_name = d.check_name
WHERE  d.layer = 'bronze' AND d.check_category <> 'gate';

-- 7. Results — this batch only
-- Every query in this section filters on v_run_id, which is one month. The
-- table underneath still holds every month that has ever been checked; see
-- section 9 for the cross-month view.

SELECT * FROM nyc_quality.dq_run_log
WHERE  layer = 'bronze' AND batch_month = v_batch_month;

SELECT batch_month, table_name, status, COUNT(*) AS checks
FROM   nyc_quality.dq_results
-- USING keeps every column reference below unqualified: the join
-- columns are merged into one, so nothing becomes ambiguous.
JOIN   vw_batch_scope USING (table_name, batch_month)
WHERE  layer = 'bronze' AND check_category <> 'gate'

GROUP  BY batch_month, table_name, status
ORDER  BY table_name, status;
-- Everything that is not a clean pass, worst first. SKIP is shown last and
-- separately: it is an absence of evidence, not a defect.
SELECT table_name, check_category, check_name,
       failed_rows, total_rows, failed_pct, threshold_pct, min_failed_rows, status
FROM   nyc_quality.dq_results
-- USING keeps every column reference below unqualified: the join
-- columns are merged into one, so nothing becomes ambiguous.
JOIN   vw_batch_scope USING (table_name, batch_month)
WHERE  layer = 'bronze' AND check_category <> 'gate' AND status <> 'PASS'

ORDER  BY CASE status WHEN 'FAIL' THEN 0 WHEN 'WARN' THEN 1 ELSE 2 END,
          failed_pct DESC;
-- Clean PASSes are silent; these are the ones that passed with something in
-- them. Only the cast checks can land here, because everything else has
-- warn_pct = 0. Read it the way you would read a WARN: fine this month, and
-- a number that climbs month over month is the declared type going wrong.
SELECT table_name, check_name, failed_rows, total_rows, failed_pct, warn_pct
FROM   nyc_quality.dq_results
-- USING keeps every column reference below unqualified: the join
-- columns are merged into one, so nothing becomes ambiguous.
JOIN   vw_batch_scope USING (table_name, batch_month)
WHERE  layer = 'bronze' AND status = 'PASS' AND failed_rows > 0

ORDER  BY failed_pct DESC;
-- The checks that the row floor -- not the percentage -- kept out of FAIL.
-- Worth reading every run: these are real defects held below the stop line,
-- and a count that climbs month over month is the floor going stale.
SELECT table_name, check_name, failed_rows, total_rows, failed_pct,
       threshold_pct, min_failed_rows
FROM   nyc_quality.dq_results
-- USING keeps every column reference below unqualified: the join
-- columns are merged into one, so nothing becomes ambiguous.
JOIN   vw_batch_scope USING (table_name, batch_month)
WHERE  layer = 'bronze' AND status = 'WARN'

  AND  failed_rows > 0
  AND  failed_rows <= min_failed_rows
  AND  failed_pct > threshold_pct
ORDER  BY failed_rows DESC;

-- 8. Gate — per table
-- A table is STOPPED when either is true of it:
-- | Trigger | Meaning |
-- |---|---|
-- | a blocking check for that table FAILED | something broke that Silver cannot repair |
-- | `v_max_total_failures` or more FAILs on that table | no single thing is fatal, but the batch is broadly wrong |
--
-- The bulk trigger is counted per table now, not across the run. Counting it
-- across all three meant three tables each two checks short of trouble added
-- up to a stop that no single source deserved.
--
-- ### The notebook only raises for a required table
-- Every table gets a verdict; only `v_required_tables` can halt the run.
-- An empty weather batch therefore stops weather -> Silver and lets the trip
-- path continue, instead of failing the whole job the way it did in March.
--
-- ### How Silver reads the verdict
--
-- The verdicts are written back into `dq_results` as one synthetic check per
-- table, `batch_cleared_for_silver`, under check_category `gate`. No new
-- table: Silver asks the same place everything else is recorded.
--
--     SELECT status
--     FROM   nyc_mobility.nyc_quality.dq_results
--     WHERE  layer = 'bronze'
--       AND  batch_month = '<the month>'
--       AND  table_name  = 'green_taxi'
--       AND  check_name  = 'batch_cleared_for_silver';
--
-- PASS means go. FAIL means that source is not cleared for this month.
-- They are written after section 6, so they do not inflate the audit counts.

-- TEMPORARY -- report-only mode.

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
-- This is how that was found the first time: four names -- timestamp_not_null,
-- timestamp_parses, one_row_per_hour, no_rescued_data -- had been carried over
-- from the personal pipeline, whose weather column and inferred schema are
-- different. The gate claimed to guard a _rescued_data column this table does
-- not have.
--
-- Pairing by table makes the check sharper: a name that exists on some other
-- table no longer covers for a name missing on this one. Anything returned
-- below is a guarantee the gate is making and the notebook never produces.
SELECT b.table_name, b.check_name AS blocking_pair_never_produced
FROM   vw_bronze_blocking_checks b
LEFT   JOIN (SELECT DISTINCT table_name, check_name
             FROM   nyc_quality.dq_results
             JOIN   vw_batch_scope USING (table_name, batch_month)
             WHERE  layer = 'bronze') r
       ON  b.table_name = r.table_name AND b.check_name = r.check_name
WHERE  r.check_name IS NULL;


-- ### Per-table verdicts
CREATE OR REPLACE TEMPORARY VIEW vw_bronze_gate AS
-- Joined, not a correlated EXISTS. Spark does not support a correlated
-- subquery in every position -- inside an aggregate least of all -- and the
-- join says the same thing with no ambiguity about which alias is in scope.
WITH scored AS (
    SELECT d.table_name,
           s.batch_month,
           SUM(CASE WHEN d.status = 'FAIL' THEN 1 ELSE 0 END)  AS failures,
           SUM(CASE WHEN d.status = 'FAIL' AND b.check_name IS NOT NULL
                    THEN 1 ELSE 0 END)                         AS blocking_failures,
           COALESCE(concat_ws(', ', collect_list(
               CASE WHEN d.status = 'FAIL' AND b.check_name IS NOT NULL
                    THEN d.check_name END)), 'none')           AS blocking_names,
           SUM(CASE WHEN d.status = 'SKIP' THEN 1 ELSE 0 END)  AS skipped
    -- The batch, not this execution -- so the gate reads one coherent set of
    -- results rather than a mixture of two runs. The scope view is what makes
    -- taxi_zones land on its own key while the other two use the month.
    FROM       nyc_quality.dq_results d
    JOIN       vw_batch_scope s
           ON  s.table_name = d.table_name AND s.batch_month = d.batch_month
    LEFT  JOIN vw_bronze_blocking_checks b
           ON  b.table_name = d.table_name AND b.check_name = d.check_name
    WHERE  d.layer = 'bronze' AND d.check_category <> 'gate'
    GROUP  BY d.table_name, s.batch_month
)
SELECT table_name,
       batch_month,
       failures, blocking_failures, blocking_names, skipped,
       CASE WHEN blocking_failures > 0
              OR failures >= v_max_total_failures THEN 'STOP' ELSE 'GO' END AS verdict,
       array_contains(v_required_tables, table_name)                        AS is_required
FROM   scored;

SELECT table_name, batch_month, verdict, is_required,
       blocking_failures, failures, skipped, blocking_names
FROM   vw_bronze_gate
ORDER  BY CASE verdict WHEN 'STOP' THEN 0 ELSE 1 END, table_name;


-- ### Record the verdicts where Silver can read them
INSERT INTO nyc_quality.dq_results (
    run_id, run_ts, layer, table_name, check_category, check_name,
    failed_rows, total_rows, failed_pct, threshold_pct, status,
    batch_month, min_failed_rows, warn_pct
)
SELECT v_run_id, v_run_ts, 'bronze', table_name,
       'gate', 'batch_cleared_for_silver',
       blocking_failures, 1,
       CASE WHEN verdict = 'STOP' THEN 100.0 ELSE 0.0 END,
       0.0,
       CASE WHEN verdict = 'STOP' THEN 'FAIL' ELSE 'PASS' END,
       batch_month, 0, 0.0
FROM   vw_bronze_gate;


-- ### Raise, but only for a required table
DECLARE OR REPLACE VARIABLE v_stopped_required STRING;
DECLARE OR REPLACE VARIABLE v_stopped_optional STRING;

SET VAR v_stopped_required = (
    SELECT COALESCE(concat_ws(', ', collect_list(table_name)), '')
    FROM   vw_bronze_gate WHERE verdict = 'STOP' AND is_required);

SET VAR v_stopped_optional = (
    SELECT COALESCE(concat_ws(', ', collect_list(table_name)), '')
    FROM   vw_bronze_gate WHERE verdict = 'STOP' AND NOT is_required);

SELECT v_batch_month                                    AS batch_month,
       COALESCE(NULLIF(v_stopped_required, ''), 'none') AS stopped_required,
       COALESCE(NULLIF(v_stopped_optional, ''), 'none') AS stopped_optional,
       CASE WHEN v_stopped_required <> ''
              THEN 'will stop the run'
            WHEN v_stopped_optional <> ''
              THEN 'will continue; the named sources are held back from Silver'
            ELSE 'will continue' END                    AS verdict;

SELECT CASE
    WHEN v_gate_enforce AND v_stopped_required <> ''
      THEN raise_error(CONCAT('Bronze DQ gate FAILED for ', v_batch_month,
                              ': required table(s) stopped -- ', v_stopped_required,
                              '. See nyc_quality.dq_results for run ', v_run_id))
    WHEN v_stopped_optional <> ''
      THEN CONCAT('Bronze DQ gate PASSED for ', v_batch_month,
                  '; held back from Silver: ', v_stopped_optional)
    ELSE CONCAT('Bronze DQ gate PASSED for ', v_batch_month,
                ' -- every table cleared')
END AS gate;

-- A pass with non-blocking failures recorded is a normal, honest outcome.
-- Read them here and decide whether each is a threshold to measure or a
-- defect to fix:

SELECT table_name, check_category, check_name,
       failed_rows, total_rows, failed_pct, threshold_pct, min_failed_rows
FROM   nyc_quality.dq_results
-- USING keeps every column reference below unqualified: the join
-- columns are merged into one, so nothing becomes ambiguous.
JOIN   vw_batch_scope USING (table_name, batch_month)
WHERE  layer = 'bronze' AND status = 'FAIL' AND check_category <> 'gate'

ORDER  BY failed_pct DESC;

-- 9. Afterwards
--
-- Sections 7 and 8 are the batch. This section is the history, and it needs
-- no extra table: dq_results already holds every month, and batch_month is
-- what turns it into a per-month record. The two views were created in the
-- setup notebook.

-- The batch that ran most recently, whichever month that was.
SELECT batch_month, table_name, check_category, check_name,
       failed_rows, total_rows, failed_pct, threshold_pct, status
FROM   nyc_quality.vw_current_batch_dq
WHERE  layer = 'bronze'
ORDER  BY CASE status WHEN 'FAIL' THEN 0 WHEN 'WARN' THEN 1 WHEN 'SKIP' THEN 2 ELSE 3 END,
          failed_pct DESC;

-- One row per month, the headline. This is the per-month dashboard.
SELECT batch_month,
       MAX(run_ts)                                          AS last_checked,
       COUNT(*)                                             AS checks,
       SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END)     AS passed,
       SUM(CASE WHEN status = 'WARN' THEN 1 ELSE 0 END)     AS warned,
       SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END)     AS failed,
       SUM(CASE WHEN status = 'SKIP' THEN 1 ELSE 0 END)     AS skipped
FROM   nyc_quality.vw_dq_by_month
WHERE  layer = 'bronze' AND batch_month NOT LIKE 'static-%'
GROUP  BY batch_month
ORDER  BY batch_month;

-- The reference lookup, one row per VERSION of the file rather than per
-- month. More than one row here means the lookup was replaced at some point,
-- and the older rows are what it used to look like.
SELECT batch_month AS zones_version,
       MAX(run_ts) AS last_checked,
       SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS passed,
       SUM(CASE WHEN status <> 'PASS' THEN 1 ELSE 0 END) AS not_passed
FROM   nyc_quality.vw_dq_by_month
WHERE  layer IN ('preload','bronze') AND batch_month LIKE 'static-%'
GROUP  BY batch_month
ORDER  BY batch_month;

-- Has a check moved between months?
--
-- Comparing months rather than runs is the useful question now. Two runs of
-- the same month should be identical -- that is the idempotency test, and
-- LAG over run_ts answered it with a row of zeroes. A rate that climbs from
-- March to April to May is a source drifting, which is the thing worth
-- catching early.
SELECT table_name, check_name, batch_month, failed_pct, status,
       LAG(failed_pct) OVER (PARTITION BY table_name, check_name ORDER BY batch_month) AS previous_month_pct,
       ROUND(failed_pct - LAG(failed_pct) OVER (PARTITION BY table_name, check_name ORDER BY batch_month), 4) AS change
FROM   nyc_quality.vw_dq_by_month
WHERE  layer = 'bronze' AND batch_month NOT LIKE 'static-%'
ORDER  BY table_name, check_name, batch_month;

-- Checks that have never once passed, across every month loaded so far.
-- A rule that is always red is either a real standing defect or a rule that
-- does not describe this source. Either way it needs a decision, not
-- another month of being ignored.
SELECT table_name, check_name,
       COUNT(*)                                         AS months_checked,
       MAX(failed_pct)                                  AS worst_pct,
       MIN(failed_pct)                                  AS best_pct
FROM   nyc_quality.vw_dq_by_month
WHERE  layer = 'bronze'
GROUP  BY table_name, check_name
HAVING SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) = 0
   AND SUM(CASE WHEN status = 'SKIP' THEN 1 ELSE 0 END) = 0
ORDER  BY worst_pct DESC;