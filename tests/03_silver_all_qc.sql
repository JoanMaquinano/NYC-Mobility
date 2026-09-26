-- Data Quality — Silver
-- Sections
-- | § | Covers | Checks |
-- |---|---|---|
-- | 1 | `green_taxi_clean`, the classification and the promises | 27 |
-- | 2 | `taxi_zones_clean` | 13 |
-- | 3 | `weather_clean` | 25 |
-- | 4 | reconciliation with Bronze | 5 |
-- | 5 | join coverage, what Gold will actually resolve | 5 |
--
-- Then 6. Audit log · 7. Results · 8. Gate · 9. Afterwards.

SET TIME ZONE 'UTC';
USE CATALOG nyc_mobility;

-- ## Parameters — the same two every other task takes
-- | Parameter | Example | Also used by |
-- |---|---|---|
-- | `year_month`   | `2026-03`                | the Bronze and Silver MERGEs, Bronze QC |
-- | `weather_file` | `weather_march_2026.csv` | the weather MERGE, Bronze QC |
--
-- Both optional. Left blank, the month comes from whatever preload decided
-- and recorded in `dq_run_log`, so a run with no parameters still describes
-- the batch that was actually loaded and every layer agrees on which one.

DECLARE OR REPLACE VARIABLE v_run_id       STRING;
DECLARE OR REPLACE VARIABLE v_run_ts       TIMESTAMP;
DECLARE OR REPLACE VARIABLE v_batch_month  STRING;
DECLARE OR REPLACE VARIABLE v_weather_file STRING;
DECLARE OR REPLACE VARIABLE v_month_source STRING;
DECLARE OR REPLACE VARIABLE v_zones_batch  STRING;

SET VAR v_run_id = uuid();
SET VAR v_run_ts = current_timestamp();

-- ## The fallback reads SILVER, not the preload run log
-- Same correction as Bronze. Preload's `batch_month` means "the month I am
-- ABOUT to load"; Silver's means "the month I just cleaned". Inheriting one as
-- the other assumes both MERGEs ran in between, and the failure looks like a
-- data problem rather than a sequencing one: every check SKIPs against a month
-- that was never loaded, `table_not_empty` FAILs, and the gate stops a table
-- that is fine.
--
-- So the fallback asks Silver what it holds. Pass `:year_month` for any other
-- month.
SET VAR v_batch_month = COALESCE(
    NULLIF(:year_month, ''),
    (SELECT regexp_extract(source_file, '([0-9]{4}-[0-9]{2})', 1)
     FROM   nyc_silver.green_taxi_clean
     WHERE  source_file IS NOT NULL
       AND  regexp_extract(source_file, '([0-9]{4}-[0-9]{2})', 1) <> ''
     ORDER  BY silver_at DESC
     LIMIT  1),
    (SELECT batch_month
     FROM   nyc_quality.dq_run_log
     WHERE  layer = 'preload' AND batch_month IS NOT NULL
     ORDER  BY run_ts DESC
     LIMIT  1));

SET VAR v_month_source = CASE
    WHEN COALESCE(:year_month, '') <> '' THEN 'parameter'
    WHEN EXISTS (SELECT 1 FROM nyc_silver.green_taxi_clean WHERE source_file IS NOT NULL)
         THEN 'newest file in silver.green_taxi_clean'
    ELSE 'preload run log (silver is empty)' END;

SET VAR v_weather_file = COALESCE(
    NULLIF(:weather_file, ''),
    concat('weather_',
           lower(date_format(to_date(concat(v_batch_month, '-01')), 'MMMM')),
           '_', substr(v_batch_month, 1, 4), '.csv'));

-- ### And one more: a month you asked for that is not loaded
--
-- The validation above catches an unusable parameter. This catches a usable
-- one that names a month Silver does not hold -- which, before the fallback
-- was corrected, was the single most confusing failure this notebook could
-- produce: every check SKIPs, `table_not_empty` FAILs, the gate stops the run,
-- and the message names a table that is perfectly healthy.
--
-- Only applies when the month came from the PARAMETER. A month that came from
-- the fallback is by construction a month Silver holds.
SELECT CASE
    WHEN COALESCE(:year_month, '') <> ''
     AND EXISTS (SELECT 1 FROM nyc_silver.green_taxi_clean)
     AND NOT EXISTS (SELECT 1 FROM nyc_silver.green_taxi_clean
                     WHERE regexp_extract(source_file, '([0-9]{4}-[0-9]{2})', 1)
                           = v_batch_month)
      THEN raise_error(CONCAT(
             'year_month=', v_batch_month, ' has no rows in nyc_silver.green_taxi_clean. ',
             'Months actually loaded: ',
             (SELECT concat_ws(', ', array_sort(collect_set(
                  regexp_extract(source_file, '([0-9]{4}-[0-9]{2})', 1))))
              FROM nyc_silver.green_taxi_clean WHERE source_file IS NOT NULL),
             '. Run the MERGE for ', v_batch_month,
             ' first, or leave year_month blank to check the newest loaded month.'))
    ELSE CONCAT('Silver holds rows for ', v_batch_month) END AS batch_present;


-- ## taxi_zones_clean has no month in it
--
-- It is a full refresh of a 265-row lookup, so its results are keyed on the
-- lookup file's version rather than the trip month -- the same key preload
-- assigns. Inherited, not recomputed: two places deriving it is two places to
-- disagree.
SET VAR v_zones_batch = COALESCE(
    (SELECT batch_month
     FROM   nyc_quality.dq_results
     WHERE  layer = 'preload' AND table_name = 'taxi_zones'
       AND  batch_month IS NOT NULL
     ORDER  BY run_ts DESC
     LIMIT  1),
    'static-unknown');

CREATE OR REPLACE TEMPORARY VIEW vw_batch_scope AS
          SELECT 'green_taxi_clean' AS table_name, v_batch_month AS batch_month
UNION ALL SELECT 'weather_clean',                  v_batch_month
UNION ALL SELECT 'taxi_zones_clean',               v_zones_batch;


-- ## Validate before checking anything
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

SELECT v_run_id       AS run_id,
       v_run_ts       AS run_ts,
       v_batch_month  AS batch_month,
       v_month_source AS month_from,
       v_weather_file AS weather_file,
       v_zones_batch  AS zones_version;


-- ## Threshold policy — three bands, a floor, and a second line
-- | Variable | Value | Meaning |
-- |---|---|---|
-- | `v_strict_pct`   | `0.0`   | must never happen: one occurrence corrupts a join or the grain, or the check is scalar |
-- | `v_tol_pct`      | `10.0`  | the source is known to be imperfect and this much is tolerated |
-- | `v_advisory_pct` | `100.0` | reported every run, can only ever WARN, never gates |
-- | `v_cast_warn_pct` / `v_cast_fail_pct` | `5.0` / `10.0` | the pair for parse checks |
-- | `v_min_rows`     | `5`     | under this many failing rows it is a WARN whatever the rate |
--
-- ### Why most of Silver sits at 0.0 and Bronze does not
--
-- Bronze measures DATA, which arrives imperfect, so its rules are rates.
-- Most of Silver measures OUR CODE: a row labelled PASS while carrying an
-- issue is not a tolerable rate of anything, it is a `CASE` that does not
-- hold. Those checks are impossible to fail unless the transformation is
-- broken, which is exactly what makes them worth writing -- the cheapest
-- regression test there is.
--
-- The floor still applies to them. One row is a WARN, six is a FAIL, because
-- "the derivation is broken" and "one row slipped through a boundary" deserve
-- different reactions and a gate that fires on the second gets switched off.
--
-- The exceptions -- the parse checks in section 3 -- are the one place Silver
-- measures the source rather than itself, and they carry the 5/10 pair the
-- Bronze cast checks use, for the same reason.

DECLARE OR REPLACE VARIABLE v_strict_pct    DOUBLE;
DECLARE OR REPLACE VARIABLE v_tol_pct       DOUBLE;
DECLARE OR REPLACE VARIABLE v_advisory_pct  DOUBLE;
DECLARE OR REPLACE VARIABLE v_min_rows      BIGINT;
DECLARE OR REPLACE VARIABLE v_cast_warn_pct DOUBLE;
DECLARE OR REPLACE VARIABLE v_cast_fail_pct DOUBLE;

SET VAR v_strict_pct    = 0.0;
SET VAR v_tol_pct       = 10.0;
SET VAR v_advisory_pct  = 100.0;
SET VAR v_min_rows      = 5;
SET VAR v_cast_warn_pct = 5.0;
SET VAR v_cast_fail_pct = 10.0;


-- ## Batch scope

CREATE OR REPLACE TEMPORARY VIEW vw_batch_silver_taxi AS
SELECT *,
       -- The month this row's own file claims -- the same expression the
       -- cleaning notebook uses to derive its batch window, so the promises
       -- below test the rule that actually ran rather than a paraphrase of it.
       to_date(concat(regexp_extract(source_file, '([0-9]{4}-[0-9]{2})', 1),
                      '-01'))                                AS file_month_start
FROM   nyc_silver.green_taxi_clean
WHERE  regexp_extract(source_file, '([0-9]{4}-[0-9]{2})', 1) = v_batch_month;

CREATE OR REPLACE TEMPORARY VIEW vw_batch_bronze_taxi AS
SELECT *
FROM   nyc_bronze.green_taxi
WHERE  regexp_extract(source_file, '([0-9]{4}-[0-9]{2})', 1) = v_batch_month;

-- By filename, not by the month inside `date`: the weather timestamps are UTC
-- and the project is New York, so the month a row claims can differ from the
-- batch that loaded it. The file is the batch.
CREATE OR REPLACE TEMPORARY VIEW vw_batch_silver_weather AS
SELECT *
FROM   nyc_silver.weather_clean
WHERE  source_file_month = v_weather_file;

CREATE OR REPLACE TEMPORARY VIEW vw_batch_bronze_weather AS
SELECT *
FROM   nyc_bronze.weather
WHERE  source_file_month = v_weather_file;

-- What Gold actually reads, scoped to this batch. Section 5 measures join
-- coverage against this rather than the whole table, because a month whose
-- zones all resolve tells you nothing about the month being loaded.
CREATE OR REPLACE TEMPORARY VIEW vw_batch_valid_trips AS
SELECT *
FROM   vw_batch_silver_taxi
WHERE  dq_status IN ('PASS', 'WARN');

SELECT v_batch_month                                     AS batch_month,
       (SELECT COUNT(*) FROM vw_batch_bronze_taxi)       AS bronze_trips,
       (SELECT COUNT(*) FROM vw_batch_silver_taxi)       AS silver_trips,
       (SELECT COUNT(*) FROM vw_batch_valid_trips)       AS valid_trips,
       (SELECT COUNT(*) FROM vw_batch_bronze_weather)    AS bronze_weather,
       (SELECT COUNT(*) FROM vw_batch_silver_weather)    AS silver_weather,
       (SELECT COUNT(*) FROM nyc_silver.taxi_zones_clean) AS zone_rows;


-- ## The blocking list
--
-- A gate is a claim that the data is unusable, not that it is imperfect. In
-- Bronze the question was "can Silver fix it" and most row-level defects
-- could wait. Here the answer is different: Silver IS the fixing step, so a
-- rule it breaks has nothing downstream to catch it.
--
-- Two things still decide what belongs here:
--
--   1. **Is it our code or the data?** `pass_rows_carry_no_issues` can only
--      fail if the `CASE` deriving `dq_status` is wrong. Gold filters on
--      `dq_status`, so a broken derivation silently changes what Gold counts.
--      That blocks. A weather reading outside a plausible range is data.
--   2. **Is the damage confined to the row?** A duplicated merge key is not:
--      it fans out through every Gold join. A row that should have been
--      quarantined and was not is not either: it is in Gold, being counted.
--
-- ### Scoped by table
--
-- `(table, check)` pairs rather than bare names, for the reason Bronze needed
-- them: `table_not_empty` and `dq_status_populated` are emitted by more than
-- one table, and an empty weather_clean should not stop the trip path.

CREATE OR REPLACE TEMPORARY VIEW vw_silver_blocking_checks AS
SELECT * FROM VALUES
    -- green_taxi_clean -- the classification, the promises, and the grain
    ('green_taxi_clean', 'table_not_empty'),
    ('green_taxi_clean', 'dq_status_populated'),
    ('green_taxi_clean', 'qc_array_not_null'),
    ('green_taxi_clean', 'dq_status_in_domain'),
    ('green_taxi_clean', 'qc_entries_carry_severity_prefix'),
    ('green_taxi_clean', 'pass_rows_carry_no_issues'),
    ('green_taxi_clean', 'fail_rows_carry_a_fail_issue'),
    ('green_taxi_clean', 'warn_rows_carry_only_warn_issues'),
    ('green_taxi_clean', 'null_timestamps_are_quarantined'),
    ('green_taxi_clean', 'reversed_trips_are_quarantined'),
    ('green_taxi_clean', 'unresolvable_zones_are_quarantined'),
    ('green_taxi_clean', 'out_of_batch_pickups_are_quarantined'),
    ('green_taxi_clean', 'out_of_batch_dropoffs_are_quarantined'),
    ('green_taxi_clean', 'future_timestamps_are_quarantined'),
    ('green_taxi_clean', 'untraceable_rows_are_quarantined'),
    ('green_taxi_clean', 'one_row_per_merge_key'),
    ('green_taxi_clean', 'source_file_recorded'),
    ('green_taxi_clean', 'silver_at_recorded'),
    ('green_taxi_clean', 'rows_reconcile_with_bronze'),
    ('green_taxi_clean', 'revenue_preserved'),
    ('green_taxi_clean', 'every_bronze_file_present'),

    -- taxi_zones_clean -- the lookup is a primary key or it is nothing
    ('taxi_zones_clean', 'table_not_empty'),
    ('taxi_zones_clean', 'location_id_not_null'),
    ('taxi_zones_clean', 'location_id_unique'),

    -- weather_clean -- weather_hour is what Gold joins on
    ('weather_clean', 'table_not_empty'),
    ('weather_clean', 'one_row_per_hour'),
    ('weather_clean', 'dq_status_populated'),
    ('weather_clean', 'qc_array_not_null'),
    ('weather_clean', 'dq_status_in_domain'),
    ('weather_clean', 'qc_entries_carry_severity_prefix'),
    ('weather_clean', 'pass_rows_carry_no_issues'),
    ('weather_clean', 'fail_rows_carry_a_fail_issue'),
    ('weather_clean', 'warn_rows_carry_only_warn_issues'),
    -- The promise, not "the hour is never NULL". The cleaning KEEPS an
    -- unparseable hour and labels it FAIL; what must hold is that every such
    -- row carries the label, because Gold's valid view is what acts on it.
    ('weather_clean', 'null_hours_are_quarantined')
AS blocking(table_name, check_name);


-- ## Which tables the run cannot proceed without
DECLARE OR REPLACE VARIABLE v_required_tables ARRAY<STRING>;
SET VAR v_required_tables = array('green_taxi_clean');


-- ## Clear-down
--
-- On (layer, batch_month) per table, not on run_id. A fresh uuid every run
-- matches nothing, which is how the first Bronze version accumulated a new
-- row set per execution while appearing to delete one. Re-running a month
-- now replaces that month's verdict rather than adding a second.
DELETE FROM nyc_quality.dq_results
WHERE layer = 'silver' AND check_category <> 'gate'
  AND ( (table_name IN ('green_taxi_clean','weather_clean')
         AND batch_month = v_batch_month)
     OR (table_name = 'taxi_zones_clean' AND batch_month = v_zones_batch) );

-- Gate verdicts are cleared separately because they are written later, in
-- section 8, from the results the sections above produce.
DELETE FROM nyc_quality.dq_results
WHERE layer = 'silver' AND check_category = 'gate'
  AND ( (table_name IN ('green_taxi_clean','weather_clean')
         AND batch_month = v_batch_month)
     OR (table_name = 'taxi_zones_clean' AND batch_month = v_zones_batch) );

DELETE FROM nyc_quality.dq_run_log
WHERE layer = 'silver' AND batch_month = v_batch_month;


-- # 1. green_taxi_clean
--
-- ## 1a. The classification must be internally consistent
--
-- `dq_status` is derived from `qc_error_descriptions` by one `CASE`. Four
-- checks assert that the `CASE` actually holds on the rows in the table:
--
-- | Check | Fails when |
-- |---|---|
-- | `pass_rows_carry_no_issues` | a row is PASS with a non-empty array |
-- | `fail_rows_carry_a_fail_issue` | a row is FAIL with no `FAIL:` entry |
-- | `warn_rows_carry_only_warn_issues` | a row is WARN with a `FAIL:` entry, or with none at all |
-- | `qc_entries_carry_severity_prefix` | any entry starts with neither prefix |
--
-- Each is impossible if the derivation is right, which is exactly why they
-- are worth writing. A check that can only fail when the code is broken is
-- the cheapest regression test there is -- and this derivation is the thing
-- every downstream filter trusts.
--
-- The last one catches the realistic edit: someone adds a rule and writes
-- `'null passenger count'` without a prefix. `exists(..., 'FAIL:')` is false,
-- so the row silently becomes WARN. The prefix check turns a forgotten seven
-- characters into a visible failure.
--
-- ## 1b. The promises
--
-- Six conditions define FAIL in the cleaning notebook. Each gets a check
-- asserting that **no row meeting it escaped the label**. These are
-- re-derived from the data columns, not read back out of the array -- reading
-- the array would only prove the array agrees with itself.
--
-- ## 1c. What Silver changed, and whether it worked
--
-- The cleaning step clamps negatives to zero, coalesces a null ratecode to
-- 99, nulls a location id outside 1-265. `zones_within_1_to_265` and
-- `surcharges_not_negative` assert those transformations produced what they
-- promised. They are not repeats of the Bronze checks: Bronze asked whether
-- the source was clean, these ask whether the cleaning ran.

INSERT INTO nyc_quality.dq_results (
    run_id, run_ts, layer, table_name, check_category, check_name,
    failed_rows, total_rows, failed_pct, threshold_pct, status,
    batch_month, min_failed_rows, warn_pct
)
WITH
-- The merge key must be unique in the TARGET, or the MERGE updates the same
-- row more than once per run and every Gold join fans out.
grain AS (
    SELECT COALESCE(SUM(n - 1), 0) AS extra_rows
    FROM (
        SELECT COUNT(*) AS n
        FROM   vw_batch_silver_taxi
        GROUP  BY vendor_id, lpep_pickup_datetime, lpep_dropoff_datetime,
                  pu_location_id, do_location_id, trip_distance, total_amount
        HAVING COUNT(*) > 1
    )
),
s AS (
    SELECT
        COUNT(*)                                                             AS total_rows,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END                             AS t_empty,

        -- completeness of what Silver produced
        SUM(CASE WHEN dq_status             IS NULL THEN 1 ELSE 0 END)       AS c_status,
        SUM(CASE WHEN qc_error_descriptions IS NULL THEN 1 ELSE 0 END)       AS c_qc_array,
        SUM(CASE WHEN source_file           IS NULL THEN 1 ELSE 0 END)       AS c_source_file,
        SUM(CASE WHEN ingestion_time        IS NULL THEN 1 ELSE 0 END)       AS c_ingestion,
        SUM(CASE WHEN silver_at             IS NULL THEN 1 ELSE 0 END)       AS c_silver_at,

        -- validity of the classification itself
        SUM(CASE WHEN dq_status NOT IN ('PASS','WARN','FAIL')
                 THEN 1 ELSE 0 END)                                          AS v_status_domain,
        -- Every entry must carry a severity. A forgotten prefix silently
        -- downgrades a FAIL row to WARN and nothing else would notice.
        SUM(CASE WHEN qc_error_descriptions IS NOT NULL
                  AND size(filter(qc_error_descriptions,
                                  x -> NOT startswith(x, 'FAIL:')
                                   AND NOT startswith(x, 'WARN:'))) > 0
                 THEN 1 ELSE 0 END)                                          AS v_prefix,

        -- validity of what the cleaning produced
        SUM(CASE WHEN pu_location_id IS NOT NULL
                  AND pu_location_id NOT BETWEEN 1 AND 265 THEN 1 ELSE 0 END)
          + SUM(CASE WHEN do_location_id IS NOT NULL
                  AND do_location_id NOT BETWEEN 1 AND 265 THEN 1 ELSE 0 END) AS v_zone_range,
        -- The cleaning clamps these five to zero. A negative here means the
        -- clamp did not run on that row.
        SUM(CASE WHEN extra                < 0 THEN 1 ELSE 0 END)
          + SUM(CASE WHEN mta_tax              < 0 THEN 1 ELSE 0 END)
          + SUM(CASE WHEN tolls_amount         < 0 THEN 1 ELSE 0 END)
          + SUM(CASE WHEN congestion_surcharge < 0 THEN 1 ELSE 0 END)
          + SUM(CASE WHEN cbd_congestion_fee   < 0 THEN 1 ELSE 0 END)         AS v_surcharge_neg,
        -- Coalesced to 99 / 5 / 1 respectively, so a NULL means the COALESCE
        -- did not run.
        SUM(CASE WHEN ratecode_id  IS NULL THEN 1 ELSE 0 END)
          + SUM(CASE WHEN payment_type IS NULL THEN 1 ELSE 0 END)
          + SUM(CASE WHEN trip_type    IS NULL THEN 1 ELSE 0 END)             AS v_coalesced_null,
        -- Set to 1 when the source was null, zero or negative.
        SUM(CASE WHEN passenger_count IS NULL OR passenger_count <= 0
                 THEN 1 ELSE 0 END)                                          AS v_passenger_floor,
        -- Zeroed on cash payments by the cleaning step.
        SUM(CASE WHEN payment_type = 2 AND tip_amount > 0 THEN 1 ELSE 0 END) AS v_cash_tip,

        -- the derivation holds
        SUM(CASE WHEN dq_status = 'PASS' AND size(qc_error_descriptions) > 0
                 THEN 1 ELSE 0 END)                                          AS x_pass_with_issues,
        SUM(CASE WHEN dq_status = 'FAIL'
                  AND NOT exists(qc_error_descriptions, x -> startswith(x, 'FAIL:'))
                 THEN 1 ELSE 0 END)                                          AS x_fail_no_reason,
        SUM(CASE WHEN dq_status = 'WARN'
                  AND (size(qc_error_descriptions) = 0
                    OR exists(qc_error_descriptions, x -> startswith(x, 'FAIL:')))
                 THEN 1 ELSE 0 END)                                          AS x_warn_wrong,

        -- the six promises, re-derived from the columns
        SUM(CASE WHEN (lpep_pickup_datetime IS NULL OR lpep_dropoff_datetime IS NULL)
                  AND dq_status <> 'FAIL' THEN 1 ELSE 0 END)                 AS p_null_ts,
        SUM(CASE WHEN lpep_dropoff_datetime < lpep_pickup_datetime
                  AND dq_status <> 'FAIL' THEN 1 ELSE 0 END)                 AS p_reversed,
        SUM(CASE WHEN (pu_location_id IS NULL OR do_location_id IS NULL
                    OR pu_location_id NOT BETWEEN 1 AND 265
                    OR do_location_id NOT BETWEEN 1 AND 265)
                  AND dq_status <> 'FAIL' THEN 1 ELSE 0 END)                 AS p_zone,
        -- ## The batch window, re-derived the way the cleaning notebook does
        --
        -- This previously tested `pickup < TIMESTAMP'2009-01-01'`, which the
        -- cleaning notebook no longer uses. It kept PASSing, because the new
        -- rules are strictly stronger -- so it was a check quietly asserting
        -- something the code had stopped doing. That is the worst state for a
        -- check to be in: green, and testing nothing.
        --
        -- Three promises now, one per FAIL rule in the cleaning notebook.
        SUM(CASE WHEN lpep_pickup_datetime IS NOT NULL
                  AND file_month_start IS NOT NULL
                  AND DATE(lpep_pickup_datetime) NOT BETWEEN
                        add_months(file_month_start, -1)
                    AND date_sub(add_months(file_month_start, 2), 1)
                  AND dq_status <> 'FAIL' THEN 1 ELSE 0 END)                 AS p_batch_pu,
        SUM(CASE WHEN lpep_dropoff_datetime IS NOT NULL
                  AND file_month_start IS NOT NULL
                  AND DATE(lpep_dropoff_datetime) NOT BETWEEN
                        add_months(file_month_start, -1)
                    AND date_sub(add_months(file_month_start, 2), 1)
                  AND dq_status <> 'FAIL' THEN 1 ELSE 0 END)                 AS p_batch_do,
        -- The backstop, which needs no literal: no trip can be in the future.
        SUM(CASE WHEN (lpep_pickup_datetime  > current_timestamp()
                    OR lpep_dropoff_datetime > current_timestamp())
                  AND dq_status <> 'FAIL' THEN 1 ELSE 0 END)                 AS p_future,
        SUM(CASE WHEN source_file IS NULL AND dq_status <> 'FAIL'
                 THEN 1 ELSE 0 END)                                          AS p_lineage,

        -- ## The charge identity is NOT the same identity for both vendors
        --
        -- One formula across vendors 1 and 2 reported 10.63% in the first
        -- March run -- against 1.2-2.0% at source -- because vendor 1 does
        -- not use vendor 2's identity:
        --
        -- | Vendor | total_amount contains |
        -- |---|---|
        -- | 2 -- Curb | everything, including the three surcharges |
        -- | 1 -- Creative Mobile | fare + extra + mta_tax + tip + tolls. The surcharges are itemised but not added in |
        --
        -- So the combined check failed on essentially EVERY vendor-1 row
        -- (~3,700 of 4,146) and the number said nothing about data quality.
        -- Bronze and preload have split this by vendor from the start; this
        -- is Silver catching up, each measured against its own row count so a
        -- real problem in the smaller vendor is not diluted away.
        SUM(CASE WHEN vendor_id = 2
                  AND ABS(COALESCE(fare_amount, 0) + COALESCE(extra, 0)
                        + COALESCE(mta_tax, 0) + COALESCE(tip_amount, 0)
                        + COALESCE(tolls_amount, 0)
                        + COALESCE(improvement_surcharge, 0)
                        + COALESCE(congestion_surcharge, 0)
                        + COALESCE(cbd_congestion_fee, 0)
                        - COALESCE(total_amount, 0)) > 0.01
                 THEN 1 ELSE 0 END)                                          AS b_residual_v2,
        SUM(CASE WHEN vendor_id = 2 THEN 1 ELSE 0 END)                       AS n_v2,
        SUM(CASE WHEN vendor_id = 1
                  AND ABS(COALESCE(fare_amount, 0) + COALESCE(extra, 0)
                        + COALESCE(mta_tax, 0) + COALESCE(tip_amount, 0)
                        + COALESCE(tolls_amount, 0)
                        - COALESCE(total_amount, 0)) > 0.01
                 THEN 1 ELSE 0 END)                                          AS b_residual_v1,
        SUM(CASE WHEN vendor_id = 1 THEN 1 ELSE 0 END)                       AS n_v1,

        SUM(CASE WHEN dq_status = 'FAIL' THEN 1 ELSE 0 END)                  AS b_quarantined,
        SUM(CASE WHEN dq_status = 'WARN' THEN 1 ELSE 0 END)                  AS b_warned
    FROM vw_batch_silver_taxi
),
checks AS (
    SELECT 'completeness' AS check_category, 'table_not_empty' AS check_name,
           v_strict_pct AS threshold_pct, t_empty AS failed_rows, 1 AS total_rows FROM s
    UNION ALL SELECT 'completeness', 'dq_status_populated',     v_strict_pct, c_status,      total_rows FROM s
    UNION ALL SELECT 'completeness', 'qc_array_not_null',       v_strict_pct, c_qc_array,    total_rows FROM s
    UNION ALL SELECT 'completeness', 'source_file_recorded',    v_strict_pct, c_source_file, total_rows FROM s
    UNION ALL SELECT 'completeness', 'ingestion_time_recorded', v_tol_pct,    c_ingestion,   total_rows FROM s
    UNION ALL SELECT 'completeness', 'silver_at_recorded',      v_strict_pct, c_silver_at,   total_rows FROM s

    UNION ALL SELECT 'uniqueness',   'one_row_per_merge_key',   v_strict_pct,
                     (SELECT extra_rows FROM grain), total_rows FROM s

    UNION ALL SELECT 'validity',     'dq_status_in_domain',              v_strict_pct, v_status_domain,   total_rows FROM s
    UNION ALL SELECT 'validity',     'qc_entries_carry_severity_prefix', v_strict_pct, v_prefix,          total_rows FROM s
    UNION ALL SELECT 'validity',     'zones_within_1_to_265',            v_strict_pct, v_zone_range,      total_rows FROM s
    UNION ALL SELECT 'validity',     'surcharges_not_negative',          v_strict_pct, v_surcharge_neg,   total_rows FROM s
    UNION ALL SELECT 'validity',     'coalesced_codes_not_null',         v_strict_pct, v_coalesced_null,  total_rows FROM s
    UNION ALL SELECT 'validity',     'passenger_count_at_least_one',     v_strict_pct, v_passenger_floor, total_rows FROM s
    UNION ALL SELECT 'validity',     'cash_trips_carry_no_tip',          v_strict_pct, v_cash_tip,        total_rows FROM s

    UNION ALL SELECT 'consistency',  'pass_rows_carry_no_issues',        v_strict_pct, x_pass_with_issues, total_rows FROM s
    UNION ALL SELECT 'consistency',  'fail_rows_carry_a_fail_issue',     v_strict_pct, x_fail_no_reason,   total_rows FROM s
    UNION ALL SELECT 'consistency',  'warn_rows_carry_only_warn_issues', v_strict_pct, x_warn_wrong,       total_rows FROM s

    UNION ALL SELECT 'consistency',  'null_timestamps_are_quarantined',    v_strict_pct, p_null_ts,  total_rows FROM s
    UNION ALL SELECT 'consistency',  'reversed_trips_are_quarantined',     v_strict_pct, p_reversed, total_rows FROM s
    UNION ALL SELECT 'consistency',  'unresolvable_zones_are_quarantined', v_strict_pct, p_zone,     total_rows FROM s
    UNION ALL SELECT 'consistency',  'out_of_batch_pickups_are_quarantined',  v_strict_pct, p_batch_pu, total_rows FROM s
    UNION ALL SELECT 'consistency',  'out_of_batch_dropoffs_are_quarantined', v_strict_pct, p_batch_do, total_rows FROM s
    UNION ALL SELECT 'consistency',  'future_timestamps_are_quarantined',     v_strict_pct, p_future,   total_rows FROM s
    UNION ALL SELECT 'consistency',  'untraceable_rows_are_quarantined',   v_strict_pct, p_lineage,  total_rows FROM s

    -- Same names as Bronze and preload, so the three layers line up in the
    -- dashboard and a rate that jumps between them is visible as a jump.
    UNION ALL SELECT 'business',     'total_equals_sum_of_charges_v2', v_tol_pct, b_residual_v2, n_v2 FROM s
    UNION ALL SELECT 'business',     'total_equals_sum_of_charges_v1', v_tol_pct, b_residual_v1, n_v1 FROM s
    -- The aggregate limit. Every per-check threshold asks "is this rule
    -- violated too often". Only this one asks "are we excluding so much that
    -- the answer stops being about New York taxis" -- ten rules each
    -- quarantining 2 percent would all pass and between them remove a fifth.
    UNION ALL SELECT 'business',     'quarantine_rate_within_limit', v_tol_pct, b_quarantined, total_rows FROM s
    -- Advisory: can only ever WARN. A number in the results table every run
    -- beats a sentence in a comment once.
    UNION ALL SELECT 'business',     'rows_carrying_a_warning', v_advisory_pct, b_warned, total_rows FROM s
)
SELECT
    v_run_id, v_run_ts, 'silver', 'green_taxi_clean',
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    threshold_pct,
    -- Status, in the order the tests are applied.
    --   SKIP  the batch had no rows. Only table_not_empty reports that; the
    --         others stay quiet instead of each inventing a failure out of a
    --         NULL aggregate.
    --   PASS  nothing broke the rule, or it stayed under warn_pct.
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
           -- The merge key gets none either -- one duplicate fans out through
           -- every Gold join, which is not the same damage as one odd fare.
           CASE WHEN c.total_rows <= 1                          THEN 0
                WHEN c.check_name = 'one_row_per_merge_key'     THEN 0
                ELSE v_min_rows END AS min_failed_rows,
           -- 0 means "any failure is at least a WARN", which is what every
           -- check in this section should be.
           0.0 AS warn_pct
    FROM checks c
);


-- # 2. taxi_zones_clean
--
-- A full refresh of a 265-row lookup, so it is not scoped to a month -- its
-- results carry the lookup file's version key instead. Identical results
-- across months are the expected outcome, not duplication, and a second
-- version key appearing means the lookup was replaced.
--
-- `location_id_unique` is the check that matters most here. The Silver step
-- deduplicates on `location_id`, so a duplicate in this table is not a
-- duplicate in the CSV -- preload checks that -- it is the deduplication
-- having failed. One duplicate key fans the join out in Gold and inflates
-- every trip count touching that zone.

INSERT INTO nyc_quality.dq_results (
    run_id, run_ts, layer, table_name, check_category, check_name,
    failed_rows, total_rows, failed_pct, threshold_pct, status,
    batch_month, min_failed_rows, warn_pct
)
WITH bronze_zones AS (
    SELECT COUNT(DISTINCT location_id) AS n_distinct
    FROM   nyc_bronze.taxi_zones
    WHERE  location_id IS NOT NULL
),
z AS (
    SELECT
        COUNT(*)                                                          AS total_rows,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END                          AS t_empty,
        SUM(CASE WHEN location_id IS NULL THEN 1 ELSE 0 END)              AS c_id,
        -- TRIM'd by the cleaning step, so a blank here means the TRIM left an
        -- empty string rather than a value -- different from a NULL, and
        -- invisible to an IS NULL test.
        SUM(CASE WHEN zone_name    IS NULL OR TRIM(zone_name)    = '' THEN 1 ELSE 0 END) AS c_zone,
        SUM(CASE WHEN borough      IS NULL OR TRIM(borough)      = '' THEN 1 ELSE 0 END) AS c_borough,
        SUM(CASE WHEN service_zone IS NULL OR TRIM(service_zone) = '' THEN 1 ELSE 0 END) AS c_service,
        SUM(CASE WHEN source_file    IS NULL THEN 1 ELSE 0 END)           AS c_lineage,
        SUM(CASE WHEN silver_at      IS NULL THEN 1 ELSE 0 END)           AS c_silver_at,
        COUNT(location_id) - COUNT(DISTINCT location_id)                  AS u_dupes,
        SUM(CASE WHEN location_id NOT BETWEEN 1 AND 265 THEN 1 ELSE 0 END) AS v_range,
        SUM(CASE WHEN borough NOT IN ('Manhattan','Brooklyn','Queens','Bronx',
                                      'Staten Island','EWR','Unknown','N/A')
                 THEN 1 ELSE 0 END)                                       AS v_borough,
        SUM(CASE WHEN service_zone NOT IN ('Yellow Zone','Boro Zone','Airports',
                                           'EWR','N/A')
                 THEN 1 ELSE 0 END)                                       AS v_service,
        CASE WHEN COUNT(*) = 265 THEN 0 ELSE 1 END                        AS b_265,
        -- 264 and 265 both mean "the meter recorded no zone". They are kept
        -- distinct from NULL on purpose, and Gold relies on them existing.
        CASE WHEN COUNT(DISTINCT CASE WHEN location_id IN (264, 265)
                                      THEN location_id END) = 2
             THEN 0 ELSE 1 END                                            AS b_unknowns,
        CASE WHEN COUNT(DISTINCT CASE WHEN location_id IN (1, 132, 138)
                                      THEN location_id END) = 3
             THEN 0 ELSE 1 END                                            AS b_airports,
        CASE WHEN COUNT(*) = (SELECT n_distinct FROM bronze_zones)
             THEN 0 ELSE 1 END                                            AS x_reconcile
    FROM nyc_silver.taxi_zones_clean
),
checks AS (
    SELECT 'completeness' AS check_category, 'table_not_empty' AS check_name,
           v_strict_pct AS threshold_pct, t_empty AS failed_rows, 1 AS total_rows FROM z
    UNION ALL SELECT 'completeness', 'location_id_not_null',   v_strict_pct, c_id,        total_rows FROM z
    UNION ALL SELECT 'completeness', 'zone_name_not_blank',    v_strict_pct, c_zone,      total_rows FROM z
    UNION ALL SELECT 'completeness', 'borough_not_blank',      v_strict_pct, c_borough,   total_rows FROM z
    UNION ALL SELECT 'completeness', 'service_zone_not_blank', v_strict_pct, c_service,   total_rows FROM z
    UNION ALL SELECT 'completeness', 'source_file_recorded',   v_strict_pct, c_lineage,   total_rows FROM z
    UNION ALL SELECT 'completeness', 'silver_at_recorded',     v_strict_pct, c_silver_at, total_rows FROM z
    UNION ALL SELECT 'uniqueness',   'location_id_unique',     v_strict_pct, u_dupes,     total_rows FROM z
    UNION ALL SELECT 'validity',     'location_id_in_range',   v_strict_pct, v_range,     total_rows FROM z
    UNION ALL SELECT 'validity',     'borough_in_domain',      v_tol_pct,    v_borough,   total_rows FROM z
    UNION ALL SELECT 'validity',     'service_zone_in_domain', v_tol_pct,    v_service,   total_rows FROM z
    UNION ALL SELECT 'business',     'lookup_has_265_zones',       v_strict_pct, b_265,       1 FROM z
    UNION ALL SELECT 'business',     'two_unknown_zones_present',  v_strict_pct, b_unknowns,  1 FROM z
    UNION ALL SELECT 'business',     'airport_zones_present',      v_strict_pct, b_airports,  1 FROM z
    UNION ALL SELECT 'consistency',  'rows_reconcile_with_bronze', v_strict_pct, x_reconcile, 1 FROM z
)
SELECT
    v_run_id, v_run_ts, 'silver', 'taxi_zones_clean',
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    threshold_pct,
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
           CASE WHEN c.total_rows <= 1                                   THEN 0
                WHEN c.check_name IN ('location_id_unique',
                                      'location_id_not_null')            THEN 0
                ELSE v_min_rows END AS min_failed_rows,
           0.0 AS warn_pct
    FROM checks c
);


-- # 3. weather_clean
--
-- Every column arrives in Bronze as STRING and leaves Silver typed, so this
-- is where the conversion actually happens. The nine `*_parsed` checks are
-- Silver's version of Bronze's `no_nulls_added_*`: a value that was present
-- in the text and is NULL after the cast has been lost, and nothing
-- downstream can recover it.
--
-- They carry the 5/10 pair rather than 0.0 for the same reason Bronze's cast
-- checks do: a handful of unreadable values in 744 hours is the source being
-- the source; a tenth of a column turning to NULL is the cast being wrong.
--
-- `weather_hour` is the MERGE key and what Gold joins on, so it stays
-- absolute along with `one_row_per_hour`.

INSERT INTO nyc_quality.dq_results (
    run_id, run_ts, layer, table_name, check_category, check_name,
    failed_rows, total_rows, failed_pct, threshold_pct, status,
    batch_month, min_failed_rows, warn_pct
)
WITH
-- What the source offered, so the parse checks measure a loss rather than an
-- absence. A value that was already blank in Bronze was never Silver's to
-- lose.
src AS (
    SELECT COUNT(*) AS n_rows FROM vw_batch_bronze_weather
),
-- ## A parse failure is not the same as a value the source never sent
--
-- Counting NULLs in Silver alone conflates the two: a blank cell in the CSV
-- and a value that would not convert both arrive as NULL, and only the second
-- is a loss. The cleaning notebook draws this distinction itself -- it keeps
-- raw_* copies so its WARN rules can say "present and did not parse" -- so
-- the check has to draw it too, or it reports the source's gaps as the
-- pipeline's failures.
--
-- Joined on `date` rather than the parsed hour, because the hour is the thing
-- that might not have parsed. Bronze is already one row per date (its own
-- MERGE deduplicates on it), so this cannot fan out.
parse_loss AS (
    SELECT
        COUNT(*) AS n_compared,
        SUM(CASE WHEN NULLIF(trim(b.temperature_2m), '') IS NOT NULL
                  AND s.temperature_2m IS NULL THEN 1 ELSE 0 END)            AS p_temp,
        SUM(CASE WHEN NULLIF(trim(b.apparent_temperature), '') IS NOT NULL
                  AND s.apparent_temperature IS NULL THEN 1 ELSE 0 END)      AS p_apparent,
        SUM(CASE WHEN NULLIF(trim(b.precipitation_probability), '') IS NOT NULL
                  AND s.precipitation_probability IS NULL THEN 1 ELSE 0 END) AS p_prob,
        SUM(CASE WHEN NULLIF(trim(b.rain), '') IS NOT NULL
                  AND s.rain IS NULL THEN 1 ELSE 0 END)                      AS p_rain,
        SUM(CASE WHEN NULLIF(trim(b.cloud_cover), '') IS NOT NULL
                  AND s.cloud_cover IS NULL THEN 1 ELSE 0 END)               AS p_cloud,
        SUM(CASE WHEN NULLIF(trim(b.visibility), '') IS NOT NULL
                  AND s.visibility IS NULL THEN 1 ELSE 0 END)                AS p_vis,
        SUM(CASE WHEN NULLIF(trim(b.wind_speed_10m), '') IS NOT NULL
                  AND s.wind_speed_10m IS NULL THEN 1 ELSE 0 END)            AS p_wind,
        SUM(CASE WHEN NULLIF(trim(b.wind_gusts_10m), '') IS NOT NULL
                  AND s.wind_gusts_10m IS NULL THEN 1 ELSE 0 END)            AS p_gusts,
        SUM(CASE WHEN NULLIF(trim(b.weather_code), '') IS NOT NULL
                  AND s.weather_code IS NULL THEN 1 ELSE 0 END)              AS p_code,
        SUM(CASE WHEN NULLIF(trim(b.`date`), '') IS NOT NULL
                  AND s.weather_hour IS NULL THEN 1 ELSE 0 END)              AS p_hour
    FROM       vw_batch_silver_weather s
    JOIN       vw_batch_bronze_weather b ON b.`date` = s.`date`
),
-- Days the loaded months should contain, so a gap is a number rather than a
-- silence. A month needs two distinct days present before it counts as
-- covered, which excludes a single boundary hour carried in from UTC.
covered_months AS (
    SELECT date_trunc('MONTH', weather_hour) AS month_start
    FROM   vw_batch_silver_weather
    WHERE  weather_hour IS NOT NULL
    GROUP  BY date_trunc('MONTH', weather_hour)
    HAVING COUNT(DISTINCT to_date(weather_hour)) >= 2
),
expected_days AS (
    SELECT explode(sequence(month_start, last_day(month_start), INTERVAL 1 DAY)) AS d
    FROM   covered_months
),
missing_days AS (
    SELECT COUNT(*) AS n, GREATEST(COUNT(*), 1) AS denom
    FROM       expected_days e
    LEFT  JOIN (SELECT DISTINCT to_date(weather_hour) AS d
                FROM   vw_batch_silver_weather
                WHERE  weather_hour IS NOT NULL) a
           ON  e.d = a.d
    WHERE a.d IS NULL
),
-- A correlated subquery is not allowed inside an aggregate, so the
-- membership test becomes a LEFT JOIN producing a per-row boolean and the
-- aggregate then sums an ordinary column.
flagged AS (
    SELECT w.*, c.month_start IS NOT NULL AS month_is_covered
    FROM       vw_batch_silver_weather w
    LEFT  JOIN covered_months c
           ON  c.month_start = date_trunc('MONTH', w.weather_hour)
),
w AS (
    SELECT
        COUNT(*)                                                          AS total_rows,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END                          AS t_empty,

        SUM(CASE WHEN weather_hour          IS NULL THEN 1 ELSE 0 END)     AS c_hour,
        SUM(CASE WHEN dq_status             IS NULL THEN 1 ELSE 0 END)     AS c_status,
        SUM(CASE WHEN qc_error_descriptions IS NULL THEN 1 ELSE 0 END)     AS c_qc_array,
        SUM(CASE WHEN silver_at             IS NULL THEN 1 ELSE 0 END)     AS c_silver_at,
        SUM(CASE WHEN source_file_month     IS NULL
                   OR source_file_month LIKE '%{%' THEN 1 ELSE 0 END)      AS c_file_month,

        -- Uniqueness on the PARSED timestamp, not the raw string: two
        -- spellings of the same instant are distinct as strings, and a
        -- duplicate hour fans out the trip-to-weather join in Gold.
        COUNT(weather_hour) - COUNT(DISTINCT weather_hour)                 AS u_dupes,

        -- ## The promise, as in green_taxi_clean
        --
        -- The cleaning keeps an unparseable hour rather than dropping it, and
        -- labels it FAIL. So "weather_hour is never NULL" is the wrong
        -- assertion -- it would fail on a row the pipeline handled correctly.
        -- The right one is that every NULL hour carries the FAIL label, which
        -- is what Gold's valid view relies on.
        SUM(CASE WHEN weather_hour IS NULL AND dq_status <> 'FAIL'
                 THEN 1 ELSE 0 END)                                        AS p_null_hour,
        -- Every entry must carry a severity, same as green_taxi. A forgotten
        -- prefix silently downgrades a FAIL row to WARN.
        SUM(CASE WHEN qc_error_descriptions IS NOT NULL
                  AND size(filter(qc_error_descriptions,
                                  x -> NOT startswith(x, 'FAIL:')
                                   AND NOT startswith(x, 'WARN:'))) > 0
                 THEN 1 ELSE 0 END)                                        AS v_prefix,
        SUM(CASE WHEN dq_status = 'WARN'
                  AND (size(qc_error_descriptions) = 0
                    OR exists(qc_error_descriptions, x -> startswith(x, 'FAIL:')))
                 THEN 1 ELSE 0 END)                                        AS x_warn_wrong,

        -- plausibility of the typed values
        SUM(CASE WHEN temperature_2m IS NOT NULL
                  AND temperature_2m NOT BETWEEN -40.0 AND 130.0
                 THEN 1 ELSE 0 END)                                        AS v_temp,
        SUM(CASE WHEN rain IS NOT NULL AND rain < 0.0 THEN 1 ELSE 0 END)   AS v_rain,
        SUM(CASE WHEN cloud_cover IS NOT NULL
                  AND cloud_cover NOT BETWEEN 0.0 AND 100.0
                 THEN 1 ELSE 0 END)                                        AS v_cloud,
        SUM(CASE WHEN precipitation_probability IS NOT NULL
                  AND precipitation_probability NOT BETWEEN 0.0 AND 100.0
                 THEN 1 ELSE 0 END)                                        AS v_prob,
        SUM(CASE WHEN wind_gusts_10m IS NOT NULL AND wind_speed_10m IS NOT NULL
                  AND wind_gusts_10m < wind_speed_10m
                 THEN 1 ELSE 0 END)                                        AS v_gust,

        -- the derivation, same assertions as green_taxi_clean
        SUM(CASE WHEN dq_status NOT IN ('PASS','WARN','FAIL')
                 THEN 1 ELSE 0 END)                                        AS v_status_domain,
        SUM(CASE WHEN dq_status = 'PASS' AND size(qc_error_descriptions) > 0
                 THEN 1 ELSE 0 END)                                        AS x_pass_with_issues,
        SUM(CASE WHEN dq_status = 'FAIL'
                  AND NOT exists(qc_error_descriptions, x -> startswith(x, 'FAIL:'))
                 THEN 1 ELSE 0 END)                                        AS x_fail_no_reason,

        -- The description is a CASE over weather_code. Unknown with a code
        -- present means the CASE is missing a WMO value; a description other
        -- than Unknown with no code means it was derived from nothing.
        SUM(CASE WHEN weather_code IS NOT NULL
                  AND weather_description = 'Unknown' THEN 1 ELSE 0 END)   AS x_desc_unknown,
        SUM(CASE WHEN weather_code IS NULL
                  AND weather_description <> 'Unknown' THEN 1 ELSE 0 END)  AS x_desc_mismatch,

        SUM(CASE WHEN NOT month_is_covered THEN 1 ELSE 0 END)              AS v_window
    FROM flagged
),
checks AS (
    SELECT 'completeness' AS check_category, 'table_not_empty' AS check_name,
           v_strict_pct AS threshold_pct, t_empty AS failed_rows, 1 AS total_rows FROM w
    UNION ALL SELECT 'consistency',  'null_hours_are_quarantined', v_strict_pct, p_null_hour, total_rows FROM w
    UNION ALL SELECT 'completeness', 'weather_hour_populated', v_advisory_pct, c_hour,     total_rows FROM w
    UNION ALL SELECT 'completeness', 'dq_status_populated',    v_strict_pct, c_status,     total_rows FROM w
    UNION ALL SELECT 'completeness', 'qc_array_not_null',      v_strict_pct, c_qc_array,   total_rows FROM w
    UNION ALL SELECT 'completeness', 'silver_at_recorded',     v_strict_pct, c_silver_at,  total_rows FROM w
    UNION ALL SELECT 'completeness', 'source_file_month_is_real', v_advisory_pct, c_file_month, total_rows FROM w

    UNION ALL SELECT 'uniqueness',   'one_row_per_hour',       v_strict_pct, u_dupes,      total_rows FROM w

    -- Measured against the source, not against Silver's own NULLs. The
    -- denominator is the rows that could be compared.
    UNION ALL SELECT 'validity', 'temperature_parsed',        v_cast_fail_pct, (SELECT p_temp     FROM parse_loss), (SELECT n_compared FROM parse_loss) FROM w
    UNION ALL SELECT 'validity', 'apparent_temp_parsed',      v_cast_fail_pct, (SELECT p_apparent FROM parse_loss), (SELECT n_compared FROM parse_loss) FROM w
    UNION ALL SELECT 'validity', 'precip_probability_parsed', v_cast_fail_pct, (SELECT p_prob     FROM parse_loss), (SELECT n_compared FROM parse_loss) FROM w
    UNION ALL SELECT 'validity', 'rain_parsed',               v_cast_fail_pct, (SELECT p_rain     FROM parse_loss), (SELECT n_compared FROM parse_loss) FROM w
    UNION ALL SELECT 'validity', 'cloud_cover_parsed',        v_cast_fail_pct, (SELECT p_cloud    FROM parse_loss), (SELECT n_compared FROM parse_loss) FROM w
    UNION ALL SELECT 'validity', 'visibility_parsed',         v_cast_fail_pct, (SELECT p_vis      FROM parse_loss), (SELECT n_compared FROM parse_loss) FROM w
    UNION ALL SELECT 'validity', 'wind_speed_parsed',         v_cast_fail_pct, (SELECT p_wind     FROM parse_loss), (SELECT n_compared FROM parse_loss) FROM w
    UNION ALL SELECT 'validity', 'wind_gusts_parsed',         v_cast_fail_pct, (SELECT p_gusts    FROM parse_loss), (SELECT n_compared FROM parse_loss) FROM w
    UNION ALL SELECT 'validity', 'weather_code_parsed',       v_cast_fail_pct, (SELECT p_code     FROM parse_loss), (SELECT n_compared FROM parse_loss) FROM w
    UNION ALL SELECT 'validity', 'hour_parsed',               v_strict_pct,    (SELECT p_hour     FROM parse_loss), (SELECT n_compared FROM parse_loss) FROM w
    UNION ALL SELECT 'validity', 'qc_entries_carry_severity_prefix', v_strict_pct, v_prefix, total_rows FROM w
    UNION ALL SELECT 'consistency', 'warn_rows_carry_only_warn_issues', v_strict_pct, x_warn_wrong, total_rows FROM w

    UNION ALL SELECT 'validity',     'temperature_plausible',      v_tol_pct, v_temp,  total_rows FROM w
    UNION ALL SELECT 'validity',     'rain_not_negative',          v_tol_pct, v_rain,  total_rows FROM w
    UNION ALL SELECT 'validity',     'cloud_cover_0_to_100',       v_tol_pct, v_cloud, total_rows FROM w
    UNION ALL SELECT 'validity',     'precip_probability_0_to_100',v_tol_pct, v_prob,  total_rows FROM w
    UNION ALL SELECT 'validity',     'dq_status_in_domain',        v_strict_pct, v_status_domain, total_rows FROM w
    UNION ALL SELECT 'validity',     'hour_within_covered_months', v_advisory_pct, v_window, total_rows FROM w

    UNION ALL SELECT 'consistency',  'gusts_at_least_wind_speed',    v_tol_pct,    v_gust,          total_rows FROM w
    -- Tolerated, not absolute: the cleaning already WARNs on an unknown WMO
    -- code and keeps the row. A code outside the CASE is the source using a
    -- value we have not mapped -- data, not a broken transformation.
    UNION ALL SELECT 'consistency',  'description_known_for_code',   v_tol_pct,    x_desc_unknown,  total_rows FROM w
    UNION ALL SELECT 'consistency',  'description_matches_code',     v_strict_pct, x_desc_mismatch, total_rows FROM w
    UNION ALL SELECT 'consistency',  'pass_rows_carry_no_issues',    v_strict_pct, x_pass_with_issues, total_rows FROM w
    UNION ALL SELECT 'consistency',  'fail_rows_carry_a_fail_issue', v_strict_pct, x_fail_no_reason,   total_rows FROM w

    -- Silver deduplicates on the hour, so it can hold fewer rows than Bronze
    -- but never more. Equality would fail on every legitimate dedup.
    UNION ALL SELECT 'consistency',  'no_rows_invented_from_bronze', v_strict_pct,
        CASE WHEN (SELECT n_rows FROM src) >= (SELECT total_rows FROM w)
             THEN 0 ELSE 1 END, 1 FROM w
    UNION ALL SELECT 'business',     'all_expected_days_present', v_strict_pct,
        (SELECT n FROM missing_days), (SELECT denom FROM missing_days) FROM w
)
SELECT
    v_run_id, v_run_ts, 'silver', 'weather_clean',
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
           CASE WHEN c.total_rows <= 1                       THEN 0
                WHEN c.check_name IN ('one_row_per_hour',
                                      'hour_parsed')          THEN 0
                ELSE v_min_rows END AS min_failed_rows,
           -- Only the parse checks get a warn band. Everything else in this
           -- section should be at zero, so any failure is at least a WARN.
           CASE WHEN c.check_name LIKE '%_parsed'
                THEN v_cast_warn_pct ELSE 0.0 END AS warn_pct
    FROM checks c
);


-- # 4. Reconciliation with Bronze
--
-- The section that catches a `WHERE` creeping into a transformation.
--
-- ## Why it is not simply silver = bronze
--
-- The cleaning step deduplicates, so it is SUPPOSED to remove rows. The
-- assertion is that the number removed equals the number of duplicate merge
-- keys in this month of Bronze, and not one more.
--
-- ## Why the expectation is computed with Silver's own key
--
-- Silver nulls a location id outside 1-265 before it dedups. Two Bronze rows
-- with ids 300 and 400 are distinct in Bronze and both NULL in Silver, so a
-- dedup count taken on the raw Bronze columns would expect one row where
-- Silver correctly produced none. The CTE below applies the same
-- transformation before counting, so the two sides are comparable.
--
-- ## Scoped to the batch on both sides
--
-- Comparing this month of Silver against every month of Bronze is how the
-- first Bronze run reported 79% of its rows missing. Both views are the
-- month.

INSERT INTO nyc_quality.dq_results (
    run_id, run_ts, layer, table_name, check_category, check_name,
    failed_rows, total_rows, failed_pct, threshold_pct, status,
    batch_month, min_failed_rows, warn_pct
)
WITH bronze_keyed AS (
    -- Bronze rows with Silver's merge key applied, so the dedup expectation
    -- matches what Silver actually did.
    SELECT
        total_amount,
        ROW_NUMBER() OVER (
            PARTITION BY vendor_id, lpep_pickup_datetime, lpep_dropoff_datetime,
                CASE WHEN pu_location_id BETWEEN 1 AND 265 THEN pu_location_id END,
                CASE WHEN do_location_id BETWEEN 1 AND 265 THEN do_location_id END,
                trip_distance, total_amount
            ORDER BY lpep_pickup_datetime ASC, fare_amount DESC) AS rn
    FROM vw_batch_bronze_taxi
),
bronze_expected AS (
    SELECT COUNT(*)                                   AS n_rows,
           ROUND(SUM(CAST(total_amount AS DOUBLE)), 2) AS revenue
    FROM   bronze_keyed
    WHERE  rn = 1
),
bronze_all AS (
    SELECT COUNT(*) AS n_rows FROM vw_batch_bronze_taxi
),
silver_side AS (
    SELECT COUNT(*)                     AS n_rows,
           ROUND(SUM(total_amount), 2)  AS revenue
    FROM   vw_batch_silver_taxi
),
-- Files present in this month of Bronze that produced no Silver row at all.
-- Name-level, not count-level: two files of the same size reconcile by count
-- while one of them never loaded.
unprocessed AS (
    SELECT COUNT(*) AS n
    FROM (
        SELECT DISTINCT source_file FROM vw_batch_bronze_taxi
        EXCEPT
        SELECT DISTINCT source_file FROM vw_batch_silver_taxi
    )
),
checks AS (
    SELECT 'consistency' AS check_category, 'rows_reconcile_with_bronze' AS check_name,
           v_strict_pct AS threshold_pct,
           CASE WHEN (SELECT n_rows FROM bronze_expected)
                   = (SELECT n_rows FROM silver_side) THEN 0 ELSE 1 END AS failed_rows,
           1 AS total_rows
    -- A tolerance, not equality: the amounts are DOUBLE and the sum of tens of
    -- thousands of them is not associative. One cent, or a thousandth of the
    -- total, whichever is larger.
    UNION ALL SELECT 'consistency', 'revenue_preserved', v_strict_pct,
        CASE WHEN ABS(COALESCE((SELECT revenue FROM bronze_expected), 0)
                    - COALESCE((SELECT revenue FROM silver_side), 0))
                  <= GREATEST(1.0, 0.001 * ABS(COALESCE((SELECT revenue FROM bronze_expected), 0)))
             THEN 0 ELSE 1 END, 1
    UNION ALL SELECT 'consistency', 'every_bronze_file_present', v_strict_pct,
        (SELECT n FROM unprocessed), 1
    -- The dedup remainder as a rate, so a dedup that suddenly eats a third of
    -- the month is a number rather than a silent success.
    UNION ALL SELECT 'consistency', 'dedup_removal_rate', v_tol_pct,
        GREATEST((SELECT n_rows FROM bronze_all) - (SELECT n_rows FROM bronze_expected), 0),
        GREATEST((SELECT n_rows FROM bronze_all), 1)
    -- Advisory: how much of the month Silver kept. Reported every run so the
    -- trend is visible before it becomes a threshold argument.
    UNION ALL SELECT 'business', 'silver_retention_rate', v_advisory_pct,
        GREATEST((SELECT n_rows FROM bronze_all) - (SELECT n_rows FROM silver_side), 0),
        GREATEST((SELECT n_rows FROM bronze_all), 1)
)
SELECT
    v_run_id, v_run_ts, 'silver', 'green_taxi_clean',
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    threshold_pct,
    CASE WHEN total_rows = 0                                               THEN 'SKIP'
         WHEN failed_rows = 0                                              THEN 'PASS'
         WHEN failed_rows <= min_failed_rows                               THEN 'WARN'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= threshold_pct THEN 'WARN'
         ELSE 'FAIL' END,
    v_batch_month,
    min_failed_rows,
    0.0
FROM (
    -- No floor on reconciliation. Sections 1-3 measure data and code that can
    -- be imperfect at the edges; this measures whether a transformation moved
    -- every row it was given. One row short is a bug, not a quirk.
    SELECT c.*, 0 AS min_failed_rows
    FROM checks c
);


-- # 5. Join coverage — what Gold will actually resolve
--
-- The most useful checks in the notebook, because they predict a failure that
-- never raises an error.
--
-- Every dimension in Gold has an Unknown member keyed `-1`, so an
-- unresolvable foreign key does not drop the trip -- it lands in a bucket
-- labelled "we do not know". The star schema works perfectly and the answer
-- is quietly wrong.
--
-- | Check | Predicts |
-- |---|---|
-- | `pickup_zone_exists_in_lookup` | trips landing on `dim_zone` Unknown |
-- | `trip_hour_has_weather` | trips landing on `dim_weather` Unknown |
--
-- Measured against the VALID trips for this batch -- the rows Gold will
-- actually read -- not the whole table. A quarantined trip with an
-- unresolvable zone is not a join problem; it was never going to Gold.
--
-- The zone joins read taxi_zones_clean whole, which is correct rather than an
-- oversight: it is a full refresh with no month in it, so the batch's zones
-- ARE the whole table. Weather is scoped, because weather does have months.
--
-- Two denominators, deliberately. The keys answer "how many lookups need
-- fixing"; the row counts answer "how much of the month it costs". One
-- unmatched id can be a single test row or a fifth of the load.

INSERT INTO nyc_quality.dq_results (
    run_id, run_ts, layer, table_name, check_category, check_name,
    failed_rows, total_rows, failed_pct, threshold_pct, status,
    batch_month, min_failed_rows, warn_pct
)
WITH pu AS (
    SELECT COUNT(*) AS unmatched,
           GREATEST((SELECT COUNT(DISTINCT pu_location_id)
                     FROM vw_batch_valid_trips), 1) AS total
    FROM (
        SELECT DISTINCT t.pu_location_id
        FROM       vw_batch_valid_trips t
        LEFT  JOIN nyc_silver.taxi_zones_clean z ON t.pu_location_id = z.location_id
        WHERE t.pu_location_id IS NOT NULL AND z.location_id IS NULL
    )
),
do_ AS (
    SELECT COUNT(*) AS unmatched,
           GREATEST((SELECT COUNT(DISTINCT do_location_id)
                     FROM vw_batch_valid_trips), 1) AS total
    FROM (
        SELECT DISTINCT t.do_location_id
        FROM       vw_batch_valid_trips t
        LEFT  JOIN nyc_silver.taxi_zones_clean z ON t.do_location_id = z.location_id
        WHERE t.do_location_id IS NOT NULL AND z.location_id IS NULL
    )
),
-- Two versions of the same question, because they answer different things.
--
-- `wx` joins the BATCH's weather: did this month's weather file cover this
-- month's trips? That is the check, and it is what the rest of the notebook
-- means by per-batch.
--
-- `wx_any` joins ALL of weather_clean: will Gold resolve the hour from some
-- other month's file? Advisory, because it can only ever be equal to or
-- better than the first.
--
-- The gap between them is the month boundary. The weather timestamps are UTC
-- and the trips are New York, so a trip late on the last day of the month
-- lands on an hour that arrived in the NEXT month's file. Reported rather
-- than hidden: one number rising while the other does not is a boundary
-- artefact, and both rising together is a missing weather file.
wx AS (
    SELECT COUNT(*) AS unmatched,
           GREATEST((SELECT COUNT(DISTINCT date_trunc('HOUR', lpep_pickup_datetime))
                     FROM   vw_batch_valid_trips
                     WHERE  lpep_pickup_datetime IS NOT NULL), 1) AS total
    FROM (
        SELECT DISTINCT date_trunc('HOUR', t.lpep_pickup_datetime) AS h
        FROM       vw_batch_valid_trips t
        LEFT  JOIN vw_batch_silver_weather w
               ON  date_trunc('HOUR', t.lpep_pickup_datetime) = w.weather_hour
        WHERE t.lpep_pickup_datetime IS NOT NULL AND w.weather_hour IS NULL
    )
),
wx_any AS (
    SELECT COUNT(*) AS unmatched,
           GREATEST((SELECT COUNT(DISTINCT date_trunc('HOUR', lpep_pickup_datetime))
                     FROM   vw_batch_valid_trips
                     WHERE  lpep_pickup_datetime IS NOT NULL), 1) AS total
    FROM (
        SELECT DISTINCT date_trunc('HOUR', t.lpep_pickup_datetime) AS h
        FROM       vw_batch_valid_trips t
        LEFT  JOIN nyc_silver.weather_clean w
               ON  date_trunc('HOUR', t.lpep_pickup_datetime) = w.weather_hour
        WHERE t.lpep_pickup_datetime IS NOT NULL AND w.weather_hour IS NULL
    )
),
trips AS (
    SELECT
        GREATEST(COUNT(*), 1)                                   AS total_rows,
        SUM(CASE WHEN zp.location_id IS NULL THEN 1 ELSE 0 END) AS t_pu,
        SUM(CASE WHEN zd.location_id IS NULL THEN 1 ELSE 0 END) AS t_do
    FROM       vw_batch_valid_trips t
    LEFT  JOIN nyc_silver.taxi_zones_clean zp ON t.pu_location_id = zp.location_id
    LEFT  JOIN nyc_silver.taxi_zones_clean zd ON t.do_location_id = zd.location_id
),
checks AS (
    SELECT 'consistency' AS check_category, 'pickup_zone_exists_in_lookup' AS check_name,
           v_strict_pct AS threshold_pct,
           (SELECT unmatched FROM pu) AS failed_rows,
           (SELECT total FROM pu)     AS total_rows
    UNION ALL SELECT 'consistency', 'dropoff_zone_exists_in_lookup', v_strict_pct,
           (SELECT unmatched FROM do_), (SELECT total FROM do_)
    -- Tolerated rather than absolute: pickups outside the loaded window are
    -- WARN, not FAIL, so they stay in the valid view and a handful of hours
    -- legitimately have no weather row -- especially at a month boundary,
    -- where the UTC weather timestamps and the New York trip timestamps do
    -- not line up.
    UNION ALL SELECT 'consistency', 'trip_hour_has_weather', v_tol_pct,
           (SELECT unmatched FROM wx), (SELECT total FROM wx)
    -- Advisory twin: the same hours measured against every loaded month.
    UNION ALL SELECT 'consistency', 'trip_hour_has_weather_any_batch', v_advisory_pct,
           (SELECT unmatched FROM wx_any), (SELECT total FROM wx_any)
    -- Advisory twins: the row cost of the two checks above.
    UNION ALL SELECT 'consistency', 'trips_with_unmatched_pickup_zone', v_advisory_pct,
           (SELECT t_pu FROM trips), (SELECT total_rows FROM trips)
    UNION ALL SELECT 'consistency', 'trips_with_unmatched_dropoff_zone', v_advisory_pct,
           (SELECT t_do FROM trips), (SELECT total_rows FROM trips)
)
SELECT
    v_run_id, v_run_ts, 'silver', 'green_taxi_clean',
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    threshold_pct,
    CASE WHEN total_rows = 0                                               THEN 'SKIP'
         WHEN failed_rows = 0                                              THEN 'PASS'
         WHEN failed_rows <= min_failed_rows                               THEN 'WARN'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= threshold_pct THEN 'WARN'
         ELSE 'FAIL' END,
    v_batch_month,
    min_failed_rows,
    0.0
FROM (
    -- The key-level checks get no floor: an unmatched lookup key is a lookup
    -- to fix, however few trips it touches. The advisory twins report the
    -- row cost and never gate.
    SELECT c.*, 0 AS min_failed_rows
    FROM checks c
);


-- # 6. Audit log
--
-- One row for the batch, not one per execution. Read by (layer, batch_month)
-- so a re-run of the same month replaces its verdict rather than adding a
-- second one that disagrees.

INSERT INTO nyc_quality.dq_run_log (
    run_id, run_ts, layer, tables_checked, checks_run,
    checks_passed, checks_warned, checks_failed, overall_status, finished_at,
    batch_month, checks_skipped
)
SELECT
    v_run_id,
    v_run_ts,
    'silver',
    COUNT(DISTINCT d.table_name)                                 AS tables_checked,
    COUNT(*)                                                     AS checks_run,
    SUM(CASE WHEN d.status = 'PASS' THEN 1 ELSE 0 END)           AS checks_passed,
    SUM(CASE WHEN d.status = 'WARN' THEN 1 ELSE 0 END)           AS checks_warned,
    SUM(CASE WHEN d.status = 'FAIL' THEN 1 ELSE 0 END)           AS checks_failed,
    -- FAIL means THIS LAYER IS NOT USABLE -- a blocking check failed, and the
    -- gate below raises on the same condition. A non-blocking failure is a
    -- recorded defect, not a reason to stop, so it lands on WARN.
    --
    -- SKIP is excluded from the WARN test. A skipped check did not find
    -- anything wrong, it did not run, and counting it would put an empty
    -- batch permanently on WARN for reasons unrelated to quality.
    --
    -- `b.check_name IS NOT NULL` is the LEFT JOIN's way of saying "this check
    -- is on the blocking list". It replaces a correlated EXISTS inside the
    -- aggregate, which Spark will not resolve there.
    CASE WHEN SUM(CASE WHEN d.status = 'FAIL' AND b.check_name IS NOT NULL
                       THEN 1 ELSE 0 END) > 0                    THEN 'FAIL'
         WHEN SUM(CASE WHEN d.status NOT IN ('PASS', 'SKIP')
                       THEN 1 ELSE 0 END) > 0                    THEN 'WARN'
         ELSE 'PASS' END                                         AS overall_status,
    current_timestamp()                                          AS finished_at,
    v_batch_month                                                AS batch_month,
    SUM(CASE WHEN d.status = 'SKIP' THEN 1 ELSE 0 END)           AS checks_skipped
FROM       nyc_quality.dq_results d
JOIN       vw_batch_scope s
       ON  s.table_name = d.table_name AND s.batch_month = d.batch_month
LEFT  JOIN vw_silver_blocking_checks b
       ON  b.table_name = d.table_name AND b.check_name = d.check_name
WHERE  d.layer = 'silver' AND d.check_category <> 'gate';


-- # 7. Results — this batch only
--
-- Every query here joins vw_batch_scope, so it shows one month of
-- green_taxi_clean and weather_clean plus the current version of
-- taxi_zones_clean. The table underneath still holds every batch ever
-- checked; section 9 is the cross-batch view.

SELECT * FROM nyc_quality.dq_run_log
WHERE  layer = 'silver' AND batch_month = v_batch_month;

SELECT batch_month, table_name, status, COUNT(*) AS checks
FROM   nyc_quality.dq_results
-- USING keeps every column reference unqualified: the join columns are
-- merged into one, so nothing becomes ambiguous.
JOIN   vw_batch_scope USING (table_name, batch_month)
WHERE  layer = 'silver' AND check_category <> 'gate'
GROUP  BY batch_month, table_name, status
ORDER  BY table_name, status;

-- Everything that is not a clean pass, worst first. SKIP is shown last and
-- separately: it is an absence of evidence, not a defect.
SELECT table_name, check_category, check_name,
       failed_rows, total_rows, failed_pct, warn_pct, threshold_pct,
       min_failed_rows, status
FROM   nyc_quality.dq_results
JOIN   vw_batch_scope USING (table_name, batch_month)
WHERE  layer = 'silver' AND check_category <> 'gate' AND status <> 'PASS'
ORDER  BY CASE status WHEN 'FAIL' THEN 0 WHEN 'WARN' THEN 1 ELSE 2 END,
          failed_pct DESC;

-- Passed, but not cleanly. Only the weather parse checks can land here --
-- everything else has warn_pct = 0. Read it like a WARN: fine this month, and
-- a number climbing month over month is a cast going wrong.
SELECT table_name, check_name, failed_rows, total_rows, failed_pct, warn_pct
FROM   nyc_quality.dq_results
JOIN   vw_batch_scope USING (table_name, batch_month)
WHERE  layer = 'silver' AND status = 'PASS' AND failed_rows > 0
ORDER  BY failed_pct DESC;

-- The checks the row floor -- not the percentage -- kept out of FAIL. Worth
-- reading every run: these are real defects held below the stop line, and a
-- count that climbs month over month is the floor going stale.
SELECT table_name, check_name, failed_rows, total_rows, failed_pct,
       threshold_pct, min_failed_rows
FROM   nyc_quality.dq_results
JOIN   vw_batch_scope USING (table_name, batch_month)
WHERE  layer = 'silver' AND status = 'WARN'
  AND  failed_rows > 0 AND failed_rows <= min_failed_rows
  AND  failed_pct > threshold_pct
ORDER  BY failed_rows DESC;


-- # 8. Gate — per table
--
-- A table is STOPPED when either is true of it:
--
-- | Trigger | Meaning |
-- |---|---|
-- | a blocking check for that table FAILED | the transformation is wrong, not the data |
-- | `v_max_total_failures` or more FAILs on that table | no single thing is fatal, but the batch is broadly wrong |
--
-- Counted per table, not across the run. Three tables each two checks short
-- of trouble adding up to a stop is a stop no single source deserved.
--
-- ### Only a required table raises
--
-- Every table gets a verdict; only `v_required_tables` halts the run. A bad
-- weather batch therefore stops weather -> Gold and lets the trip path
-- continue.
--
-- ### How Gold reads the verdict
--
-- Written back into `dq_results` as one synthetic check per table,
-- `batch_cleared_for_gold`, under check_category `gate`. No new table: Gold
-- asks the same place everything else is recorded.
--
--     SELECT status
--     FROM   nyc_mobility.nyc_quality.dq_results
--     WHERE  layer = 'silver'
--       AND  batch_month = :year_month
--       AND  table_name  = 'green_taxi_clean'
--       AND  check_name  = 'batch_cleared_for_gold';
--
-- They are written after section 6, so they do not inflate the audit counts.

-- Enforcement switch.
--
-- FALSE: verdicts are still computed, written and displayed, but the notebook
-- does not raise, so the job carries on to Gold. TRUE: a stopped required
-- table raises as designed.
--
-- One line so that suspending enforcement during a migration is one edit
-- and a grep for v_gate_enforce finds it. Leave it TRUE: a gate parked on
-- FALSE indefinitely is not a gate, it is a report.
DECLARE OR REPLACE VARIABLE v_gate_enforce BOOLEAN;
SET VAR v_gate_enforce = TRUE;

DECLARE OR REPLACE VARIABLE v_max_total_failures INT;
SET VAR v_max_total_failures = 5;


-- ### The blocking list has to be checked against reality
--
-- A pair in that list that no check ever emits is inert: it matches nothing,
-- so the gate neither blocks nor complains. That is what makes it dangerous
-- -- the list reads like a guarantee, and the next person to rename a check
-- turns one of those guarantees off without touching the gate. The Bronze
-- gate carried four such names for weeks.
--
-- Anything returned below is a guarantee this gate is making and the notebook
-- never produces. Expect zero rows.
SELECT b.table_name, b.check_name AS blocking_pair_never_produced
FROM   vw_silver_blocking_checks b
LEFT   JOIN (SELECT DISTINCT table_name, check_name
             FROM   nyc_quality.dq_results
             JOIN   vw_batch_scope USING (table_name, batch_month)
             WHERE  layer = 'silver') r
       ON  b.table_name = r.table_name AND b.check_name = r.check_name
WHERE  r.check_name IS NULL;


-- ### Per-table verdicts
CREATE OR REPLACE TEMPORARY VIEW vw_silver_gate AS
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
    FROM       nyc_quality.dq_results d
    JOIN       vw_batch_scope s
           ON  s.table_name = d.table_name AND s.batch_month = d.batch_month
    LEFT  JOIN vw_silver_blocking_checks b
           ON  b.table_name = d.table_name AND b.check_name = d.check_name
    WHERE  d.layer = 'silver' AND d.check_category <> 'gate'
    GROUP  BY d.table_name, s.batch_month
)
SELECT table_name, batch_month, failures, blocking_failures, blocking_names, skipped,
       CASE WHEN blocking_failures > 0
              OR failures >= v_max_total_failures THEN 'STOP' ELSE 'GO' END AS verdict,
       array_contains(v_required_tables, table_name)                        AS is_required
FROM   scored;

SELECT table_name, batch_month, verdict, is_required,
       blocking_failures, failures, skipped, blocking_names
FROM   vw_silver_gate
ORDER  BY CASE verdict WHEN 'STOP' THEN 0 ELSE 1 END, table_name;


-- ### Record the verdicts where Gold can read them
INSERT INTO nyc_quality.dq_results (
    run_id, run_ts, layer, table_name, check_category, check_name,
    failed_rows, total_rows, failed_pct, threshold_pct, status,
    batch_month, min_failed_rows, warn_pct
)
SELECT v_run_id, v_run_ts, 'silver', table_name,
       'gate', 'batch_cleared_for_gold',
       blocking_failures, 1,
       CASE WHEN verdict = 'STOP' THEN 100.0 ELSE 0.0 END,
       0.0,
       CASE WHEN verdict = 'STOP' THEN 'FAIL' ELSE 'PASS' END,
       batch_month, 0, 0.0
FROM   vw_silver_gate;


-- ### Raise, but only for a required table
DECLARE OR REPLACE VARIABLE v_stopped_required STRING;
DECLARE OR REPLACE VARIABLE v_stopped_optional STRING;

SET VAR v_stopped_required = (
    SELECT COALESCE(concat_ws(', ', collect_list(table_name)), '')
    FROM   vw_silver_gate WHERE verdict = 'STOP' AND is_required);

SET VAR v_stopped_optional = (
    SELECT COALESCE(concat_ws(', ', collect_list(table_name)), '')
    FROM   vw_silver_gate WHERE verdict = 'STOP' AND NOT is_required);

SELECT v_batch_month                                    AS batch_month,
       COALESCE(NULLIF(v_stopped_required, ''), 'none') AS stopped_required,
       COALESCE(NULLIF(v_stopped_optional, ''), 'none') AS stopped_optional,
       CASE WHEN v_stopped_required <> ''
              THEN 'will stop the run'
            WHEN v_stopped_optional <> ''
              THEN 'will continue; the named tables are held back from Gold'
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
    FROM   vw_silver_gate WHERE verdict = 'STOP' AND is_required);

SELECT CASE
    WHEN v_gate_enforce AND v_stopped_required <> ''
      THEN raise_error(CONCAT('Silver DQ gate FAILED for ', v_batch_month,
                              ': ', v_stopped_detail,
                              '. See nyc_quality.dq_results for run ', v_run_id))
    WHEN v_stopped_optional <> ''
      THEN CONCAT('Silver DQ gate PASSED for ', v_batch_month,
                  '; held back from Gold: ', v_stopped_optional)
    ELSE CONCAT('Silver DQ gate PASSED for ', v_batch_month,
                ' -- every table cleared')
END AS gate;

-- A pass with non-blocking failures recorded is a normal, honest outcome.
-- Read them here and decide whether each is a threshold to measure or a
-- defect to fix:
SELECT table_name, check_category, check_name,
       failed_rows, total_rows, failed_pct, threshold_pct, min_failed_rows
FROM   nyc_quality.dq_results
JOIN   vw_batch_scope USING (table_name, batch_month)
WHERE  layer = 'silver' AND status = 'FAIL' AND check_category <> 'gate'
ORDER  BY failed_pct DESC;


-- # 9. Afterwards
--
-- Sections 7 and 8 are the batch. This is the history, and it needs no extra
-- table: dq_results already holds every month, and batch_month is what turns
-- it into a per-month record.

-- The defence of every row not in Gold, for this batch.
SELECT reason, COUNT(*) AS trips
FROM  (SELECT explode(filter(qc_error_descriptions, x -> startswith(x, 'FAIL:'))) AS reason
       FROM   vw_batch_silver_taxi
       WHERE  dq_status = 'FAIL')
GROUP  BY reason
ORDER  BY trips DESC;

SELECT dq_status,
       COUNT(*)                                           AS trips,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 3) AS pct,
       ROUND(SUM(total_amount), 2)                        AS revenue
FROM   vw_batch_silver_taxi
GROUP  BY dq_status
ORDER  BY trips DESC;

-- One row per month, the headline. This is the per-month dashboard. The
-- static zone versions are excluded: they are not months and would sort in
-- among them.
SELECT batch_month,
       MAX(run_ts)                                      AS last_checked,
       COUNT(*)                                         AS checks,
       SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS passed,
       SUM(CASE WHEN status = 'WARN' THEN 1 ELSE 0 END) AS warned,
       SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed,
       SUM(CASE WHEN status = 'SKIP' THEN 1 ELSE 0 END) AS skipped
FROM   nyc_quality.vw_dq_by_month
WHERE  layer = 'silver' AND batch_month NOT LIKE 'static-%'
GROUP  BY batch_month
ORDER  BY batch_month;

-- Has a check moved between months?
--
-- Comparing months rather than runs is the useful question. Two runs of the
-- same month should be identical -- that is the idempotency test, and LAG
-- over run_ts answered it with a row of zeroes. A rate that climbs from March
-- to April to May is a source drifting, which is worth catching early.
SELECT table_name, check_name, batch_month, failed_pct, status,
       LAG(failed_pct) OVER (PARTITION BY table_name, check_name ORDER BY batch_month) AS previous_month_pct,
       ROUND(failed_pct - LAG(failed_pct) OVER (PARTITION BY table_name, check_name ORDER BY batch_month), 4) AS change
FROM   nyc_quality.vw_dq_by_month
WHERE  layer = 'silver' AND batch_month NOT LIKE 'static-%'
ORDER  BY table_name, check_name, batch_month;

-- Checks that have never once passed, across every month loaded so far. A
-- rule that is always red is either a real standing defect or a rule that
-- does not describe this transformation. Either way it needs a decision, not
-- another month of being ignored.
SELECT table_name, check_name,
       COUNT(*)        AS months_checked,
       MAX(failed_pct) AS worst_pct,
       MIN(failed_pct) AS best_pct
FROM   nyc_quality.vw_dq_by_month
WHERE  layer = 'silver' AND batch_month NOT LIKE 'static-%'
GROUP  BY table_name, check_name
HAVING SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) = 0
   AND SUM(CASE WHEN status = 'SKIP' THEN 1 ELSE 0 END) = 0
ORDER  BY worst_pct DESC;

-- The latest run of every layer side by side: the whole pipeline's quality
-- position in one row each.
SELECT layer, batch_month, overall_status, checks_run, checks_passed,
       checks_warned, checks_failed, checks_skipped, run_ts
FROM   nyc_quality.vw_latest_dq_run
ORDER  BY CASE layer WHEN 'preload' THEN 1 WHEN 'bronze' THEN 2
                     WHEN 'silver'  THEN 3 ELSE 4 END;
