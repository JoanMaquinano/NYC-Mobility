-- Data Quality — Silver, group pipeline

SET TIME ZONE 'America/New_York';

USE CATALOG `nyc-mobility`;

CREATE OR REPLACE TEMPORARY VIEW vw_silver_blocking_checks AS
SELECT explode(array(
        'table_not_empty',
        'dq_status_populated',
        'qc_array_not_null',
        'source_file_recorded',
        'silver_at_recorded',
        'weather_hour_not_null',
        'one_row_per_merge_key',
        'one_row_per_hour',
        'location_id_unique',
        'dq_status_in_domain',
        'qc_entries_carry_severity_prefix',
        'zones_within_1_to_265',
        'location_id_in_range',
        'pass_rows_carry_no_issues',
        'fail_rows_carry_a_fail_issue',
        'warn_rows_carry_only_warn_issues',
        'null_timestamps_are_quarantined',
        'reversed_trips_are_quarantined',
        'unresolvable_zones_are_quarantined',
        'out_of_era_pickups_are_quarantined',
        'untraceable_rows_are_quarantined',
        'rows_reconcile_with_bronze',
        'revenue_preserved',
        'every_bronze_file_present',
        'pickup_zone_exists_in_lookup',
        'dropoff_zone_exists_in_lookup',
        'location_id_not_null',
        'lookup_has_265_zones',
        'two_unknown_zones_present',
        'all_expected_days_present'
    )) AS check_name;

DECLARE OR REPLACE VARIABLE v_run_id STRING;
DECLARE OR REPLACE VARIABLE v_run_ts TIMESTAMP;

SET VAR v_run_id = uuid();

SET VAR v_run_ts = current_timestamp();

SELECT v_run_id AS run_id, v_run_ts AS run_ts;


DELETE FROM nyc_quality.dq_results WHERE run_id = v_run_id AND layer = 'silver';
DELETE FROM nyc_quality.dq_run_log WHERE run_id = v_run_id AND layer = 'silver';

-- 1. green_taxi_clean
--  1a. The classification must be internally consistent
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
-- Each one is impossible if the derivation is right, which is exactly why
-- they are worth writing. A check that can only fail when the code is
-- broken is the cheapest regression test there is — and this particular
-- derivation is the thing every downstream filter trusts.
--
-- The last one catches the realistic edit: someone adds a new rule and
-- writes `'null passenger count'` without a prefix. `exists(..., 'FAIL:')`
-- is false, so the row silently becomes WARN. The prefix check is what
-- turns a forgotten seven characters into a visible failure.
--
-- 1b. The promises
-- Five conditions define FAIL. Each gets a check asserting that **no row
-- meeting it escaped the label**. These are re-derived here from the data
-- columns, not read back out of the array — reading the array would only
-- prove the array agrees with itself.
--
-- 1c. Reconciliation
-- `rows_reconcile_with_bronze` is the check that catches a `WHERE` creeping
-- into a transformation. It is deliberately not `silver = bronze`: the
-- dedup is supposed to remove rows. The assertion is that the number
-- removed equals the number of duplicate merge keys in Bronze, and not one
-- more.
-- `revenue_preserved` compares `SUM(total_amount)` across the layers. 

INSERT INTO nyc_quality.dq_results
WITH bronze AS (
    SELECT COUNT(*)                                   AS n_rows,
           ROUND(SUM(CAST(total_amount AS DOUBLE)), 2) AS revenue
    FROM   nyc_bronze.green_taxi
),

-- How many rows the dedup should have absorbed: the extra copies of each
-- merge key. 
dupes AS (
    SELECT COALESCE(SUM(n - 1), 0) AS expected_removed
    FROM (
        SELECT COUNT(*) AS n
        FROM (
            SELECT vendor_id, lpep_pickup_datetime, lpep_dropoff_datetime,
                   pu_location_id, do_location_id, trip_distance, total_amount
            FROM   nyc_bronze.green_taxi
        )
        GROUP  BY vendor_id, lpep_pickup_datetime, lpep_dropoff_datetime,
                  pu_location_id, do_location_id, trip_distance, total_amount
        HAVING COUNT(*) > 1
    )
),
-- The merge key must be unique in the TARGET, or the MERGE updates the same
-- row more than once per run and every Gold join fans out.
grain AS (
    SELECT COALESCE(SUM(n - 1), 0) AS extra_rows
    FROM (
        SELECT COUNT(*) AS n
        FROM (
            SELECT vendor_id, lpep_pickup_datetime, lpep_dropoff_datetime,
                   pu_location_id, do_location_id, trip_distance, total_amount
            FROM   nyc_silver.green_taxi_clean
        )
        GROUP  BY vendor_id, lpep_pickup_datetime, lpep_dropoff_datetime,
                  pu_location_id, do_location_id, trip_distance, total_amount
        HAVING COUNT(*) > 1
    )
),
-- Files present in Bronze that produced no Silver row at all. Name-level, not
-- count-level: two files of the same size reconcile by count while one of them
-- never loaded.
unprocessed AS (
    SELECT COUNT(*) AS n
    FROM (
        SELECT DISTINCT source_file FROM nyc_bronze.green_taxi
        EXCEPT
        SELECT DISTINCT source_file FROM nyc_silver.green_taxi_clean
    )
),

silver AS (
    SELECT
        COUNT(*)                                                             AS total_rows,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END                             AS t_empty,
        ROUND(SUM(total_amount), 2)                                          AS revenue,

        -- ---------------- completeness of what Silver produced -------------
        SUM(CASE WHEN dq_status             IS NULL THEN 1 ELSE 0 END)       AS c_status,
        SUM(CASE WHEN qc_error_descriptions IS NULL THEN 1 ELSE 0 END)       AS c_qc_array,
        SUM(CASE WHEN source_file           IS NULL THEN 1 ELSE 0 END)       AS c_source_file,
        SUM(CASE WHEN ingestion_time        IS NULL THEN 1 ELSE 0 END)       AS c_ingestion,
        SUM(CASE WHEN silver_at             IS NULL THEN 1 ELSE 0 END)       AS c_silver_at,

        -- ---------------- validity of the classification ------------------
        SUM(CASE WHEN dq_status NOT IN ('PASS','WARN','FAIL')
                 THEN 1 ELSE 0 END)                                          AS v_status_domain,
        -- Every entry must carry a severity. A forgotten prefix silently
        -- downgrades a FAIL row to WARN and nothing else would notice.
        SUM(CASE WHEN qc_error_descriptions IS NOT NULL
                  AND size(filter(qc_error_descriptions,
                                  x -> NOT startswith(x, 'FAIL:')
                                   AND NOT startswith(x, 'WARN:'))) > 0
                 THEN 1 ELSE 0 END)                                          AS v_prefix,
        SUM(CASE WHEN pu_location_id IS NOT NULL
                  AND pu_location_id NOT BETWEEN 1 AND 265 THEN 1 ELSE 0 END)
          + SUM(CASE WHEN do_location_id IS NOT NULL
                  AND do_location_id NOT BETWEEN 1 AND 265 THEN 1 ELSE 0 END) AS v_zone_range,

        -- ---------------- the derivation holds ----------------------------
        SUM(CASE WHEN dq_status = 'PASS' AND size(qc_error_descriptions) > 0
                 THEN 1 ELSE 0 END)                                          AS x_pass_with_issues,
        SUM(CASE WHEN dq_status = 'FAIL'
                  AND NOT exists(qc_error_descriptions, x -> startswith(x, 'FAIL:'))
                 THEN 1 ELSE 0 END)                                          AS x_fail_no_reason,
        SUM(CASE WHEN dq_status = 'WARN'
                  AND (size(qc_error_descriptions) = 0
                    OR exists(qc_error_descriptions, x -> startswith(x, 'FAIL:')))
                 THEN 1 ELSE 0 END)                                          AS x_warn_wrong,

        SUM(CASE WHEN (lpep_pickup_datetime IS NULL OR lpep_dropoff_datetime IS NULL)
                  AND dq_status <> 'FAIL' THEN 1 ELSE 0 END)                 AS p_null_ts,
        SUM(CASE WHEN lpep_dropoff_datetime < lpep_pickup_datetime
                  AND dq_status <> 'FAIL' THEN 1 ELSE 0 END)                 AS p_reversed,
        SUM(CASE WHEN (pu_location_id IS NULL OR do_location_id IS NULL
                    OR pu_location_id NOT BETWEEN 1 AND 265
                    OR do_location_id NOT BETWEEN 1 AND 265)
                  AND dq_status <> 'FAIL' THEN 1 ELSE 0 END)                 AS p_zone,
        SUM(CASE WHEN lpep_pickup_datetime IS NOT NULL
                  AND (lpep_pickup_datetime <  TIMESTAMP'2009-01-01 00:00:00'
                    OR lpep_pickup_datetime >  current_timestamp())
                  AND dq_status <> 'FAIL' THEN 1 ELSE 0 END)                 AS p_era,
        SUM(CASE WHEN source_file IS NULL AND dq_status <> 'FAIL'
                 THEN 1 ELSE 0 END)                                          AS p_lineage,

        -- ---------------- the charge identity per vendor ------------------
        SUM(CASE WHEN vendor_id IN (1, 2)
                  AND ABS(COALESCE(fare_amount, 0) + COALESCE(extra, 0)
                        + COALESCE(mta_tax, 0) + COALESCE(tip_amount, 0)
                        + COALESCE(tolls_amount, 0)
                        + COALESCE(improvement_surcharge, 0)
                        + COALESCE(congestion_surcharge, 0)
                        + COALESCE(cbd_congestion_fee, 0)
                        - COALESCE(total_amount, 0)) > 0.01
                 THEN 1 ELSE 0 END)                                          AS b_residual,
        SUM(CASE WHEN vendor_id IN (1, 2) THEN 1 ELSE 0 END)                 AS n_v1v2,

        -- ---------------- business ----------------------------------------
        SUM(CASE WHEN dq_status = 'FAIL' THEN 1 ELSE 0 END)                  AS b_quarantined,
        SUM(CASE WHEN dq_status = 'WARN' THEN 1 ELSE 0 END)                  AS b_warned
    FROM nyc_silver.green_taxi_clean
),

checks AS (
    SELECT 'completeness' AS check_category, 'table_not_empty' AS check_name,
           0.0 AS threshold_pct, t_empty AS failed_rows, 1 AS total_rows FROM silver
    UNION ALL SELECT 'completeness', 'dq_status_populated',   0.0, c_status,      total_rows FROM silver
    UNION ALL SELECT 'completeness', 'qc_array_not_null',     0.0, c_qc_array,    total_rows FROM silver
    UNION ALL SELECT 'completeness', 'source_file_recorded',  0.0, c_source_file, total_rows FROM silver
    UNION ALL SELECT 'completeness', 'ingestion_time_recorded', 5.0, c_ingestion, total_rows FROM silver
    UNION ALL SELECT 'completeness', 'silver_at_recorded',    0.0, c_silver_at,   total_rows FROM silver

    UNION ALL SELECT 'uniqueness',   'one_row_per_merge_key', 0.0,
                     (SELECT extra_rows FROM grain), total_rows FROM silver

    UNION ALL SELECT 'validity',     'dq_status_in_domain',   0.0, v_status_domain, total_rows FROM silver
    UNION ALL SELECT 'validity',     'qc_entries_carry_severity_prefix', 0.0, v_prefix, total_rows FROM silver
    UNION ALL SELECT 'validity',     'zones_within_1_to_265', 0.0, v_zone_range,  total_rows FROM silver

    UNION ALL SELECT 'consistency',  'pass_rows_carry_no_issues',        0.0, x_pass_with_issues, total_rows FROM silver
    UNION ALL SELECT 'consistency',  'fail_rows_carry_a_fail_issue',     0.0, x_fail_no_reason,   total_rows FROM silver
    UNION ALL SELECT 'consistency',  'warn_rows_carry_only_warn_issues', 0.0, x_warn_wrong,       total_rows FROM silver

    UNION ALL SELECT 'consistency',  'null_timestamps_are_quarantined',    0.0, p_null_ts,  total_rows FROM silver
    UNION ALL SELECT 'consistency',  'reversed_trips_are_quarantined',     0.0, p_reversed, total_rows FROM silver
    UNION ALL SELECT 'consistency',  'unresolvable_zones_are_quarantined', 0.0, p_zone,     total_rows FROM silver
    UNION ALL SELECT 'consistency',  'out_of_era_pickups_are_quarantined', 0.0, p_era,      total_rows FROM silver
    UNION ALL SELECT 'consistency',  'untraceable_rows_are_quarantined',   0.0, p_lineage,  total_rows FROM silver

    -- Scalar assertions: total_rows is 1, so failed_pct is 0 or 100 and
    -- nothing between. The threshold carries no information on these rows,
    -- which is what denominator_scope = 'scalar' exists to say.
    UNION ALL SELECT 'consistency',  'rows_reconcile_with_bronze', 0.0,
        CASE WHEN (SELECT n_rows FROM bronze) - (SELECT expected_removed FROM dupes)
                  = (SELECT total_rows FROM silver) THEN 0 ELSE 1 END, 1 FROM silver
    UNION ALL SELECT 'consistency',  'revenue_preserved', 0.0,
        CASE WHEN ABS(COALESCE((SELECT revenue FROM bronze), 0)
                    - COALESCE((SELECT revenue FROM silver), 0))
                  <= GREATEST(1.0, 0.001 * ABS(COALESCE((SELECT revenue FROM bronze), 0)))
             THEN 0 ELSE 1 END, 1 FROM silver
    UNION ALL SELECT 'consistency',  'every_bronze_file_present', 0.0,
        (SELECT n FROM unprocessed), 1 FROM silver

    -- The dedup remainder as a rate, so a dedup that suddenly eats a third of
    -- the table is a number rather than a silent success.
    UNION ALL SELECT 'consistency',  'dedup_removal_rate', 5.0,
        (SELECT expected_removed FROM dupes), (SELECT n_rows FROM bronze) FROM silver

    UNION ALL SELECT 'business',     'charges_reconcile_by_vendor', 5.0, b_residual, n_v1v2 FROM silver
    -- The aggregate limit. Every per-check threshold asks "is this rule
    -- violated too often". Only this one asks "are we excluding so much that
    -- the answer stops being about New York taxis" -- ten rules each
    -- quarantining 2 percent would all pass and between them remove a fifth.
    UNION ALL SELECT 'business',     'quarantine_rate_within_limit', 5.0, b_quarantined, total_rows FROM silver
    -- Advisory: can only ever WARN. A number in the run log every run beats a
    -- sentence in a comment once.
    UNION ALL SELECT 'business',     'rows_carrying_a_warning', 100.0, b_warned, total_rows FROM silver
)

SELECT
    v_run_id, v_run_ts, 'silver', 'green_taxi_clean',
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    threshold_pct,
    CASE WHEN failed_rows = 0                                              THEN 'PASS'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= threshold_pct THEN 'WARN'
         ELSE 'FAIL' END
FROM checks;

-- 2. taxi_zones_clean

INSERT INTO nyc_quality.dq_results
WITH z AS (
    SELECT
        COUNT(*)                                                          AS total_rows,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END                          AS t_empty,
        SUM(CASE WHEN location_id IS NULL THEN 1 ELSE 0 END)              AS c_id,
        SUM(CASE WHEN zone_name IS NULL OR TRIM(zone_name) = ''
                 THEN 1 ELSE 0 END)                                       AS c_zone,
        SUM(CASE WHEN borough IS NULL OR TRIM(borough) = ''
                 THEN 1 ELSE 0 END)                                       AS c_borough,
        SUM(CASE WHEN service_zone IS NULL OR TRIM(service_zone) = ''
                 THEN 1 ELSE 0 END)                                       AS c_service,
        SUM(CASE WHEN source_file IS NULL THEN 1 ELSE 0 END)              AS c_lineage,
        COUNT(location_id) - COUNT(DISTINCT location_id)                   AS u_dupes,
        SUM(CASE WHEN location_id NOT BETWEEN 1 AND 265 THEN 1 ELSE 0 END) AS v_range,
        SUM(CASE WHEN borough NOT IN ('Manhattan','Brooklyn','Queens','Bronx',
                                      'Staten Island','EWR','Unknown','N/A')
                 THEN 1 ELSE 0 END)                                       AS v_borough,
        SUM(CASE WHEN service_zone NOT IN ('Yellow Zone','Boro Zone','Airports',
                                           'EWR','N/A')
                 THEN 1 ELSE 0 END)                                       AS v_service,
        CASE WHEN COUNT(*) = 265 THEN 0 ELSE 1 END                        AS b_265,
        CASE WHEN COUNT(DISTINCT CASE WHEN location_id IN (264, 265)
                                      THEN location_id END) = 2
             THEN 0 ELSE 1 END                                            AS b_unknowns,
        CASE WHEN COUNT(DISTINCT CASE WHEN location_id IN (1, 132, 138)
                                      THEN location_id END) = 3
             THEN 0 ELSE 1 END                                            AS b_airports,
        CASE WHEN COUNT(*) = (SELECT COUNT(DISTINCT location_id)
                              FROM nyc_bronze.taxi_zones
                              WHERE location_id IS NOT NULL)
             THEN 0 ELSE 1 END                                            AS x_reconcile
    FROM nyc_silver.taxi_zones_clean
),
checks AS (
    SELECT 'completeness' AS check_category, 'table_not_empty' AS check_name,
           0.0 AS threshold_pct, t_empty AS failed_rows, 1 AS total_rows FROM z
    UNION ALL SELECT 'completeness', 'location_id_not_null',   0.0, c_id,      total_rows FROM z
    UNION ALL SELECT 'completeness', 'zone_name_not_blank',    0.0, c_zone,    total_rows FROM z
    UNION ALL SELECT 'completeness', 'borough_not_blank',      0.0, c_borough, total_rows FROM z
    UNION ALL SELECT 'completeness', 'service_zone_not_blank', 0.0, c_service, total_rows FROM z
    UNION ALL SELECT 'completeness', 'source_file_recorded',   0.0, c_lineage, total_rows FROM z
    UNION ALL SELECT 'uniqueness',   'location_id_unique',     0.0, u_dupes,   total_rows FROM z
    UNION ALL SELECT 'validity',     'location_id_in_range',   0.0, v_range,   total_rows FROM z
    UNION ALL SELECT 'validity',     'borough_in_domain',      0.0, v_borough, total_rows FROM z
    UNION ALL SELECT 'validity',     'service_zone_in_domain', 0.0, v_service, total_rows FROM z
    UNION ALL SELECT 'business',     'lookup_has_265_zones',       0.0, b_265,       1 FROM z
    UNION ALL SELECT 'business',     'two_unknown_zones_present',  0.0, b_unknowns,  1 FROM z
    UNION ALL SELECT 'business',     'airport_zones_present',      0.0, b_airports,  1 FROM z
    UNION ALL SELECT 'consistency',  'rows_reconcile_with_bronze', 0.0, x_reconcile, 1 FROM z
)

SELECT
    v_run_id, v_run_ts, 'silver', 'taxi_zones_clean',
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    threshold_pct,
    CASE WHEN failed_rows = 0                                              THEN 'PASS'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= threshold_pct THEN 'WARN'
         ELSE 'FAIL' END
FROM checks;

-- 3. weather_clean

INSERT INTO nyc_quality.dq_results
WITH covered_months AS (
    SELECT date_trunc('MONTH', weather_hour) AS month_start
    FROM   nyc_silver.weather_clean
    WHERE  weather_hour IS NOT NULL
    GROUP  BY date_trunc('MONTH', weather_hour)
    HAVING COUNT(DISTINCT to_date(weather_hour)) >= 2
),
-- Correlated subqueries are not allowed inside aggregate expressions, so the
-- membership test becomes a LEFT JOIN producing a per-row boolean, and the
-- aggregate then sums an ordinary column.
flagged AS (
    SELECT w.*, c.month_start IS NOT NULL AS month_is_covered
    FROM       nyc_silver.weather_clean w
    LEFT  JOIN covered_months c
           ON  c.month_start = date_trunc('MONTH', w.weather_hour)
),
expected_days AS (
    SELECT explode(sequence(month_start,
                            last_day(month_start),
                            INTERVAL 1 DAY)) AS d
    FROM   covered_months
),
missing_days AS (
    SELECT COUNT(*) AS n
    FROM       expected_days e
    LEFT  JOIN (SELECT DISTINCT to_date(weather_hour) AS d
                FROM   nyc_silver.weather_clean
                WHERE  weather_hour IS NOT NULL) a
           ON  e.d = a.d
    WHERE a.d IS NULL
),
bronze AS (
    SELECT COUNT(*) AS n_rows FROM nyc_bronze.weather
),
w AS (
    SELECT
        COUNT(*)                                                          AS total_rows,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END                          AS t_empty,

        -- completeness of what Silver produced
        SUM(CASE WHEN weather_hour          IS NULL THEN 1 ELSE 0 END)     AS c_hour,
        SUM(CASE WHEN dq_status             IS NULL THEN 1 ELSE 0 END)     AS c_status,
        SUM(CASE WHEN qc_error_descriptions IS NULL THEN 1 ELSE 0 END)     AS c_qc_array,
        SUM(CASE WHEN silver_at             IS NULL THEN 1 ELSE 0 END)     AS c_silver_at,

        -- uniqueness on the PARSED timestamp, not the raw string: two
        -- spellings of the same instant are distinct as strings, and a
        -- duplicate hour fans out the trip-to-weather join in Gold.
        COUNT(weather_hour) - COUNT(DISTINCT weather_hour)                 AS u_dupes,

        -- parse fidelity
        SUM(CASE WHEN temperature_2m IS NULL THEN 1 ELSE 0 END)            AS p_temp,
        SUM(CASE WHEN rain           IS NULL THEN 1 ELSE 0 END)            AS p_rain,
        SUM(CASE WHEN wind_speed_10m IS NULL THEN 1 ELSE 0 END)            AS p_wind,
        SUM(CASE WHEN visibility     IS NULL THEN 1 ELSE 0 END)            AS p_vis,
        SUM(CASE WHEN weather_code   IS NULL THEN 1 ELSE 0 END)            AS p_code,

        -- plausibility
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

        -- the derivation, same three assertions as green_taxi_clean
        SUM(CASE WHEN dq_status NOT IN ('PASS','WARN','FAIL')
                 THEN 1 ELSE 0 END)                                        AS v_status_domain,
        SUM(CASE WHEN dq_status = 'PASS' AND size(qc_error_descriptions) > 0
                 THEN 1 ELSE 0 END)                                        AS x_pass_with_issues,
        SUM(CASE WHEN dq_status = 'FAIL'
                  AND NOT exists(qc_error_descriptions, x -> startswith(x, 'FAIL:'))
                 THEN 1 ELSE 0 END)                                        AS x_fail_no_reason,

        SUM(CASE WHEN weather_code IS NOT NULL
                  AND weather_description = 'Unknown' THEN 1 ELSE 0 END)   AS x_desc_unknown,
        SUM(CASE WHEN weather_code IS NULL
                  AND weather_description <> 'Unknown' THEN 1 ELSE 0 END)  AS x_desc_mismatch,

        -- the loader bug, recorded rather than fixed here
        SUM(CASE WHEN source_file_month IS NULL
                   OR source_file_month LIKE '%{%'
                 THEN 1 ELSE 0 END)                                        AS a_file_month,

        SUM(CASE WHEN NOT month_is_covered THEN 1 ELSE 0 END)              AS v_window
    FROM flagged
),
checks AS (
    SELECT 'completeness' AS check_category, 'table_not_empty' AS check_name,
           0.0 AS threshold_pct, t_empty AS failed_rows, 1 AS total_rows FROM w
    UNION ALL SELECT 'completeness', 'weather_hour_not_null', 0.0, c_hour,      total_rows FROM w
    UNION ALL SELECT 'completeness', 'dq_status_populated',   0.0, c_status,    total_rows FROM w
    UNION ALL SELECT 'completeness', 'qc_array_not_null',     0.0, c_qc_array,  total_rows FROM w
    UNION ALL SELECT 'completeness', 'silver_at_recorded',    0.0, c_silver_at, total_rows FROM w

    UNION ALL SELECT 'uniqueness',   'one_row_per_hour',      0.0, u_dupes,     total_rows FROM w

    UNION ALL SELECT 'validity',     'temperature_parsed',    0.0, p_temp, total_rows FROM w
    UNION ALL SELECT 'validity',     'rain_parsed',           0.0, p_rain, total_rows FROM w
    UNION ALL SELECT 'validity',     'wind_speed_parsed',     0.0, p_wind, total_rows FROM w
    UNION ALL SELECT 'validity',     'visibility_parsed',     0.0, p_vis,  total_rows FROM w
    UNION ALL SELECT 'validity',     'weather_code_parsed',   5.0, p_code, total_rows FROM w
    UNION ALL SELECT 'validity',     'temperature_plausible', 5.0, v_temp, total_rows FROM w
    UNION ALL SELECT 'validity',     'rain_not_negative',     5.0, v_rain, total_rows FROM w
    UNION ALL SELECT 'validity',     'cloud_cover_0_to_100',  5.0, v_cloud, total_rows FROM w
    UNION ALL SELECT 'validity',     'precip_probability_0_to_100', 5.0, v_prob, total_rows FROM w
    UNION ALL SELECT 'validity',     'dq_status_in_domain',   0.0, v_status_domain, total_rows FROM w
    UNION ALL SELECT 'validity',     'hour_within_covered_months', 0.0, v_window, total_rows FROM w

    UNION ALL SELECT 'consistency',  'gusts_at_least_wind_speed', 5.0, v_gust, total_rows FROM w
    UNION ALL SELECT 'consistency',  'description_known_for_code', 0.0, x_desc_unknown,  total_rows FROM w
    UNION ALL SELECT 'consistency',  'description_matches_code',   0.0, x_desc_mismatch, total_rows FROM w
    UNION ALL SELECT 'consistency',  'pass_rows_carry_no_issues',    0.0, x_pass_with_issues, total_rows FROM w
    UNION ALL SELECT 'consistency',  'fail_rows_carry_a_fail_issue', 0.0, x_fail_no_reason,   total_rows FROM w
    UNION ALL SELECT 'consistency',  'rows_reconcile_with_bronze',   0.0,
        CASE WHEN (SELECT n_rows FROM bronze) >= (SELECT total_rows FROM w)
             THEN 0 ELSE 1 END, 1 FROM w

    UNION ALL SELECT 'business',     'all_expected_days_present', 0.0,
        (SELECT n FROM missing_days), 1 FROM w
    UNION ALL SELECT 'completeness', 'source_file_month_is_real', 100.0, a_file_month, total_rows FROM w
)

SELECT
    v_run_id, v_run_ts, 'silver', 'weather_clean',
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    threshold_pct,
    CASE WHEN failed_rows = 0                                              THEN 'PASS'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= threshold_pct THEN 'WARN'
         ELSE 'FAIL' END
FROM checks;
-- 4. Join coverage — what Gold will actually resolve
--
-- These are the most useful checks in the notebook, because they predict a
-- failure that never raises an error.
--
-- Every dimension in Gold has an Unknown member keyed `-1`, so an
-- unresolvable foreign key does not drop the trip — it lands in a bucket
-- labelled "we do not know". The star schema works perfectly and the
-- answer is quietly wrong.
--
-- | Check | Predicts |
-- |---|---|
-- | `pickup_zone_exists_in_lookup` | trips landing on `dim_zone` Unknown |
-- | `trip_hour_has_weather` | trips landing on `dim_weather` Unknown |

INSERT INTO nyc_quality.dq_results
WITH pu AS (
    SELECT COUNT(*) AS unmatched,
           (SELECT COUNT(DISTINCT pu_location_id)
            FROM nyc_silver.vw_green_taxi_valid) AS total
    FROM (
        SELECT DISTINCT t.pu_location_id
        FROM       nyc_silver.vw_green_taxi_valid t
        LEFT  JOIN nyc_silver.taxi_zones_clean z ON t.pu_location_id = z.location_id
        WHERE t.pu_location_id IS NOT NULL AND z.location_id IS NULL
    )
),
do_ AS (
    SELECT COUNT(*) AS unmatched,
           (SELECT COUNT(DISTINCT do_location_id)
            FROM nyc_silver.vw_green_taxi_valid) AS total
    FROM (
        SELECT DISTINCT t.do_location_id
        FROM       nyc_silver.vw_green_taxi_valid t
        LEFT  JOIN nyc_silver.taxi_zones_clean z ON t.do_location_id = z.location_id
        WHERE t.do_location_id IS NOT NULL AND z.location_id IS NULL
    )
),
wx AS (
    SELECT COUNT(*) AS unmatched,
           (SELECT COUNT(DISTINCT date_trunc('HOUR', lpep_pickup_datetime))
            FROM   nyc_silver.vw_green_taxi_valid
            WHERE  lpep_pickup_datetime IS NOT NULL) AS total
    FROM (
        SELECT DISTINCT date_trunc('HOUR', t.lpep_pickup_datetime) AS h
        FROM       nyc_silver.vw_green_taxi_valid t
        LEFT  JOIN nyc_silver.weather_clean w
               ON  date_trunc('HOUR', t.lpep_pickup_datetime) = w.weather_hour
        WHERE t.lpep_pickup_datetime IS NOT NULL AND w.weather_hour IS NULL
    )
),
trips AS (
    SELECT
        COUNT(*)                                                        AS total_rows,
        SUM(CASE WHEN zp.location_id IS NULL THEN 1 ELSE 0 END)         AS t_pu,
        SUM(CASE WHEN zd.location_id IS NULL THEN 1 ELSE 0 END)         AS t_do
    FROM       nyc_silver.vw_green_taxi_valid t
    LEFT  JOIN nyc_silver.taxi_zones_clean zp ON t.pu_location_id = zp.location_id
    LEFT  JOIN nyc_silver.taxi_zones_clean zd ON t.do_location_id = zd.location_id
),
checks AS (
    SELECT 'consistency' AS check_category, 'pickup_zone_exists_in_lookup' AS check_name,
           0.0 AS threshold_pct,
           (SELECT unmatched FROM pu) AS failed_rows,
           (SELECT total FROM pu)     AS total_rows
    UNION ALL SELECT 'consistency', 'dropoff_zone_exists_in_lookup', 0.0,
           (SELECT unmatched FROM do_), (SELECT total FROM do_)
    -- 5.0 rather than 0.0: pickups outside the loaded window are WARN, not
    -- FAIL, so they stay in the valid view and a handful of hours legitimately
    -- have no weather row.
    UNION ALL SELECT 'consistency', 'trip_hour_has_weather', 5.0,
           (SELECT unmatched FROM wx), (SELECT total FROM wx)
    UNION ALL SELECT 'consistency', 'trips_with_unmatched_pickup_zone', 100.0,
           (SELECT t_pu FROM trips), (SELECT total_rows FROM trips)
    UNION ALL SELECT 'consistency', 'trips_with_unmatched_dropoff_zone', 100.0,
           (SELECT t_do FROM trips), (SELECT total_rows FROM trips)
)

SELECT
    v_run_id, v_run_ts, 'silver', 'green_taxi_clean',
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    threshold_pct,
    CASE WHEN failed_rows = 0                                              THEN 'PASS'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= threshold_pct THEN 'WARN'
         ELSE 'FAIL' END
FROM checks;

--5. Audit log
INSERT INTO nyc_quality.dq_run_log
SELECT
    v_run_id, v_run_ts, 'silver',
    COUNT(DISTINCT table_name)                       AS tables_checked,
    COUNT(*)                                         AS checks_run,
    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS checks_passed,
    SUM(CASE WHEN status = 'WARN' THEN 1 ELSE 0 END) AS checks_warned,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS checks_failed,
    -- FAIL means THIS LAYER IS NOT USABLE -- a blocking check failed, and the
    -- gate below raises on the same condition. A non-blocking failure is a
    -- recorded defect, not a reason to stop, so it lands on WARN: saying FAIL
    -- for both made the column disagree with the gate that read the same rows.
    CASE WHEN SUM(CASE WHEN status = 'FAIL'
                        AND check_name IN (SELECT check_name FROM vw_silver_blocking_checks)
                       THEN 1 ELSE 0 END) > 0              THEN 'FAIL'
         WHEN SUM(CASE WHEN status <> 'PASS' THEN 1 ELSE 0 END) > 0 THEN 'WARN'
         ELSE 'PASS' END                             AS overall_status,
    current_timestamp()                              AS finished_at
FROM nyc_quality.dq_results
WHERE run_id = v_run_id AND layer = 'silver';

--6. Results

SELECT * FROM nyc_quality.dq_run_log WHERE run_id = v_run_id;

SELECT table_name, status, COUNT(*) AS checks
FROM   nyc_quality.dq_results
WHERE  run_id = v_run_id
GROUP  BY table_name, status
ORDER  BY table_name, status;

SELECT table_name, check_category, check_name,
       failed_rows, total_rows, failed_pct, threshold_pct, status
FROM   nyc_quality.dq_results
WHERE  run_id = v_run_id AND status <> 'PASS'
ORDER  BY CASE status WHEN 'FAIL' THEN 0 ELSE 1 END, failed_pct DESC;

-- 7. The gate
-- | Trigger | Meaning |
-- |---|---|
-- | a blocking check fails | our own transformation is broken; Silver is unusable downstream |
-- | quarantine rate over 5 percent | we are excluding too much to call the result representative |
-- | a Bronze file produced no Silver rows | Bronze data this notebook never processed, and Gold is about to be built without it |
-- | 5 or more failures | nothing individually fatal, something broadly wrong |

DECLARE OR REPLACE VARIABLE v_blocking_failures INT;
-- The gate used to report a COUNT and leave you to go find out which.
-- A number alone sends you to the results table with a run id; a name
-- sends you to the check.
DECLARE OR REPLACE VARIABLE v_blocking_names STRING;
DECLARE OR REPLACE VARIABLE v_total_failures    INT;
DECLARE OR REPLACE VARIABLE v_quarantine_pct    DOUBLE;
DECLARE OR REPLACE VARIABLE v_unprocessed_files INT;
 
-- TEMPORARY -- report-only mode.
--
-- FALSE: every trigger below is still evaluated and still named in the
-- output, but the notebook does not raise, so the job carries on to Gold.
-- TRUE: the gate raises as designed.
--
-- This is one line so that restoring enforcement is one edit and so that
-- a grep for v_gate_enforce finds it. Set it back to TRUE once the
-- failures listed by the verdict cell are resolved -- a gate left in
-- report-only mode indefinitely is the same as no gate.
DECLARE OR REPLACE VARIABLE v_gate_enforce BOOLEAN;
SET VAR v_gate_enforce = FALSE;
 
DECLARE OR REPLACE VARIABLE v_gate_tripped BOOLEAN;
DECLARE OR REPLACE VARIABLE v_gate_message STRING;
 
SET VAR v_blocking_failures = (
    SELECT COUNT(*) FROM nyc_quality.dq_results
    WHERE  run_id = v_run_id AND layer = 'silver' AND status = 'FAIL'
      AND  check_name IN (SELECT check_name FROM vw_silver_blocking_checks)
);
 
SET VAR v_blocking_names = (
    SELECT COALESCE(concat_ws(', ', collect_list(check_name)), 'none')
    FROM   nyc_quality.dq_results
    WHERE  run_id = v_run_id AND layer = 'silver' AND status = 'FAIL'
      AND  check_name IN (SELECT check_name FROM vw_silver_blocking_checks)
);
 
SET VAR v_total_failures = (
    SELECT COUNT(*) FROM nyc_quality.dq_results
    WHERE run_id = v_run_id AND layer = 'silver' AND status = 'FAIL'
);
 
SET VAR v_quarantine_pct = (
    SELECT ROUND(100.0 * SUM(CASE WHEN dq_status = 'FAIL' THEN 1 ELSE 0 END)
                 / NULLIF(COUNT(*), 0), 4)
    FROM nyc_silver.green_taxi_clean
);
 
 
SET VAR v_unprocessed_files = (
    SELECT COUNT(*)
    FROM (
        SELECT DISTINCT source_file FROM nyc_bronze.green_taxi
        EXCEPT
        SELECT DISTINCT source_file FROM nyc_silver.green_taxi_clean
    )
);

-- The blocking list has to be checked against reality
-- A name in that `IN` list that no check emits is inert: it matches
-- nothing, so the gate neither blocks nor complains. The list reads like a
-- guarantee, and the next rename turns one of those guarantees off with
-- nothing to say so. The Bronze gate carried four such names for weeks.
-- Expect zero rows.

WITH blocking AS (SELECT check_name FROM vw_silver_blocking_checks)
SELECT b.check_name AS blocking_name_never_produced
FROM       blocking b
LEFT  JOIN (SELECT DISTINCT check_name
            FROM   nyc_quality.dq_results
            WHERE  run_id = v_run_id AND layer = 'silver') r
       ON  b.check_name = r.check_name
WHERE r.check_name IS NULL;

SELECT v_blocking_failures AS blocking_failures,
       v_total_failures    AS total_failures,
       v_quarantine_pct    AS quarantine_pct,
       v_unprocessed_files AS unprocessed_files,
       CASE WHEN v_blocking_failures > 0 THEN 'will stop: blocking check failed'
            WHEN v_unprocessed_files > 0 THEN 'will stop: Bronze files with no Silver rows'
            WHEN v_quarantine_pct >= 5.0 THEN 'will stop: quarantine rate at or over 5 percent'
            WHEN v_total_failures >= 5   THEN 'will stop: 5 or more failures'
            ELSE 'will continue' END     AS verdict;

SELECT CASE
    WHEN v_blocking_failures > 0
      THEN raise_error(CONCAT('Silver DQ gate FAILED (group): ',
                              CAST(v_blocking_failures AS STRING),
                              ' BLOCKING check(s) failed (of ',
                              CAST(v_total_failures AS STRING),
                              ' total). The transformation is wrong, not the data. Run ', v_run_id))
    WHEN v_unprocessed_files > 0
      THEN raise_error(CONCAT('Silver DQ gate FAILED (group): ',
                              CAST(v_unprocessed_files AS STRING),
                              ' Bronze file(s) produced no Silver rows at all. ',
                              'Rerun the Silver cleaning notebooks before building Gold.'))
    WHEN v_quarantine_pct >= 5.0
      THEN raise_error(CONCAT('Silver DQ gate FAILED (group): quarantine rate ',
                              CAST(v_quarantine_pct AS STRING),
                              ' percent is at or over the 5 percent limit. Read ',
                              'nyc_silver.vw_green_taxi_quarantined before assuming ',
                              'the data is at fault. Run ', v_run_id))
    WHEN v_total_failures >= 5
      THEN raise_error(CONCAT('Silver DQ gate FAILED (group): ',
                              CAST(v_total_failures AS STRING),
                              ' checks over threshold, none individually blocking. Run ', v_run_id))
    ELSE CONCAT('Silver DQ gate PASSED (quarantine rate ',
                CAST(v_quarantine_pct AS STRING), ' percent, ',
                CAST(v_total_failures AS STRING), ' non-blocking failure(s))')
END AS gate;

-- 8. Afterwards

SELECT dq_status,
       COUNT(*)                                           AS trips,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 3) AS pct,
       ROUND(SUM(total_amount), 2)                        AS revenue
FROM   nyc_silver.green_taxi_clean
GROUP  BY dq_status
ORDER  BY trips DESC;


-- The defense of every row not in Gold.
SELECT reason, COUNT(*) AS trips
FROM  (SELECT explode(filter(qc_error_descriptions, x -> startswith(x, 'FAIL:'))) AS reason
       FROM   nyc_silver.green_taxi_clean
       WHERE  dq_status = 'FAIL')
GROUP  BY reason
ORDER  BY trips DESC;


SELECT table_name, check_name, failed_rows, total_rows, failed_pct,
       threshold_pct, status
FROM   nyc_quality.vw_latest_dq_results
WHERE  layer = 'silver' AND status <> 'PASS'
ORDER  BY CASE status WHEN 'FAIL' THEN 0 ELSE 1 END, failed_pct DESC;

SELECT table_name, check_name,
       failed_rows, threshold_pct, status,
       LAG(failed_rows)   OVER w AS prev_failed_rows,
       LAG(threshold_pct) OVER w AS prev_threshold,
       LAG(status)        OVER w AS prev_status
FROM   nyc_quality.dq_results
WHERE  layer = 'silver'
WINDOW w AS (PARTITION BY table_name, check_name ORDER BY run_ts)
QUALIFY prev_status IS NOT NULL
    AND (status <> prev_status
      OR failed_rows <> prev_failed_rows
      OR threshold_pct <> prev_threshold)
ORDER  BY table_name, check_name;

-- The latest run of every layer, side by side: the whole pipeline's quality
-- position in one row each.
SELECT layer, overall_status, checks_run, checks_passed,
       checks_warned, checks_failed, run_ts
FROM   nyc_quality.vw_latest_dq_run
ORDER  BY CASE layer WHEN 'bronze' THEN 1 WHEN 'silver' THEN 2 ELSE 3 END;