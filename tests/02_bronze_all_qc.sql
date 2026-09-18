-- Data Quality — Bronze, group pipeline
--
-- Covers all three group Bronze tables in **one run**: `green_taxi`,
-- `taxi_zones`, `weather`, plus two cross-cutting sections.

-- Sections
-- | § | Covers | Checks |
-- |---|---|---|
-- | 1 | `green_taxi` | 40 |
-- | 2 | `taxi_zones` | 14 |
-- | 3 | `weather` | 36 |
-- | 4 | referential integrity, trips → zones | 4 |
-- | 5 | load fidelity, `green_taxi` vs landed Parquet | 21 |
-- | | **total** | **115** |
--
-- Then 6. Audit log · 7. Results · 8. Gate · 9. Afterwards.

-- Every notebook in this project sets this. The taxi timestamps are naive
-- wall-clock New York time, so New York is the project's canonical zone and
-- every other source is read into it. A named zone, not a fixed -05:00
-- offset: the name is what makes the 8 March DST change handle itself.
SET TIME ZONE 'America/New_York';
USE CATALOG `nyc-mobility`;


DECLARE OR REPLACE VARIABLE v_run_id STRING;
DECLARE OR REPLACE VARIABLE v_run_ts TIMESTAMP;

SET VAR v_run_id = uuid();

SET VAR v_run_ts = current_timestamp();

SELECT v_run_id AS run_id, v_run_ts AS run_ts;


-- Each run gets a new UUID, so previous rows from the same run will not exist.
-- For now, dq_results is append-only and each execution is kept in the history.
-- If v_run_id is replaced with a stable job run ID, the DELETE can support reruns safely.
-- Use vw_latest_dq_results to view the most recent results.
DELETE FROM nyc_quality.dq_results
WHERE run_id = v_run_id AND layer = 'bronze'
  AND table_name IN ('green_taxi', 'taxi_zones', 'weather');

DELETE FROM nyc_quality.dq_run_log WHERE run_id = v_run_id AND layer = 'bronze';

-- 1. green_taxi
-- ## Thresholds 
-- | Value | Meaning |
-- |---|---|
-- | **`0.0`** | must never happen: one occurrence corrupts an aggregate, a join or the grain — or the check is scalar, where a percentage is 0 or 100 and nothing between |
-- | **`5.0`** | the source is known to be imperfect and this much is tolerated |
-- | **`100.0`** | advisory: reported every run, can only ever WARN, never gates |

-- A 5% threshold allows up to 6,668 violations out of 133,367 trips
-- This is acceptable because the old thresholds mainly matched the current data
-- rather than reflecting actual business risk.
--
-- Why not use 5% for every check?
-- Percentage thresholds work when the impact grows with the number of bad rows.
-- For example, a null passenger_count mainly affects statistics for that trip.

-- Why not 5% for every check?
-- Some issues can cause much larger downstream errors even at very low rates.
-- For example, one duplicate location_id can duplicate joined Gold records,
-- while a few extreme trip distances can heavily distort averages.

-- Why not use 0% for every check?
-- Some flagged values are valid business cases.
-- For example, negative fares can represent No Charge or Dispute trips.
-- A 0% threshold would make valid data fail repeatedly and reduce trust in the checks.

-- Which basis applies to which check
--  `dq_rules.rationale` carries the `[structural]` /
-- `[tolerated]` / `[provisional]` tag per rule, which is the one place it
-- can be read next to the threshold it explains. Two copies of that
-- judgement would drift.


-- ## The three vendors report fares differently
-- One accounting identity across all vendors fails on about a quarter of
-- rows — not because the data is bad but because each provider uses a
-- different convention. So: one check per vendor, each measured against
-- that vendor's own row count.
--
-- | Vendor | `total_amount` contains | Observed |
-- |---|---|---|
-- | 2 — Curb | everything, including `cbd_congestion_fee` | 1.33% |
-- | 1 — Creative Mobile | `fare + extra + mta_tax + tip + tolls`. The three surcharges are itemised but not added in | 1.63% |
-- | 6 — Myle | the real charge; `fare_amount` is a placeholder | n/a |
--
-- Those percentages are a **baseline to compare future runs against**, not
-- the basis for either threshold. Both checks sit at the policy 5.0.

-- Observed, not documented
-- TLC does not publish vendor-specific accounting rules.
-- These patterns were identified from profiling the March–May 2026 data only,
-- so they should be treated as dataset findings, not permanent rules.
-- Vendor behavior may change in future loads, so vendor-specific checks are kept.
-- Each check uses its own vendor row count as the denominator to avoid diluting failures.

-- VendorID 6 (Myle)
-- Myle has no values for the six dispatch fields across all rows, so these
-- nulls are treated as structural rather than missing data.
-- Completeness checks exclude Myle, while separate checks verify this pattern.
-- NULL vendor_id values are handled by completeness checks, not validity checks,
-- to avoid reporting the same issue twice.


INSERT INTO nyc_quality.dq_results
WITH metrics AS (
    SELECT
        COUNT(*)                                                                    AS total_rows,

-- Explicitly fail an empty load once, instead of producing many misleading
-- downstream failures from NULL aggregate results.
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END                                    AS t_empty,
        -- completeness
        SUM(CASE WHEN lpep_pickup_datetime  IS NULL THEN 1 ELSE 0 END)              AS c_pickup_ts,
        SUM(CASE WHEN lpep_dropoff_datetime IS NULL THEN 1 ELSE 0 END)              AS c_dropoff_ts,
        SUM(CASE WHEN pu_location_id        IS NULL THEN 1 ELSE 0 END)              AS c_pu,
        SUM(CASE WHEN do_location_id        IS NULL THEN 1 ELSE 0 END)              AS c_do,
        SUM(CASE WHEN trip_distance         IS NULL THEN 1 ELSE 0 END)              AS c_distance,
        SUM(CASE WHEN fare_amount           IS NULL THEN 1 ELSE 0 END)              AS c_fare,
        SUM(CASE WHEN total_amount          IS NULL THEN 1 ELSE 0 END)              AS c_total,
        SUM(CASE WHEN passenger_count IS NULL AND vendor_id <> 6 THEN 1 ELSE 0 END) AS c_passengers,
        SUM(CASE WHEN vendor_id             IS NULL THEN 1 ELSE 0 END)              AS c_vendor,
        SUM(CASE WHEN source_file           IS NULL THEN 1 ELSE 0 END)              AS c_lineage,
        SUM(CASE WHEN ingestion_time        IS NULL THEN 1 ELSE 0 END)              AS c_ingested,

        -- Validity: trip date vs. source file month
        -- Expected month is derived from source_file (e.g. 2026-03), avoiding hardcoded lists
        -- and catching trips stored in the wrong monthly file.
        -- Missing or malformed source_file values are checked separately.
        SUM(CASE WHEN regexp_extract(source_file, '([0-9]{4}-[0-9]{2})', 1) <> ''
                  AND date_format(lpep_pickup_datetime, 'yyyy-MM')
                      <> regexp_extract(source_file, '([0-9]{4}-[0-9]{2})', 1)
                 THEN 1 ELSE 0 END)                                                 AS v_window,
        SUM(CASE WHEN trip_distance   < 0 THEN 1 ELSE 0 END)                        AS v_distance_neg,
        SUM(CASE WHEN fare_amount     < 0 THEN 1 ELSE 0 END)                        AS v_fare_neg,
        SUM(CASE WHEN total_amount    < 0 THEN 1 ELSE 0 END)                        AS v_total_neg,
        SUM(CASE WHEN passenger_count < 0 THEN 1 ELSE 0 END)                        AS v_passengers_neg,
        SUM(CASE WHEN passenger_count > 9 THEN 1 ELSE 0 END)                        AS v_passengers_high,
        SUM(CASE WHEN pu_location_id NOT BETWEEN 1 AND 265 THEN 1 ELSE 0 END)       AS v_pu_range,
        SUM(CASE WHEN do_location_id NOT BETWEEN 1 AND 265 THEN 1 ELSE 0 END)       AS v_do_range,

        -- An explicit 0 is a different problem from a NULL: the meter recorded
        -- a value and that value was "nobody".
        SUM(CASE WHEN passenger_count = 0 THEN 1 ELSE 0 END)                        AS v_passengers_zero,

        -- Trip distance plausibility
        -- One 111,005-mile trip was observed, while p99.9 is about 31 miles.
        -- A 200-mile cutoff therefore flags clear outliers without affecting normal trips.

        -- Named trip_distance_plausible because failed_rows counts violations, and the
        -- cutoff may change later without requiring a rename.

        -- Threshold is 5%, not 0%, because >200 miles is implausible, not impossible.
        -- A 0% threshold would fail every run on the existing outliers and become noise.

        --A 5% threshold still catches major issues such as unit-conversion errors.
        --Smaller increases are better monitored through trend checks.
        SUM(CASE WHEN trip_distance > 200 THEN 1 ELSE 0 END)                        AS v_distance_absurd,
        SUM(CASE WHEN vendor_id          NOT IN (1,2,6)             THEN 1 ELSE 0 END) AS v_vendor,
        SUM(CASE WHEN ratecode_id        NOT IN (1,2,3,4,5,6,99)    THEN 1 ELSE 0 END) AS v_ratecode,
        SUM(CASE WHEN payment_type       NOT IN (0,1,2,3,4,5,6)     THEN 1 ELSE 0 END) AS v_payment,
        SUM(CASE WHEN trip_type          NOT IN (1,2)               THEN 1 ELSE 0 END) AS v_trip_type,
        SUM(CASE WHEN store_and_fwd_flag NOT IN ('Y','N')           THEN 1 ELSE 0 END) AS v_sf_flag,
        -- consistency
        SUM(CASE WHEN lpep_dropoff_datetime <  lpep_pickup_datetime THEN 1 ELSE 0 END) AS x_time_order,
        SUM(CASE WHEN lpep_dropoff_datetime =  lpep_pickup_datetime THEN 1 ELSE 0 END) AS x_zero_duration,
        SUM(CASE WHEN timestampdiff(SECOND, lpep_pickup_datetime, lpep_dropoff_datetime)
                      > 86400 THEN 1 ELSE 0 END)                                       AS x_over_24h,
        SUM(CASE WHEN vendor_id = 6 AND passenger_count IS NOT NULL
                  THEN 1 ELSE 0 END)                                                   AS x_myle_unexpected,

        -- The six dispatch fields are null as a SET, never individually.
        SUM(CASE WHEN (CASE WHEN passenger_count      IS NULL THEN 1 ELSE 0 END
                     + CASE WHEN ratecode_id          IS NULL THEN 1 ELSE 0 END
                     + CASE WHEN payment_type         IS NULL THEN 1 ELSE 0 END
                     + CASE WHEN trip_type            IS NULL THEN 1 ELSE 0 END
                     + CASE WHEN store_and_fwd_flag   IS NULL THEN 1 ELSE 0 END
                     + CASE WHEN congestion_surcharge IS NULL THEN 1 ELSE 0 END)
                      NOT IN (0, 6) THEN 1 ELSE 0 END)                                 AS x_dispatch_partial,

        -- business: one charge identity per vendor.
        SUM(CASE WHEN vendor_id = 2
                  AND ABS(total_amount - (
                          COALESCE(fare_amount, 0) + COALESCE(extra, 0)
                        + COALESCE(mta_tax, 0) + COALESCE(tip_amount, 0)
                        + COALESCE(tolls_amount, 0) + COALESCE(improvement_surcharge, 0)
                        + COALESCE(congestion_surcharge, 0)
                        + COALESCE(cbd_congestion_fee, 0))) > 0.01
                  THEN 1 ELSE 0 END)                                                   AS b_total_mismatch_v2,
        SUM(CASE WHEN vendor_id = 2 THEN 1 ELSE 0 END)                                 AS n_vendor2,
        -- Vendor 1 total_amount includes fare, extra, MTA tax, tip, and tolls,
        -- but excludes the three separately reported surcharges.
        -- It is therefore more than the metered fare, but not the full passenger charge.
        SUM(CASE WHEN vendor_id = 1
                  AND ABS(total_amount - (
                          COALESCE(fare_amount, 0) + COALESCE(extra, 0)
                        + COALESCE(mta_tax, 0) + COALESCE(tip_amount, 0)
                        + COALESCE(tolls_amount, 0))) > 0.01
                  THEN 1 ELSE 0 END)                                                   AS b_total_mismatch_v1,
        SUM(CASE WHEN vendor_id = 1 THEN 1 ELSE 0 END)                                 AS n_vendor1,

        -- Vendor 6 fare_amount stays near 2.75 across all distance bands, while
        -- total_amount increases with distance. It is therefore not a metered fare.
        -- A cutoff of 10 is above the observed maximum (9) but well below the average total (~29).
        SUM(CASE WHEN vendor_id = 6 AND fare_amount > 10 THEN 1 ELSE 0 END)            AS b_myle_fare_real,
        SUM(CASE WHEN vendor_id = 6 THEN 1 ELSE 0 END)                                 AS n_vendor6,

        -- Round fares on trips that barely happened: 300 appears 22 times at
        -- ~3 seconds and zero distance, plus groups at 250, 200, 160, 120.
        -- Invisible inside fare_implies_some_distance, so it needs its own check.
        SUM(CASE WHEN fare_amount > 100 AND trip_distance = 0
                  AND timestampdiff(SECOND, lpep_pickup_datetime,
                                    lpep_dropoff_datetime) < 60
                  THEN 1 ELSE 0 END)                                                   AS b_fare_implausible,
        SUM(CASE WHEN payment_type = 2 AND tip_amount > 0 THEN 1 ELSE 0 END)           AS b_cash_tip,
        SUM(CASE WHEN fare_amount > 0 AND trip_distance = 0 THEN 1 ELSE 0 END)         AS b_fare_no_distance,
        SUM(CASE WHEN try_divide(trip_distance,
                                 timestampdiff(SECOND, lpep_pickup_datetime,
                                               lpep_dropoff_datetime) / 3600.0) > 100
                  THEN 1 ELSE 0 END)                                                   AS b_impossible_speed
    FROM nyc_bronze.green_taxi
),

-- Duplicates are counted separately with GROUP BY and returned through a scalar subquery.
-- md5(to_json(struct(...))) fingerprints the row without listing every column.
-- Provenance columns are excluded so identical trips from different files still count as duplicates.
dupes AS (
    SELECT COUNT(*) - COUNT(DISTINCT row_fingerprint) AS duplicate_rows
    FROM (
        SELECT md5(to_json(struct(* EXCEPT (source_file, ingestion_time)))) AS row_fingerprint
        FROM nyc_bronze.green_taxi
    )
),
checks AS (
    SELECT 'completeness' AS check_category, 'table_not_empty' AS check_name, 0.0 AS threshold_pct, t_empty AS failed_rows, 1 AS total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'pickup_datetime_not_null',   0.0, c_pickup_ts,        total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'dropoff_datetime_not_null',  0.0, c_dropoff_ts,       total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'pickup_zone_not_null',       0.0, c_pu,               total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'dropoff_zone_not_null',      0.0, c_do,               total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'trip_distance_not_null',     5.0, c_distance,         total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'fare_amount_not_null',       5.0, c_fare,             total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'total_amount_not_null',      5.0, c_total,            total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'passenger_count_not_null_excl_myle', 5.0, c_passengers, total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'vendor_id_not_null',         5.0, c_vendor,           total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'source_file_recorded',       0.0, c_lineage,          total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'ingestion_time_recorded',    5.0, c_ingested,         total_rows FROM metrics
    -- Green taxi Parquet has no trip id, so two genuinely distinct trips can
    -- share every value. Tolerated at the policy 5.0; a double load would show up as tens of percent.
    UNION ALL SELECT 'uniqueness',   'no_exact_duplicate_rows',    5.0, (SELECT duplicate_rows FROM dupes), total_rows FROM metrics

    UNION ALL SELECT 'validity',     'pickup_month_matches_source_file', 5.0, v_window,     total_rows FROM metrics
    UNION ALL SELECT 'validity',     'trip_distance_not_negative', 0.0, v_distance_neg,     total_rows FROM metrics
    UNION ALL SELECT 'validity',     'fare_amount_not_negative',   5.0, v_fare_neg,         total_rows FROM metrics
    UNION ALL SELECT 'validity',     'total_amount_not_negative',  5.0, v_total_neg,        total_rows FROM metrics
    UNION ALL SELECT 'validity',     'passenger_count_not_negative', 0.0, v_passengers_neg, total_rows FROM metrics
    UNION ALL SELECT 'validity',     'passenger_count_plausible',  5.0, v_passengers_high,  total_rows FROM metrics
    UNION ALL SELECT 'validity',     'passenger_count_not_zero',   5.0, v_passengers_zero,  total_rows FROM metrics
    UNION ALL SELECT 'validity',     'trip_distance_plausible',    5.0, v_distance_absurd,  total_rows FROM metrics
    UNION ALL SELECT 'validity',     'pickup_zone_in_range',       5.0, v_pu_range,         total_rows FROM metrics
    UNION ALL SELECT 'validity',     'dropoff_zone_in_range',      5.0, v_do_range,         total_rows FROM metrics
    UNION ALL SELECT 'validity',     'vendor_id_in_domain',        5.0, v_vendor,           total_rows FROM metrics
    UNION ALL SELECT 'validity',     'ratecode_in_domain',         5.0, v_ratecode,         total_rows FROM metrics
    UNION ALL SELECT 'validity',     'payment_type_in_domain',     5.0, v_payment,          total_rows FROM metrics
    UNION ALL SELECT 'validity',     'trip_type_in_domain',        5.0, v_trip_type,        total_rows FROM metrics
    UNION ALL SELECT 'validity',     'store_and_fwd_flag_in_domain', 5.0, v_sf_flag,        total_rows FROM metrics

    UNION ALL SELECT 'consistency',  'dropoff_after_pickup',       5.0, x_time_order,       total_rows FROM metrics
    UNION ALL SELECT 'consistency',  'duration_not_zero',          5.0, x_zero_duration,    total_rows FROM metrics
    UNION ALL SELECT 'consistency',  'duration_under_24_hours',    5.0, x_over_24h,         total_rows FROM metrics
    UNION ALL SELECT 'consistency',  'myle_dispatch_fields_stay_null', 0.0, x_myle_unexpected,  total_rows FROM metrics
    UNION ALL SELECT 'consistency',  'dispatch_fields_null_as_a_set',  0.0, x_dispatch_partial, total_rows FROM metrics

    UNION ALL SELECT 'business',     'total_equals_sum_of_charges_v2', 5.0, b_total_mismatch_v2, n_vendor2 FROM metrics
    UNION ALL SELECT 'business',     'total_equals_sum_of_charges_v1', 5.0, b_total_mismatch_v1, n_vendor1 FROM metrics
    UNION ALL SELECT 'business',     'myle_fare_stays_placeholder',    0.0, b_myle_fare_real,    n_vendor6 FROM metrics
    UNION ALL SELECT 'business',     'fare_plausible_for_duration',    5.0, b_fare_implausible,  total_rows FROM metrics
    UNION ALL SELECT 'business',     'no_tip_recorded_on_cash',        5.0, b_cash_tip,          total_rows FROM metrics
    UNION ALL SELECT 'business',     'fare_implies_some_distance',     5.0, b_fare_no_distance,  total_rows FROM metrics
    UNION ALL SELECT 'business',     'implied_speed_under_100mph',     5.0, b_impossible_speed,  total_rows FROM metrics
)
SELECT
    v_run_id, v_run_ts, 'bronze', 'green_taxi',
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    threshold_pct,
    CASE WHEN failed_rows = 0                                              THEN 'PASS'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= threshold_pct THEN 'WARN'
         ELSE 'FAIL' END
FROM checks;

--2. taxi_zones
-- Small, static dimension table, so most checks use zero tolerance.
-- A defect can mislabel every trip that joins to it, not just one row.

-- location_id is the primary key, so uniqueness has zero tolerance.
-- Duplicates would fan out Gold joins and inflate trip counts.
-- `location_id` is text, so every cast is a `try_cast`

-- location_id arrives as STRING, so TRY_CAST is used to avoid notebook failures on invalid values.
-- location_id_in_range flags both unparseable values and IDs outside 1–265.


-- LocationIDs 103–105 share the same zone name, so grouping by ID vs. name gives different counts.
-- This is advisory to make that distinction explicit.
-- IDs 264 and 265 mean "Unknown" and are kept separate from NULL,
-- which represents a missing value.

INSERT INTO nyc_quality.dq_results
WITH metrics AS (
    SELECT
        COUNT(*)                                                                AS total_rows,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END                                AS t_empty,
        SUM(CASE WHEN location_id    IS NULL THEN 1 ELSE 0 END)                 AS c_location,
        SUM(CASE WHEN borough        IS NULL THEN 1 ELSE 0 END)                 AS c_borough,
        SUM(CASE WHEN `zone`         IS NULL THEN 1 ELSE 0 END)                 AS c_zone,
        SUM(CASE WHEN service_zone   IS NULL THEN 1 ELSE 0 END)                 AS c_service,
        SUM(CASE WHEN source_file    IS NULL THEN 1 ELSE 0 END)                 AS c_lineage,
        SUM(CASE WHEN ingestion_time IS NULL THEN 1 ELSE 0 END)                 AS c_ingested,

        -- Unparseable OR out of range. 
        SUM(CASE WHEN location_id IS NOT NULL
                  AND (try_cast(location_id AS INT) IS NULL
                       OR try_cast(location_id AS INT) NOT BETWEEN 1 AND 265)
                  THEN 1 ELSE 0 END)                                            AS v_id_range,
        SUM(CASE WHEN borough NOT IN ('Manhattan','Queens','Brooklyn','Bronx',
                                      'Staten Island','EWR','Unknown','N/A')
                  THEN 1 ELSE 0 END)                                            AS v_borough_domain,
        SUM(CASE WHEN service_zone NOT IN ('Boro Zone','Yellow Zone','Airports',
                                           'EWR','N/A')
                  THEN 1 ELSE 0 END)                                            AS v_service_domain,

        COUNT(location_id) - COUNT(DISTINCT location_id)                         AS u_id_dupes,
        CASE WHEN COUNT(*) = 265 THEN 0 ELSE 1 END                              AS t_row_count,

        CASE WHEN COUNT(DISTINCT CASE WHEN try_cast(location_id AS INT) IN (1,132,138)
                                      THEN location_id END) = 3
             THEN 0 ELSE 1 END                                                  AS t_airports
    FROM nyc_bronze.taxi_zones
),
-- Zone names shared by more than one id. Needs a GROUP BY, so it comes in as
-- a scalar subquery rather than a join.
shared_names AS (
    SELECT COALESCE(SUM(n), 0) AS ids_sharing_a_name
    FROM (
        SELECT COUNT(*) AS n
        FROM   nyc_bronze.taxi_zones
        -- 264 and 265 are both "Unknown" by design. try_cast so a malformed id
        -- cannot take the cell down; such a row is reported by
        -- location_id_in_range and simply is not excluded here.
        WHERE  COALESCE(try_cast(location_id AS INT), -1) NOT IN (264, 265)
        GROUP  BY `zone`
        HAVING COUNT(*) > 1
    )
),
checks AS (
    SELECT 'completeness' AS check_category, 'table_not_empty' AS check_name, 0.0 AS threshold_pct, t_empty AS failed_rows, 1 AS total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'location_id_not_null',    0.0, c_location,       total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'borough_not_null',        5.0, c_borough,        total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'zone_name_not_null',      5.0, c_zone,           total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'service_zone_not_null',   5.0, c_service,        total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'source_file_recorded',    0.0, c_lineage,        total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'ingestion_time_recorded', 5.0, c_ingested,       total_rows FROM metrics
    -- The single most important check here: a duplicate key fans out the join
    -- in Gold and inflates every trip count.
    UNION ALL SELECT 'uniqueness',   'location_id_unique',      0.0, u_id_dupes,       total_rows FROM metrics

    UNION ALL SELECT 'validity',     'location_id_in_range',    5.0, v_id_range,       total_rows FROM metrics
    UNION ALL SELECT 'validity',     'borough_in_domain',       5.0, v_borough_domain, total_rows FROM metrics
    UNION ALL SELECT 'validity',     'service_zone_in_domain',  5.0, v_service_domain, total_rows FROM metrics

    UNION ALL SELECT 'business',     'lookup_has_265_zones',    0.0, t_row_count,      1 FROM metrics
    UNION ALL SELECT 'business',     'airport_zones_present',   0.0, t_airports,       1 FROM metrics
    -- Advisory: an observation about the source, not a defect. Expect exactly
    -- 3 (LocationIDs 103/104/105).
    UNION ALL SELECT 'business',     'zone_names_shared_is_3', 100.0,
                     CASE WHEN (SELECT ids_sharing_a_name FROM shared_names) = 3
                          THEN 0 ELSE 1 END, 1 FROM metrics
)

SELECT
    v_run_id, v_run_ts, 'bronze', 'taxi_zones',
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    threshold_pct,
    CASE WHEN failed_rows = 0                                              THEN 'PASS'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= threshold_pct THEN 'WARN'
         ELSE 'FAIL' END
FROM checks;

-- 3. weather
-- Every column is STRING, so every column needs two checks
--
-- | Check | Catches |
-- |---|---|
-- | `*_not_null` | the value is missing |
-- | `*_parses` | the value is there but will not convert |
--

INSERT INTO nyc_quality.dq_results
-- Blank is not the same as absent -- except in a table loaded entirely as
-- STRING, where it is. `IS NULL` does not see '' or '   ', so an empty cell
-- passes every completeness check below AND fails every parse check: the
-- column is reported as present and unreadable at the same time, which is the
-- wrong diagnosis twice. Normalising once here means every check downstream
-- inherits it and no individual check has to remember.
--
-- `ingestion_timestamp` is passed through untouched -- it is the one column
-- that is not source text.
WITH w AS (
    SELECT
        NULLIF(trim(`date`),                    '') AS `date`,
        NULLIF(trim(temperature_2m),            '') AS temperature_2m,
        NULLIF(trim(apparent_temperature),      '') AS apparent_temperature,
        NULLIF(trim(precipitation_probability), '') AS precipitation_probability,
        NULLIF(trim(rain),                      '') AS rain,
        NULLIF(trim(weather_code),              '') AS weather_code,
        NULLIF(trim(cloud_cover),               '') AS cloud_cover,
        NULLIF(trim(visibility),                '') AS visibility,
        NULLIF(trim(wind_speed_10m),            '') AS wind_speed_10m,
        NULLIF(trim(wind_gusts_10m),            '') AS wind_gusts_10m,
        NULLIF(trim(`month`),                   '') AS `month`,
        NULLIF(trim(source_file_month),         '') AS source_file_month,
        ingestion_timestamp                         AS ingestion_timestamp
    FROM nyc_bronze.weather
),

-- Covered months are derived from the data, so no hardcoded month list is needed.
-- A month must contain at least two distinct days to exclude the single boundary hour
-- from the previous month. That hour is still checked by hour_within_covered_m_
covered_months AS (
    SELECT date_trunc('MONTH', try_cast(`date` AS TIMESTAMP)) AS month_start
    FROM   w
    WHERE  try_cast(`date` AS TIMESTAMP) IS NOT NULL
    GROUP  BY date_trunc('MONTH', try_cast(`date` AS TIMESTAMP))
    HAVING COUNT(DISTINCT to_date(try_cast(`date` AS TIMESTAMP))) >= 2
),

flagged AS (
    SELECT w.*,
           c.month_start IS NOT NULL AS month_is_covered
    FROM       w
    LEFT JOIN  covered_months c
           ON  c.month_start = date_trunc('MONTH', try_cast(w.`date` AS TIMESTAMP))
),

metrics AS (
    SELECT
        COUNT(*)                                                                    AS total_rows,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END                                    AS t_empty,
        -- completeness
        SUM(CASE WHEN `date`                     IS NULL THEN 1 ELSE 0 END)         AS c_date,
        SUM(CASE WHEN temperature_2m             IS NULL THEN 1 ELSE 0 END)         AS c_temp,
        SUM(CASE WHEN apparent_temperature       IS NULL THEN 1 ELSE 0 END)         AS c_apparent,
        SUM(CASE WHEN precipitation_probability  IS NULL THEN 1 ELSE 0 END)         AS c_precip_prob,
        SUM(CASE WHEN rain                       IS NULL THEN 1 ELSE 0 END)         AS c_rain,
        SUM(CASE WHEN weather_code               IS NULL THEN 1 ELSE 0 END)         AS c_code,
        SUM(CASE WHEN cloud_cover                IS NULL THEN 1 ELSE 0 END)         AS c_cloud,
        SUM(CASE WHEN visibility                 IS NULL THEN 1 ELSE 0 END)         AS c_visibility,
        SUM(CASE WHEN wind_speed_10m             IS NULL THEN 1 ELSE 0 END)         AS c_wind,
        SUM(CASE WHEN wind_gusts_10m             IS NULL THEN 1 ELSE 0 END)         AS c_gusts,
        SUM(CASE WHEN `month`                    IS NULL THEN 1 ELSE 0 END)         AS c_month,
        SUM(CASE WHEN ingestion_timestamp        IS NULL THEN 1 ELSE 0 END)         AS c_ingested,
        SUM(CASE WHEN source_file_month          IS NULL THEN 1 ELSE 0 END)         AS c_lineage,
        -- present but unparseable: a different problem from missing
        SUM(CASE WHEN `date` IS NOT NULL
                  AND try_cast(`date` AS TIMESTAMP) IS NULL THEN 1 ELSE 0 END)      AS p_date,
        SUM(CASE WHEN temperature_2m IS NOT NULL
                  AND try_cast(temperature_2m AS DOUBLE) IS NULL THEN 1 ELSE 0 END) AS p_temp,
        SUM(CASE WHEN apparent_temperature IS NOT NULL
                  AND try_cast(apparent_temperature AS DOUBLE) IS NULL THEN 1 ELSE 0 END) AS p_apparent,
        SUM(CASE WHEN precipitation_probability IS NOT NULL
                  AND try_cast(precipitation_probability AS DOUBLE) IS NULL THEN 1 ELSE 0 END) AS p_precip_prob,
        SUM(CASE WHEN rain IS NOT NULL
                  AND try_cast(rain AS DOUBLE) IS NULL THEN 1 ELSE 0 END)           AS p_rain,
        SUM(CASE WHEN weather_code IS NOT NULL
                  AND try_cast(weather_code AS DOUBLE) IS NULL THEN 1 ELSE 0 END)   AS p_code,
        SUM(CASE WHEN cloud_cover IS NOT NULL
                  AND try_cast(cloud_cover AS DOUBLE) IS NULL THEN 1 ELSE 0 END)    AS p_cloud,
        SUM(CASE WHEN visibility IS NOT NULL
                  AND try_cast(visibility AS DOUBLE) IS NULL THEN 1 ELSE 0 END)     AS p_visibility,
        SUM(CASE WHEN wind_speed_10m IS NOT NULL
                  AND try_cast(wind_speed_10m AS DOUBLE) IS NULL THEN 1 ELSE 0 END) AS p_wind,
        SUM(CASE WHEN wind_gusts_10m IS NOT NULL
                  AND try_cast(wind_gusts_10m AS DOUBLE) IS NULL THEN 1 ELSE 0 END) AS p_gusts,
        -- uniqueness: the hour is the MERGE key, so a duplicate means the merge
        -- condition is not doing what it is supposed to.

        COUNT(try_cast(`date` AS TIMESTAMP))
          - COUNT(DISTINCT try_cast(`date` AS TIMESTAMP))                           AS u_date_dupes,

        -- validity: ranges, assuming metric units (see the units cell below)
        SUM(CASE WHEN try_cast(temperature_2m AS DOUBLE) NOT BETWEEN -30 AND 50
                  THEN 1 ELSE 0 END)                                                AS v_temp_range,
        SUM(CASE WHEN try_cast(precipitation_probability AS DOUBLE) NOT BETWEEN 0 AND 100
                  THEN 1 ELSE 0 END)                                                AS v_precip_prob_range,
        SUM(CASE WHEN try_cast(cloud_cover AS DOUBLE) NOT BETWEEN 0 AND 100
                  THEN 1 ELSE 0 END)                                                AS v_cloud_range,
        SUM(CASE WHEN try_cast(rain AS DOUBLE) < 0 THEN 1 ELSE 0 END)               AS v_rain_neg,
        SUM(CASE WHEN try_cast(visibility AS DOUBLE) < 0 THEN 1 ELSE 0 END)         AS v_visibility_neg,
        SUM(CASE WHEN try_cast(wind_speed_10m AS DOUBLE) < 0 THEN 1 ELSE 0 END)     AS v_wind_neg,
        -- WMO 4677 present-weather codes actually used by Open-Meteo.
        SUM(CASE WHEN try_cast(weather_code AS DOUBLE) IS NOT NULL
                  AND (try_cast(weather_code AS DOUBLE)
                           <> ROUND(try_cast(weather_code AS DOUBLE))
                    OR CAST(try_cast(weather_code AS DOUBLE) AS INT) NOT IN
                      (0,1,2,3,45,48,51,53,55,56,57,61,63,65,66,67,
                       71,73,75,77,80,81,82,85,86,95,96,99))
                  THEN 1 ELSE 0 END)                                                AS v_code_domain,
        -- consistency
        -- A gust is by definition a peak of the wind, so it cannot be below the
        -- sustained speed. Both sides cast: comparing the raw strings is
        -- LEXICOGRAPHIC, which gives no error and a wrong number.
        SUM(CASE WHEN try_cast(wind_gusts_10m AS DOUBLE)
                    < try_cast(wind_speed_10m AS DOUBLE) THEN 1 ELSE 0 END)         AS x_gust_below_wind,

        -- month must match the timestamp.
        -- Accepts YYYY-MM, M, or MM formats because the source convention is undocumented.
        SUM(CASE WHEN try_cast(`date` AS TIMESTAMP) IS NOT NULL
                  AND `month` IS NOT NULL
                  AND trim(`month`) <> date_format(try_cast(`date` AS TIMESTAMP), 'yyyy-MM')
                  AND COALESCE(try_cast(trim(`month`) AS INT), -1)
                      <> month(try_cast(`date` AS TIMESTAMP))
                  THEN 1 ELSE 0 END)                                                AS x_month_mismatch,

        -- The MERGE writes '{weather_file}' literally. If the notebook ran as
        -- plain SQL rather than through Python formatting, that text lands in
        -- every row and lineage is gone. Never raises an error on its own.
        SUM(CASE WHEN source_file_month LIKE '%{%}%' THEN 1 ELSE 0 END)             AS x_placeholder,
        SUM(CASE WHEN try_cast(`date` AS TIMESTAMP) IS NOT NULL
                  AND NOT month_is_covered
                 THEN 1 ELSE 0 END)                                                 AS b_window
    FROM flagged
),

checks AS (
    SELECT 'completeness' AS check_category, 'table_not_empty' AS check_name, 0.0 AS threshold_pct, t_empty AS failed_rows, 1 AS total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'date_not_null',              0.0, c_date,        total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'temperature_not_null',       5.0, c_temp,        total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'apparent_temp_not_null',     5.0, c_apparent,    total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'precip_probability_not_null',5.0, c_precip_prob, total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'rain_not_null',              5.0, c_rain,        total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'weather_code_not_null',      5.0, c_code,        total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'cloud_cover_not_null',       5.0, c_cloud,       total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'visibility_not_null',        5.0, c_visibility,  total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'wind_speed_not_null',        5.0, c_wind,        total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'wind_gusts_not_null',        5.0, c_gusts,       total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'month_not_null',             5.0, c_month,       total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'ingestion_time_recorded',    5.0, c_ingested,    total_rows FROM metrics
    UNION ALL SELECT 'completeness', 'source_file_recorded',       0.0, c_lineage,     total_rows FROM metrics

    UNION ALL SELECT 'validity',     'date_parses',                0.0, p_date,        total_rows FROM metrics
    UNION ALL SELECT 'validity',     'temperature_parses',         5.0, p_temp,        total_rows FROM metrics
    UNION ALL SELECT 'validity',     'apparent_temp_parses',       5.0, p_apparent,    total_rows FROM metrics
    UNION ALL SELECT 'validity',     'precip_probability_parses',  5.0, p_precip_prob, total_rows FROM metrics
    UNION ALL SELECT 'validity',     'rain_parses',                5.0, p_rain,        total_rows FROM metrics
    UNION ALL SELECT 'validity',     'weather_code_parses',        5.0, p_code,        total_rows FROM metrics
    UNION ALL SELECT 'validity',     'cloud_cover_parses',         5.0, p_cloud,       total_rows FROM metrics
    UNION ALL SELECT 'validity',     'visibility_parses',          5.0, p_visibility,  total_rows FROM metrics
    UNION ALL SELECT 'validity',     'wind_speed_parses',          5.0, p_wind,        total_rows FROM metrics
    UNION ALL SELECT 'validity',     'wind_gusts_parses',          5.0, p_gusts,       total_rows FROM metrics

    -- Renamed from one_row_per_date. The grain is hourly, and a name that
    -- says "date" invites the next reader to relax it to a daily rule.
    UNION ALL SELECT 'uniqueness',   'one_row_per_hour',           0.0, u_date_dupes,  total_rows FROM metrics

    UNION ALL SELECT 'validity',     'temperature_plausible',      5.0, v_temp_range,       total_rows FROM metrics
    UNION ALL SELECT 'validity',     'precip_probability_0_to_100',5.0, v_precip_prob_range,total_rows FROM metrics
    UNION ALL SELECT 'validity',     'cloud_cover_0_to_100',       5.0, v_cloud_range,      total_rows FROM metrics
    UNION ALL SELECT 'validity',     'rain_not_negative',          5.0, v_rain_neg,         total_rows FROM metrics
    UNION ALL SELECT 'validity',     'visibility_not_negative',    5.0, v_visibility_neg,   total_rows FROM metrics
    UNION ALL SELECT 'validity',     'wind_speed_not_negative',    5.0, v_wind_neg,         total_rows FROM metrics
    UNION ALL SELECT 'validity',     'weather_code_in_wmo_domain', 5.0, v_code_domain,      total_rows FROM metrics

    UNION ALL SELECT 'consistency',  'gusts_at_least_wind_speed',  5.0, x_gust_below_wind,  total_rows FROM metrics
    UNION ALL SELECT 'consistency',  'month_agrees_with_date',     5.0, x_month_mismatch,   total_rows FROM metrics
    UNION ALL SELECT 'consistency',  'source_file_is_not_placeholder', 100.0, x_placeholder, total_rows FROM metrics

    UNION ALL SELECT 'validity',     'hour_within_covered_months', 0.0, b_window,           total_rows FROM metrics
)

SELECT
    v_run_id, v_run_ts, 'bronze', 'weather',
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    threshold_pct,
    CASE WHEN failed_rows = 0                                              THEN 'PASS'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= threshold_pct THEN 'WARN'
         ELSE 'FAIL' END
FROM checks;

-- Check the units before trusting the weather ranges

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
FROM nyc_bronze.weather;

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

INSERT INTO nyc_quality.dq_results
WITH pu AS (
    SELECT COUNT(*) AS unmatched, (SELECT COUNT(DISTINCT pu_location_id)
                                   FROM nyc_bronze.green_taxi) AS total
    FROM (
        SELECT DISTINCT t.pu_location_id
        FROM   nyc_bronze.green_taxi t
        LEFT   JOIN nyc_bronze.taxi_zones z
               ON t.pu_location_id = try_cast(z.location_id AS INT)
        WHERE  t.pu_location_id IS NOT NULL AND z.location_id IS NULL
    )
),
do_ AS (
    SELECT COUNT(*) AS unmatched, (SELECT COUNT(DISTINCT do_location_id)
                                   FROM nyc_bronze.green_taxi) AS total
    FROM (
        SELECT DISTINCT t.do_location_id
        FROM   nyc_bronze.green_taxi t
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
    FROM   nyc_bronze.green_taxi t
    LEFT   JOIN nyc_bronze.taxi_zones zp
           ON t.pu_location_id = try_cast(zp.location_id AS INT)
    LEFT   JOIN nyc_bronze.taxi_zones zd
           ON t.do_location_id = try_cast(zd.location_id AS INT)
),
checks AS (
    SELECT 'consistency' AS check_category, 'pickup_zone_exists_in_lookup' AS check_name,
           0.0 AS threshold_pct,
           (SELECT unmatched FROM pu) AS failed_rows,
           (SELECT total FROM pu)     AS total_rows
    UNION ALL
    SELECT 'consistency', 'dropoff_zone_exists_in_lookup', 0.0,
           (SELECT unmatched FROM do_), (SELECT total FROM do_)
    UNION ALL
    SELECT 'consistency', 'trips_with_unmatched_pickup_zone', 100.0,
           (SELECT pu_rows FROM rows_hit), (SELECT n_trips FROM rows_hit)
    UNION ALL
    SELECT 'consistency', 'trips_with_unmatched_dropoff_zone', 100.0,
           (SELECT do_rows FROM rows_hit), (SELECT n_trips FROM rows_hit)
)

SELECT
    v_run_id, v_run_ts, 'bronze', 'green_taxi',
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    threshold_pct,
    CASE WHEN failed_rows = 0                                              THEN 'PASS'
         WHEN 100.0 * failed_rows / NULLIF(total_rows, 0) <= threshold_pct THEN 'WARN'
         ELSE 'FAIL' END
FROM checks;

-- # 5. Load fidelity — did the cast lose anything?
--
-- **This section exists because `green_taxi` declares its schema and
-- casts.** It has no counterpart in the personal pipeline.
--
-- The problem it solves
--
-- After `CAST(passenger_count AS INT)` runs, a value that failed to
-- convert and a value that was never there are **both NULL**. Nothing in
-- the Bronze table distinguishes them.
-- `read_files` normally adds `_rescued_data` to catch exactly this, but it
-- only appears when the schema is *inferred*. A declared schema skips
-- inference, so a value that does not fit has nowhere to go.


INSERT INTO nyc_quality.dq_results
WITH raw AS (
    -- The landed files, original column names, original Parquet types.
    --
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
    FROM read_files('/Volumes/workspace/default/ftw-b12-de/groups/week-08/group-d/green-taxi/',
                    format => 'parquet')
),
files AS (
    -- Name-level reconciliation, not a count.
    SELECT
        (SELECT COUNT(*) FROM (
            SELECT DISTINCT _metadata.file_name AS f
            FROM   read_files('/Volumes/workspace/default/ftw-b12-de/groups/week-08/group-d/green-taxi/',
                              format => 'parquet')
            EXCEPT
            SELECT DISTINCT element_at(split(source_file, '/'), -1)
            FROM   nyc_bronze.green_taxi
        ))                                                             AS n_unloaded,
        (SELECT COUNT(DISTINCT _metadata.file_name)
         FROM read_files('/Volumes/workspace/default/ftw-b12-de/groups/week-08/group-d/green-taxi/',
                         format => 'parquet'))                         AS n_landed
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
    FROM nyc_bronze.green_taxi
),
fidelity AS (
    -- GREATEST(..., 0) because only an INCREASE means loss. Fewer nulls than
    -- the source would be a stranger problem, caught by the row-count check.
    SELECT 'business' AS check_category, 'row_count_matches_source' AS check_name,
           0.0 AS threshold_pct,
           ABS((SELECT n_rows FROM loaded) - (SELECT n_rows FROM raw)) AS failed_rows,
           (SELECT n_rows FROM raw)                                    AS total_rows
    UNION ALL SELECT 'business', 'every_landed_file_is_loaded', 0.0,
           (SELECT n_unloaded FROM files), (SELECT n_landed FROM files)
    UNION ALL SELECT 'completeness', 'no_nulls_added_vendor_id', 0.0,
        GREATEST((SELECT n_vendor_id FROM loaded) - (SELECT n_vendor_id FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_pu_location_id', 0.0,
        GREATEST((SELECT n_pu_location_id FROM loaded) - (SELECT n_pu_location_id FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_do_location_id', 0.0,
        GREATEST((SELECT n_do_location_id FROM loaded) - (SELECT n_do_location_id FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_ratecode_id', 0.0,
        GREATEST((SELECT n_ratecode_id FROM loaded) - (SELECT n_ratecode_id FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_payment_type', 0.0,
        GREATEST((SELECT n_payment_type FROM loaded) - (SELECT n_payment_type FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_trip_type', 0.0,
        GREATEST((SELECT n_trip_type FROM loaded) - (SELECT n_trip_type FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_passenger_count', 0.0,
        GREATEST((SELECT n_passenger_count FROM loaded) - (SELECT n_passenger_count FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_store_and_fwd_flag', 0.0,
        GREATEST((SELECT n_store_and_fwd_flag FROM loaded) - (SELECT n_store_and_fwd_flag FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_pickup_datetime', 0.0,
        GREATEST((SELECT n_pickup_datetime FROM loaded) - (SELECT n_pickup_datetime FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_dropoff_datetime', 0.0,
        GREATEST((SELECT n_dropoff_datetime FROM loaded) - (SELECT n_dropoff_datetime FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_trip_distance', 0.0,
        GREATEST((SELECT n_trip_distance FROM loaded) - (SELECT n_trip_distance FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_fare_amount', 0.0,
        GREATEST((SELECT n_fare_amount FROM loaded) - (SELECT n_fare_amount FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_total_amount', 0.0,
        GREATEST((SELECT n_total_amount FROM loaded) - (SELECT n_total_amount FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_extra', 0.0,
        GREATEST((SELECT n_extra FROM loaded) - (SELECT n_extra FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_mta_tax', 0.0,
        GREATEST((SELECT n_mta_tax FROM loaded) - (SELECT n_mta_tax FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_tip_amount', 0.0,
        GREATEST((SELECT n_tip_amount FROM loaded) - (SELECT n_tip_amount FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_tolls_amount', 0.0,
        GREATEST((SELECT n_tolls_amount FROM loaded) - (SELECT n_tolls_amount FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_improvement_surcharge', 0.0,
        GREATEST((SELECT n_improvement_surcharge FROM loaded) - (SELECT n_improvement_surcharge FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_congestion_surcharge', 0.0,
        GREATEST((SELECT n_congestion_surcharge FROM loaded) - (SELECT n_congestion_surcharge FROM raw), 0), (SELECT n_rows FROM raw)
)

SELECT
    v_run_id, v_run_ts, 'bronze', 'green_taxi',
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    threshold_pct,
    CASE WHEN failed_rows = 0 THEN 'PASS' ELSE 'FAIL' END
FROM fidelity;

-- If `row_count_matches_source` fails, `COPY INTO` either skipped a file it
-- had already loaded or loaded one twice. If `every_landed_file_is_loaded`
-- fails, a specific file landed and was never picked up — the `EXCEPT` in
-- the `files` CTE will name it if you run it on its own. If a
-- `no_nulls_added_*` check fails, that column's `CAST` is rejecting real
-- values — swap it for `try_cast` and add a flag, or widen the declared
-- type. All three are loader bugs, not data problems, and belong with
-- whoever owns the load.

-- 6. Audit log

INSERT INTO nyc_quality.dq_run_log
SELECT
    v_run_id,
    v_run_ts,
    'bronze',
    COUNT(DISTINCT table_name)                                   AS tables_checked,
    COUNT(*)                                                     AS checks_run,
    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END)             AS checks_passed,
    SUM(CASE WHEN status = 'WARN' THEN 1 ELSE 0 END)             AS checks_warned,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END)             AS checks_failed,
    CASE WHEN SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) > 0 THEN 'FAIL'
         WHEN SUM(CASE WHEN status = 'WARN' THEN 1 ELSE 0 END) > 0 THEN 'WARN'
         ELSE 'PASS' END                                         AS overall_status,
    current_timestamp()                                          AS finished_at
FROM nyc_quality.dq_results
WHERE run_id = v_run_id;

-- 7. Results

SELECT * FROM nyc_quality.dq_run_log WHERE run_id = v_run_id;

SELECT table_name, status, COUNT(*) AS checks
FROM   nyc_quality.dq_results
WHERE  run_id = v_run_id
GROUP  BY table_name, status
ORDER  BY table_name, status;

-- Everything that is not a clean pass, worst first.
SELECT table_name, check_category, check_name,
       failed_rows, total_rows, failed_pct, threshold_pct, status
FROM   nyc_quality.dq_results
WHERE  run_id = v_run_id AND status <> 'PASS'
ORDER  BY CASE status WHEN 'FAIL' THEN 0 ELSE 1 END, failed_pct DESC;

-- 8. Gate
-- Two ways to stop the pipeline
-- | Trigger | Meaning |
-- |---|---|
-- | **any blocking check fails** | one thing broke that makes the data unusable downstream |
-- | **5 or more checks fail in total** | no single thing is fatal, but something is broadly wrong |
--
-- Everything else is still written to `dq_results` with `status = 'FAIL'`.
-- It is recorded and queryable — it just does not halt the run.
--
-- Why not simply "any FAIL"
-- A gate that fires on noise gets switched off, which is worse than no
-- gate. Most of these thresholds are the policy 5.0 rather than a measured
-- claim about this source, and one of them being slightly wrong should not
-- stop a load.


DECLARE OR REPLACE VARIABLE v_blocking_failures INT;
DECLARE OR REPLACE VARIABLE v_total_failures    INT;

SET VAR v_blocking_failures = (
    SELECT COUNT(*)
    FROM   nyc_quality.dq_results
    WHERE  run_id = v_run_id AND status = 'FAIL'
      AND  check_name IN (
        -- the grain: a row that cannot be placed in time or space
        'pickup_datetime_not_null','dropoff_datetime_not_null',
        'pickup_zone_not_null','dropoff_zone_not_null',
        'date_not_null','date_parses',
        'location_id_not_null',
        -- the load produced nothing at all
        'table_not_empty',
        -- keys: a duplicate fans out a join and inflates every total
        'location_id_unique','one_row_per_hour',
        -- lineage: without it nothing can be traced
        'source_file_recorded',
        -- physically impossible values that still cast cleanly
        'trip_distance_not_negative','passenger_count_not_negative',
        -- referential integrity: trips that can never resolve to a zone
        'pickup_zone_exists_in_lookup','dropoff_zone_exists_in_lookup',
        -- our own pipeline damaged the data on the way in
        'row_count_matches_source','every_landed_file_is_loaded',
        'no_nulls_added_vendor_id','no_nulls_added_pu_location_id',
        'no_nulls_added_do_location_id','no_nulls_added_ratecode_id',
        'no_nulls_added_payment_type','no_nulls_added_trip_type',
        'no_nulls_added_passenger_count','no_nulls_added_store_and_fwd_flag',
        'no_nulls_added_pickup_datetime','no_nulls_added_dropoff_datetime',
        'no_nulls_added_trip_distance','no_nulls_added_fare_amount',
        'no_nulls_added_total_amount','no_nulls_added_extra',
        'no_nulls_added_mta_tax','no_nulls_added_tip_amount',
        'no_nulls_added_tolls_amount','no_nulls_added_improvement_surcharge',
        'no_nulls_added_congestion_surcharge'
      )
);

SET VAR v_total_failures = (
    SELECT COUNT(*) FROM nyc_quality.dq_results
    WHERE run_id = v_run_id AND status = 'FAIL'
);


-- #The blocking list has to be checked against reality
-- A name in that `IN` list that no check ever emits is inert: it matches
-- nothing, so the gate neither blocks nor complains. That is exactly what
-- makes it dangerous. The list reads like a guarantee, and the next
-- person to rename a check turns one of those guarantees off without
-- touching the gate.
--
-- This is how that was found the first time: four names in the list —
-- `timestamp_not_null`, `timestamp_parses`, `one_row_per_hour`,
-- `no_rescued_data` — were carried over from the personal pipeline, whose
-- weather column and inferred schema are different. The gate claimed to
-- guard a `_rescued_data` column this table does not have.
--
-- It matters again right now: this revision renamed two checks
-- (`pickup_within_load_window` → `pickup_month_matches_source_file`,
-- `hour_within_load_window` → `hour_within_covered_months`) and removed
-- two more. Neither renamed name was in the blocking list, which this
-- cell is what confirms rather than what asserts.
--
-- So the list is verified against the run instead of trusted. Anything
-- returned below is a name the gate is watching for and the notebook
-- never produces.


WITH blocking(check_name) AS (
    SELECT explode(array(
        'pickup_datetime_not_null','dropoff_datetime_not_null',
        'pickup_zone_not_null','dropoff_zone_not_null',
        'date_not_null','date_parses','location_id_not_null',
        'table_not_empty','location_id_unique','one_row_per_hour',
        'source_file_recorded','trip_distance_not_negative',
        'passenger_count_not_negative',
        'pickup_zone_exists_in_lookup','dropoff_zone_exists_in_lookup',
        'row_count_matches_source','every_landed_file_is_loaded',
        'no_nulls_added_vendor_id','no_nulls_added_pu_location_id',
        'no_nulls_added_do_location_id','no_nulls_added_ratecode_id',
        'no_nulls_added_payment_type','no_nulls_added_trip_type',
        'no_nulls_added_passenger_count','no_nulls_added_store_and_fwd_flag',
        'no_nulls_added_pickup_datetime','no_nulls_added_dropoff_datetime',
        'no_nulls_added_trip_distance','no_nulls_added_fare_amount',
        'no_nulls_added_total_amount','no_nulls_added_extra',
        'no_nulls_added_mta_tax','no_nulls_added_tip_amount',
        'no_nulls_added_tolls_amount','no_nulls_added_improvement_surcharge',
        'no_nulls_added_congestion_surcharge'
    ))
)
SELECT b.check_name AS blocking_name_never_produced
FROM   blocking b
LEFT   JOIN (SELECT DISTINCT check_name
             FROM   nyc_quality.dq_results
             WHERE  run_id = v_run_id) r
       ON b.check_name = r.check_name
WHERE  r.check_name IS NULL;

SELECT v_blocking_failures AS blocking_failures,
       v_total_failures    AS total_failures,
       CASE WHEN v_blocking_failures > 0 THEN 'will stop: blocking check failed'
            WHEN v_total_failures >= 5   THEN 'will stop: 5 or more failures'
            ELSE 'will continue' END AS verdict;

SELECT CASE
    WHEN v_blocking_failures > 0
      THEN raise_error(CONCAT('Bronze DQ gate FAILED (group): ',
                              CAST(v_blocking_failures AS STRING),
                              ' BLOCKING check(s) failed (of ',
                              CAST(v_total_failures AS STRING),
                              ' total). See nyc_quality.dq_results for run ', v_run_id))
    WHEN v_total_failures >= 5
      THEN raise_error(CONCAT('Bronze DQ gate FAILED (group): ',
                              CAST(v_total_failures AS STRING),
                              ' checks over threshold, none individually blocking. ',
                              'See nyc_quality.dq_results for run ', v_run_id))
    ELSE CONCAT('Bronze DQ gate PASSED (', CAST(v_total_failures AS STRING),
                ' non-blocking failure(s) recorded)')
END AS gate;

-- A pass with non-blocking failures recorded is a normal, honest outcome.
-- Read them here and decide whether each is a threshold to measure or a
-- defect to fix:


SELECT table_name, check_category, check_name,
       failed_rows, total_rows, failed_pct, threshold_pct
FROM   nyc_quality.dq_results
WHERE  run_id = v_run_id AND status = 'FAIL'
ORDER  BY failed_pct DESC;

-- 9. Afterwards
-- The latest group run, without hardcoding a run_id.
SELECT table_name, check_category, check_name,
       failed_rows, total_rows, failed_pct, threshold_pct, status
FROM   nyc_quality.dq_results
WHERE  table_name IN ('green_taxi','taxi_zones','weather')
  AND  run_id = (SELECT run_id FROM nyc_quality.dq_results
                 WHERE table_name IN ('green_taxi','taxi_zones','weather')
                 ORDER BY run_ts DESC LIMIT 1)
ORDER  BY CASE status WHEN 'FAIL' THEN 0 WHEN 'WARN' THEN 1 ELSE 2 END, failed_pct DESC;

-- Has any check moved since the previous run?
SELECT table_name, check_name, run_ts, failed_pct, status,
       LAG(failed_pct) OVER (PARTITION BY table_name, check_name ORDER BY run_ts) AS previous_pct,
       ROUND(failed_pct - LAG(failed_pct) OVER (PARTITION BY table_name, check_name ORDER BY run_ts), 4) AS change
FROM   nyc_quality.dq_results
WHERE  table_name IN ('green_taxi','taxi_zones','weather')
ORDER  BY table_name, check_name, run_ts;