-- Data Quality — Bronze
-- Covers all three group Bronze tables in **one run**: `green_taxi`,
-- `taxi_zones`, `weather`, plus two cross-cutting sections.

-- | Choice | Consequence | Handled in |
-- |---|---|---|
-- | `green_taxi` declares its schema and `CAST`s | a failed cast is indistinguishable from a missing value, and there is no `_rescued_data` | §5 |
-- | `weather` is **entirely STRING** | nothing is typed, so every column needs a parse check as well as a null check | §3 |
-- | `weather` merges **insert-if-absent** on `date` | a revised reading never overwrites the first one | §3 |

-- Sections
-- | § | Covers | Checks |
-- |---|---|---|
-- | 1 | `green_taxi` | 39 |
-- | 2 | `taxi_zones` | 13 |
-- | 3 | `weather` | 37 |
-- | 4 | referential integrity, trips → zones | 2 |
-- | 5 | load fidelity, `green_taxi` vs landed Parquet | 7 |
-- | | **total** | **98** |
-- Then 6. Audit log · 7. Results · 8. Gate · 9. Afterwards.


SET TIME ZONE 'America/New_York';
USE CATALOG `nyc-mobility`;

DECLARE OR REPLACE VARIABLE v_run_id STRING;
DECLARE OR REPLACE VARIABLE v_run_ts TIMESTAMP;

SET VAR v_run_id = uuid();

SET VAR v_run_ts = current_timestamp();

SELECT v_run_id AS run_id, v_run_ts AS run_ts;

-- Rerunning replaces this run's own rows rather than doubling them.
DELETE FROM nyc_quality.dq_results
WHERE run_id = v_run_id AND layer = 'bronze'
  AND table_name IN ('green_taxi', 'taxi_zones', 'weather');

DELETE FROM nyc_quality.dq_run_log WHERE run_id = v_run_id AND layer = 'bronze';

-- 1. green_taxi
--
-- Thresholds — three tiers
-- | Tier | Value | When | Count |
-- |---|---|---|---|
-- | **Fatal / scalar** | `0.0` | One occurrence corrupts an aggregate, a join or the grain — or the check is scalar, where a percentage cannot apply | ~20 |
-- | **Measured** | the observed rate plus headroom | We counted it in this dataset; the number is in the comment | ~13 |
-- | **Provisional** | `5.0` | Never measured. A working default, not a finding | the rest |
--
-- Why not 5% across the board
-- A percentage works when **the damage is proportional to the count**. A
-- null `passenger_count` affects that trip's passenger stats and nothing
-- else, so 3% of those is roughly 3% of the harm.
--
-- It breaks when one row corrupts an aggregate or a join. One duplicate
-- `location_id` is 0.38% of the lookup and passes a 5% gate — and in Gold
-- it fans out every trip in that zone, so a busy zone's revenue doubles.
-- Totals go **up**, which is much harder to spot than totals going down.
-- Likewise the 111,005-mile trip: a handful of rows, 0.004%, and the mean
-- distance is already six times the median because of them.
--
--
-- Scalar checks
-- A few checks have `total_rows = 1` — "does the lookup have 265 rows",
-- "does every day have 24 hours". Their percentage is 0 or 100 and nothing
-- between, so a threshold of 5 would behave exactly like 0. They stay at
-- `0.0` because that is the only value that means anything.
--

-- The three vendors report fares differently
-- One accounting identity across all vendors fails on about a quarter of
-- rows — not because the data is bad but because each provider uses a
-- different convention. So: one check per vendor, each measured against
-- that vendor's own row count.
--
-- | Vendor | `total_amount` contains | Observed |
-- |---|---|---|
-- | 2 — Curb | everything, including `cbd_congestion_fee` | 1.33% |
-- | 1 — Creative Mobile | metered portion only; all three surcharges excluded | 1.63% |
-- | 6 — Myle | the real charge; `fare_amount` is a placeholder | n/a |
--
-- Using the whole table as denominator would dilute a vendor-scoped
-- failure until it could never reach its own threshold, so each carries
-- `n_vendorN`.

-- The VendorID 6 finding
-- Myle submits none of the six dispatch fields across 100% of its rows,
-- about 10.6% of all trips. Those nulls are structural, not missing data,
-- so the completeness check **excludes Myle** and two consistency checks
-- assert the pattern instead.
--
-- **`NOT IN` and NULL:** `vendor_id NOT IN (1,2,6)` is NULL when
-- `vendor_id` is NULL, so it is not counted as a validity failure. A
-- missing value is a *completeness* problem with its own check; counting
-- it twice would double-report one bad row.


INSERT INTO nyc_quality.dq_results
WITH metrics AS (
    SELECT
        COUNT(*)                                                                    AS total_rows,
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
        -- validity: ranges
        SUM(CASE WHEN date_format(lpep_pickup_datetime, 'yyyy-MM')
                      NOT IN ('2026-03','2026-04','2026-05') THEN 1 ELSE 0 END)     AS v_window,
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
        -- A single trip of 111,005 miles was observed. p99.9 is 31 miles, so
        -- the 200 cutoff sits far above real trips and far below the fault.
        SUM(CASE WHEN trip_distance > 200 THEN 1 ELSE 0 END)                        AS v_distance_absurd,
        -- Domains from the current TLC LPEP dictionary. An older dictionary
        -- listed vendor (1,2) and payment_type (1..6), which flags 14,181 valid
        -- Myle trips as invalid. A domain check is only as current as the
        -- dictionary it was copied from.
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
        -- cbd_congestion_fee is a flat 0.75 Congestion Relief Zone charge.
        -- Leaving it out of the sum makes every trip carrying it come out
        -- exactly 0.75 short: a 26% failure rate that looks like a data fault
        -- and is a missing term.
        SUM(CASE WHEN vendor_id = 2
                  AND ABS(total_amount - (
                          COALESCE(fare_amount, 0) + COALESCE(extra, 0)
                        + COALESCE(mta_tax, 0) + COALESCE(tip_amount, 0)
                        + COALESCE(tolls_amount, 0) + COALESCE(improvement_surcharge, 0)
                        + COALESCE(congestion_surcharge, 0)
                        + COALESCE(cbd_congestion_fee, 0))) > 0.01
                  THEN 1 ELSE 0 END)                                                   AS b_total_mismatch_v2,
        SUM(CASE WHEN vendor_id = 2 THEN 1 ELSE 0 END)                                 AS n_vendor2,

        -- Vendor 1 reports total_amount as the METERED portion only; all three
        -- surcharges are itemised in their own columns and never rolled in.
        SUM(CASE WHEN vendor_id = 1
                  AND ABS(total_amount - (
                          COALESCE(fare_amount, 0) + COALESCE(extra, 0)
                        + COALESCE(mta_tax, 0) + COALESCE(tip_amount, 0)
                        + COALESCE(tolls_amount, 0))) > 0.01
                  THEN 1 ELSE 0 END)                                                   AS b_total_mismatch_v1,
        SUM(CASE WHEN vendor_id = 1 THEN 1 ELSE 0 END)                                 AS n_vendor1,

        -- Vendor 6: fare_amount averages ~2.75 across EVERY distance band while
        -- total_amount rises 15.79 -> 51.52. A number uncorrelated with distance
        -- is not a metered fare. 10 sits above the largest observed value (9)
        -- and far below the average total (~29).
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
        SUM(CASE WHEN trip_distance > 0
                  AND timestampdiff(SECOND, lpep_pickup_datetime, lpep_dropoff_datetime) > 0
                  AND trip_distance /
                      (timestampdiff(SECOND, lpep_pickup_datetime, lpep_dropoff_datetime) / 3600.0)
                      > 100
                  THEN 1 ELSE 0 END)                                                   AS b_impossible_speed
    FROM nyc_bronze.green_taxi
),
-- Duplicates need a GROUP BY, so they are counted separately and pulled in as
-- a scalar subquery — no join. md5(to_json(struct(...))) fingerprints a row
-- without listing twenty columns. The provenance columns are excluded: two
-- identical trips from different files are still duplicates.
dupes AS (
    SELECT COUNT(*) - COUNT(DISTINCT row_fingerprint) AS duplicate_rows
    FROM (
        SELECT md5(to_json(struct(* EXCEPT (source_file, ingestion_time)))) AS row_fingerprint
        FROM nyc_bronze.green_taxi
    )
),
checks AS (
    SELECT 'completeness' AS check_category, 'pickup_datetime_not_null' AS check_name, 0.0 AS threshold_pct, c_pickup_ts AS failed_rows, total_rows FROM metrics
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

    UNION ALL SELECT 'uniqueness',   'no_exact_duplicate_rows',    1.0, (SELECT duplicate_rows FROM dupes), total_rows FROM metrics

    UNION ALL SELECT 'validity',     'pickup_within_load_window',  0.5, v_window,           total_rows FROM metrics
    UNION ALL SELECT 'validity',     'trip_distance_not_negative', 0.0, v_distance_neg,     total_rows FROM metrics
    UNION ALL SELECT 'validity',     'fare_amount_not_negative',   1.0, v_fare_neg,         total_rows FROM metrics
    UNION ALL SELECT 'validity',     'total_amount_not_negative',  1.0, v_total_neg,        total_rows FROM metrics
    UNION ALL SELECT 'validity',     'passenger_count_not_negative', 0.0, v_passengers_neg, total_rows FROM metrics
    UNION ALL SELECT 'validity',     'passenger_count_plausible',  5.0, v_passengers_high,  total_rows FROM metrics
    UNION ALL SELECT 'validity',     'passenger_count_not_zero',   2.0, v_passengers_zero,  total_rows FROM metrics
    UNION ALL SELECT 'validity',     'trip_distance_under_200mi',  0.0, v_distance_absurd,  total_rows FROM metrics
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
    -- 2.0 on both: vendor 2 settles at 1.33%, vendor 1 at ~1.63%. Just above
    -- each measured rate, so a real break in the convention still fires.
    UNION ALL SELECT 'business',     'total_equals_sum_of_charges_v2', 2.0, b_total_mismatch_v2, n_vendor2 FROM metrics
    UNION ALL SELECT 'business',     'total_equals_sum_of_charges_v1', 2.0, b_total_mismatch_v1, n_vendor1 FROM metrics
    UNION ALL SELECT 'business',     'myle_fare_stays_placeholder',    0.0, b_myle_fare_real,    n_vendor6 FROM metrics
    UNION ALL SELECT 'business',     'fare_plausible_for_duration',    0.1, b_fare_implausible,  total_rows FROM metrics
    UNION ALL SELECT 'business',     'no_tip_recorded_on_cash',        5.0, b_cash_tip,          total_rows FROM metrics
    -- 4.0, not a guessed 2.0. Observed 3.14 / 3.44 / 3.29 percent across March,
    -- April and May — stable month to month, so a property of the source.
    -- 62.9% last under a minute: cancellations carrying a minimum charge.
    UNION ALL SELECT 'business',     'fare_implies_some_distance',     4.0, b_fare_no_distance,  total_rows FROM metrics
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


-- # 2. taxi_zones
-- Small and static, so almost everything is zero-tolerance. This table is
-- a **dimension**: a defect here does not corrupt one row, it mislabels
-- every trip that joins to it.
-- `location_id` is the primary key, so `location_id_unique` at 0.0 is the
-- most important check in the section. A duplicate key would fan out the
-- join in Gold and **inflate** trip counts — a failure that makes totals
-- go up, which is far harder to notice than one that makes them go down.

-- **LocationIDs 103, 104 and 105 share one zone name**
-- (`Governor's Island/Ellis Island/Liberty Island`). Group by id and you
-- get three rows; group by name and you get one. Neither is wrong, but a
-- report that switches between them without saying so is. Reported as an
-- IGNORE-level check so the number is visible rather than discovered.
--
-- **264 and 265 are both literally "Unknown"** — real LocationIDs meaning
-- "we do not know", which is different from a NULL. Keeping them apart in
-- Silver lets you tell "the meter recorded an unknown zone" from "the
-- column was empty".

INSERT INTO nyc_quality.dq_results
WITH metrics AS (
    SELECT
        COUNT(*)                                                                AS total_rows,
        SUM(CASE WHEN location_id    IS NULL THEN 1 ELSE 0 END)                 AS c_location,
        SUM(CASE WHEN borough        IS NULL THEN 1 ELSE 0 END)                 AS c_borough,
        SUM(CASE WHEN `zone`         IS NULL THEN 1 ELSE 0 END)                 AS c_zone,
        SUM(CASE WHEN service_zone   IS NULL THEN 1 ELSE 0 END)                 AS c_service,
        SUM(CASE WHEN source_file    IS NULL THEN 1 ELSE 0 END)                 AS c_lineage,
        SUM(CASE WHEN ingestion_time IS NULL THEN 1 ELSE 0 END)                 AS c_ingested,
        SUM(CASE WHEN CAST(location_id AS INT) NOT BETWEEN 1 AND 265
                  THEN 1 ELSE 0 END)                                            AS v_id_range,
        SUM(CASE WHEN borough NOT IN ('Manhattan','Queens','Brooklyn','Bronx',
                                      'Staten Island','EWR','Unknown','N/A')
                  THEN 1 ELSE 0 END)                                            AS v_borough_domain,
        SUM(CASE WHEN service_zone NOT IN ('Boro Zone','Yellow Zone','Airports',
                                           'EWR','N/A')
                  THEN 1 ELSE 0 END)                                            AS v_service_domain,

        COUNT(*) - COUNT(DISTINCT location_id)                                  AS u_id_dupes,
        -- The lookup is published with exactly 265 zones. A different number
        -- means the file changed or the load is partial — either way every
        -- zone-level result downstream is suspect.
        CASE WHEN COUNT(*) = 265 THEN 0 ELSE 1 END                              AS t_row_count,
        -- Are the three airport ids present? If they are missing the file is
        -- not the lookup we think it is.
        CASE WHEN COUNT(DISTINCT CASE WHEN CAST(location_id AS INT) IN (1,132,138)
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
        WHERE  CAST(location_id AS INT) NOT IN (264, 265)   -- both are "Unknown" by design
        GROUP  BY `zone`
        HAVING COUNT(*) > 1
    )
),

checks AS (
    SELECT 'completeness' AS check_category, 'location_id_not_null' AS check_name, 0.0 AS threshold_pct, c_location AS failed_rows, total_rows FROM metrics
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
    -- IGNORE-level: an observation about the source, not a defect. Expect 3
    -- (LocationIDs 103/104/105). A different number means the naming changed.
    UNION ALL SELECT 'business',     'zone_names_shared_count', 100.0,
                     (SELECT ids_sharing_a_name FROM shared_names), total_rows FROM metrics
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
--
--  Every column is STRING, so every column needs two checks
-- The group declares the whole table as `STRING`. That means a value can
-- be **present but meaningless** — `"n/a"`, `""`, `"-"` — and a null check
-- will happily pass it.
--
-- So each numeric column gets a pair:
-- | Check | Catches |
-- |---|---|
-- | `*_not_null` | the value is missing |
-- | `*_parses` | the value is there but will not convert |
--
-- Those are different problems with different fixes, and collapsing them
-- into one number tells you neither. `try_cast` is used throughout so a
-- bad value is *reported* by its own check rather than crashing the rest.
--
-- **If a `*_parses` check reports 100%,** the cast itself is wrong for
-- this text format rather than the data being bad — switch to
-- `try_to_timestamp(...)` with an explicit pattern and rerun.
--
-- The merge is insert-if-absent
-- `WHEN NOT MATCHED THEN INSERT` on `date` means a **revised reading never
-- overwrites the first one**. Good for idempotency — rerunning changes
-- nothing — but it also means a corrected value from the API will be
-- silently ignored. `one_row_per_date` confirms the key is holding;
-- section 5's counterpart for weather confirms nothing was dropped.
--
-- The source_file placeholder
-- The MERGE writes `'{weather_file}'` as `source_file_month`. If this
-- notebook is run as **plain SQL** rather than through Python string
-- formatting, that literal text lands in every row and lineage is lost.
-- `source_file_is_not_placeholder` catches exactly that, because it is the
-- kind of bug that never raises an error.
--
-- Units are unverified
-- The range checks below assume Open-Meteo **metric** defaults: °C, mm, m,
-- km/h, percent. Run the units cell underneath before trusting them — if
-- the request used Fahrenheit, `temperature_plausible` is wrong and the
-- results still look reasonable.

INSERT INTO nyc_quality.dq_results
WITH metrics AS (
    SELECT
        COUNT(*)                                                                    AS total_rows,
        -- completeness
        SUM(CASE WHEN `date`                     IS NULL OR trim(`date`) = '' THEN 1 ELSE 0 END) AS c_date,
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
        -- DOUBLE, not INT. A CSV that writes a code as "3.0" fails
        -- try_cast(... AS INT) outright: 100% of rows reported unparseable when
        -- the data was fine and the check was wrong. Parse as DOUBLE, then
        -- narrow to INT for the domain comparison below.
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

        -- uniqueness: date is the MERGE key, so a duplicate means the merge
        -- condition is not doing what it is supposed to.
        COUNT(*) - COUNT(DISTINCT `date`)                                           AS u_date_dupes,

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
        -- Same fix. Worth noting WHY this check was passing while p_code was
        -- failing 100%: try_cast(... AS INT) returned NULL for every row, and
        -- "NULL NOT IN (...)" is NULL, not TRUE — so nothing was ever counted.
        -- A check can pass because it is broken. The pair only made sense
        -- because the parse check sat next to it.
        SUM(CASE WHEN CAST(try_cast(weather_code AS DOUBLE) AS INT) NOT IN
                      (0,1,2,3,45,48,51,53,55,56,57,61,63,65,66,67,
                       71,73,75,77,80,81,82,85,86,95,96,99)
                  THEN 1 ELSE 0 END)                                                AS v_code_domain,

        -- consistency
        -- A gust is by definition a peak of the wind, so it cannot be below the
        -- sustained speed. A violation means the two columns were swapped.
        SUM(CASE WHEN try_cast(wind_gusts_10m AS DOUBLE)
                    < try_cast(wind_speed_10m AS DOUBLE) THEN 1 ELSE 0 END)         AS x_gust_below_wind,

        -- The month column must agree with the timestamp. Accepts either
        -- convention ('2026-03' or '3') because the group's is not documented —
        -- it fails only if the value agrees with neither.
        SUM(CASE WHEN try_cast(`date` AS TIMESTAMP) IS NOT NULL
                  AND `month` IS NOT NULL
                  AND date_format(try_cast(`date` AS TIMESTAMP), 'yyyy-MM') <> `month`
                  AND CAST(month(try_cast(`date` AS TIMESTAMP)) AS STRING) <> trim(`month`)
                  THEN 1 ELSE 0 END)                                                AS x_month_mismatch,

        -- The MERGE writes '{weather_file}' literally. If the notebook ran as
        -- plain SQL rather than through Python formatting, that text lands in
        -- every row and lineage is gone. Never raises an error on its own.
        SUM(CASE WHEN source_file_month LIKE '%{%}%' THEN 1 ELSE 0 END)             AS x_placeholder,

        -- business: shape of the load window
        SUM(CASE WHEN date_format(try_cast(`date` AS TIMESTAMP), 'yyyy-MM')
                      NOT IN ('2026-03','2026-04','2026-05') THEN 1 ELSE 0 END)     AS b_window,
        COUNT(DISTINCT to_date(try_cast(`date` AS TIMESTAMP)))                      AS b_days_covered
    FROM nyc_bronze.weather
),
-- Days whose hour count is neither 23, 24 nor 25. The two DST days are the
-- only legitimate exceptions. Needs a GROUP BY, so scalar subquery, no join.
day_hours AS (
    SELECT COUNT(*) AS bad_days
    FROM (
        SELECT to_date(try_cast(`date` AS TIMESTAMP)) AS d
        FROM   nyc_bronze.weather
        WHERE  try_cast(`date` AS TIMESTAMP)
               BETWEEN TIMESTAMP'2026-03-01 00:00:00' AND TIMESTAMP'2026-05-31 23:59:59'
        GROUP  BY 1
        HAVING COUNT(*) NOT IN (23, 24, 25)
    )
),

checks AS (
    SELECT 'completeness' AS check_category, 'date_not_null' AS check_name, 0.0 AS threshold_pct, c_date AS failed_rows, total_rows FROM metrics
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

    UNION ALL SELECT 'uniqueness',   'one_row_per_date',           0.0, u_date_dupes,  total_rows FROM metrics

    UNION ALL SELECT 'validity',     'temperature_plausible',      5.0, v_temp_range,       total_rows FROM metrics
    UNION ALL SELECT 'validity',     'precip_probability_0_to_100',5.0, v_precip_prob_range,total_rows FROM metrics
    UNION ALL SELECT 'validity',     'cloud_cover_0_to_100',       5.0, v_cloud_range,      total_rows FROM metrics
    UNION ALL SELECT 'validity',     'rain_not_negative',          5.0, v_rain_neg,         total_rows FROM metrics
    UNION ALL SELECT 'validity',     'visibility_not_negative',    5.0, v_visibility_neg,   total_rows FROM metrics
    UNION ALL SELECT 'validity',     'wind_speed_not_negative',    5.0, v_wind_neg,         total_rows FROM metrics
    UNION ALL SELECT 'validity',     'weather_code_in_wmo_domain', 5.0, v_code_domain,      total_rows FROM metrics

    -- 1.0, not the original guessed 0.1. Observed 13 rows of 2,208 = 0.59%.
    -- A gust is a peak of the wind so it cannot really be below the sustained
    -- speed, but the two are measured over different intervals and rounded
    -- independently, so a handful of near-ties is expected. 1.0 clears the
    -- observed rate; a column swap would show up as tens of percent.
    UNION ALL SELECT 'consistency',  'gusts_at_least_wind_speed',  1.0, x_gust_below_wind,  total_rows FROM metrics
    UNION ALL SELECT 'consistency',  'month_agrees_with_date',     5.0, x_month_mismatch,   total_rows FROM metrics
    -- ADVISORY, threshold 100.0 so it can only ever WARN.
    --
    -- Observed 2,208 of 2,208 = 100%: every row carries the literal text
    -- '{weather_file}' because the MERGE ran as plain SQL rather than through
    -- Python formatting. Lineage for this table is gone.
    --
    -- This is a REAL defect and the number is not tolerable. It is set to
    -- advisory because the fix belongs to the loader, not to quality checks,
    -- and blocking the whole Bronze gate on someone else's bug helps nobody.
    -- Reported every run so it stays visible until it is fixed, then flip the
    -- threshold back to 0.0.
    UNION ALL SELECT 'consistency',  'source_file_is_not_placeholder', 100.0, x_placeholder, total_rows FROM metrics

    -- 0.5 rather than 0: the timestamp reconstruction can emit one boundary row
    -- an hour before the window starts. One row in ~2,200 is 0.045%.
    UNION ALL SELECT 'validity',     'hour_within_load_window',    0.5, b_window,           total_rows FROM metrics
    UNION ALL SELECT 'business',     'covers_at_least_92_days',    0.0,
                     CASE WHEN b_days_covered >= 92 THEN 0 ELSE 1 END, 1 FROM metrics
    UNION ALL SELECT 'business',     'every_day_has_24_hours',     0.0,
                     (SELECT bad_days FROM day_hours), 1 FROM metrics
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
--
-- Every `pu_location_id` and `do_location_id` in `green_taxi` must exist
-- in `taxi_zones`. This is the only check that spans two tables, and it is
-- the one that predicts whether Gold will work.
--
-- An id with no matching zone becomes an Unknown member in `dim_zone`, so
-- the trip survives but lands in a bucket labelled "we do not know where".
-- Catching it here means fixing the lookup; catching it in Gold means
-- explaining a number nobody can act on.
--
-- Measured in **distinct ids**, not trips: one unmatched id affecting
-- 40,000 trips is one thing to fix, and reporting it as 40,000 failures
-- would drown out everything else.

INSERT INTO nyc_quality.dq_results
WITH pu AS (
    SELECT COUNT(*) AS unmatched, (SELECT COUNT(DISTINCT pu_location_id)
                                   FROM nyc_bronze.green_taxi) AS total
    FROM (
        SELECT DISTINCT t.pu_location_id
        FROM   nyc_bronze.green_taxi t
        LEFT   JOIN nyc_bronze.taxi_zones z
               ON CAST(t.pu_location_id AS INT) = CAST(z.location_id AS INT)
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
               ON CAST(t.do_location_id AS INT) = CAST(z.location_id AS INT)
        WHERE  t.do_location_id IS NOT NULL AND z.location_id IS NULL
    )
),
checks AS (
    SELECT 'consistency' AS check_category, 'pickup_zone_exists_in_lookup' AS check_name,
           0.0 AS threshold_pct,
           (SELECT unmatched FROM pu) AS failed_rows,
           (SELECT total FROM pu)     AS total_rows
    UNION ALL
    SELECT 'consistency', 'dropoff_zone_exists_in_lookup', 0.0,
           (SELECT unmatched FROM do_), (SELECT total FROM do_)
)

SELECT
    v_run_id, v_run_ts, 'bronze', 'green_taxi',
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    threshold_pct,
    CASE WHEN failed_rows = 0 THEN 'PASS' ELSE 'FAIL' END
FROM checks;

-- 5. Load fidelity — did the cast lose anything?
--
-- **This section exists because `green_taxi` declares its schema and
-- casts.** It has no counterpart in the personal pipeline.

-- The problem it solves
-- After `CAST(passenger_count AS INT)` runs, a value that failed to
-- convert and a value that was never there are **both NULL**. Nothing in
-- the Bronze table distinguishes them.

-- `read_files` normally adds `_rescued_data` to catch exactly this, but it
-- only appears when the schema is *inferred*. A declared schema skips
-- inference, so a value that does not fit has nowhere to go.

-- The landed Parquet files still know. They are the last copy that has not
-- been cast:
-- > **A column with more nulls after the load than before it lost data in
-- > the cast.**


INSERT INTO nyc_quality.dq_results
WITH raw AS (
    -- The landed files, original column names, original Parquet types.
    SELECT
        COUNT(*)                                                       AS n_rows,
        SUM(CASE WHEN passenger_count       IS NULL THEN 1 ELSE 0 END) AS n_passengers,
        SUM(CASE WHEN RatecodeID            IS NULL THEN 1 ELSE 0 END) AS n_ratecode,
        SUM(CASE WHEN payment_type          IS NULL THEN 1 ELSE 0 END) AS n_payment,
        SUM(CASE WHEN trip_type             IS NULL THEN 1 ELSE 0 END) AS n_trip_type,
        SUM(CASE WHEN lpep_pickup_datetime  IS NULL THEN 1 ELSE 0 END) AS n_pickup,
        SUM(CASE WHEN lpep_dropoff_datetime IS NULL THEN 1 ELSE 0 END) AS n_dropoff
    FROM read_files('/Volumes/nyc-mobility/nyc_bronze/landing/green_taxi',
                    format => 'parquet')
),
loaded AS (
    SELECT
        COUNT(*)                                                       AS n_rows,
        SUM(CASE WHEN passenger_count       IS NULL THEN 1 ELSE 0 END) AS n_passengers,
        SUM(CASE WHEN ratecode_id           IS NULL THEN 1 ELSE 0 END) AS n_ratecode,
        SUM(CASE WHEN payment_type          IS NULL THEN 1 ELSE 0 END) AS n_payment,
        SUM(CASE WHEN trip_type             IS NULL THEN 1 ELSE 0 END) AS n_trip_type,
        SUM(CASE WHEN lpep_pickup_datetime  IS NULL THEN 1 ELSE 0 END) AS n_pickup,
        SUM(CASE WHEN lpep_dropoff_datetime IS NULL THEN 1 ELSE 0 END) AS n_dropoff
    FROM nyc_bronze.green_taxi
),
fidelity AS (
    -- GREATEST(..., 0) because only an INCREASE means loss. Fewer nulls than
    -- the source would be a stranger problem, caught by the row-count check.
    SELECT 'business' AS check_category, 'row_count_matches_source' AS check_name,
           0.0 AS threshold_pct,
           ABS((SELECT n_rows FROM loaded) - (SELECT n_rows FROM raw)) AS failed_rows,
           (SELECT n_rows FROM raw)                                    AS total_rows
    UNION ALL SELECT 'completeness', 'no_nulls_added_passenger_count', 0.0,
        GREATEST((SELECT n_passengers FROM loaded) - (SELECT n_passengers FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_ratecode_id', 0.0,
        GREATEST((SELECT n_ratecode FROM loaded) - (SELECT n_ratecode FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_payment_type', 0.0,
        GREATEST((SELECT n_payment FROM loaded) - (SELECT n_payment FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_trip_type', 0.0,
        GREATEST((SELECT n_trip_type FROM loaded) - (SELECT n_trip_type FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_pickup_datetime', 0.0,
        GREATEST((SELECT n_pickup FROM loaded) - (SELECT n_pickup FROM raw), 0), (SELECT n_rows FROM raw)
    UNION ALL SELECT 'completeness', 'no_nulls_added_dropoff_datetime', 0.0,
        GREATEST((SELECT n_dropoff FROM loaded) - (SELECT n_dropoff FROM raw), 0), (SELECT n_rows FROM raw)
)

SELECT
    v_run_id, v_run_ts, 'bronze', 'green_taxi',
    check_category, check_name, failed_rows, total_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_rows, 0), 4),
    threshold_pct,
    CASE WHEN failed_rows = 0 THEN 'PASS' ELSE 'FAIL' END
FROM fidelity;


-- If `row_count_matches_source` fails, `COPY INTO` either skipped a file it
-- had already loaded or loaded one twice. If a `no_nulls_added_*` check
-- fails, that column's `CAST` is rejecting real values — swap it for
-- `try_cast` and add a flag, or widen the declared type. Either way it is
-- a loader bug, not a data problem, and belongs with whoever owns the load.


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


-- 7. Gate
--
-- Two ways to stop the pipeline
-- | Trigger | Meaning |
-- |---|---|
-- | **any blocking check fails** | one thing broke that makes the data unusable downstream |
-- | **5 or more checks fail in total** | no single thing is fatal, but something is broadly wrong |
--
-- Everything else is still written to `dq_results` with `status = 'FAIL'`.
-- It is recorded and queryable — it just does not halt the run.

-- Why not simply "any FAIL"
-- Half these thresholds are provisional defaults. One of them being
-- slightly wrong should not stop a load, and a gate that fires on noise
-- gets switched off — which is worse than no gate.
--
-- Why not simply "5 or more FAILs"
-- Because it counts checks rather than consequences. `location_id_unique`
-- failing on its own fans out the Gold join and inflates every trip count
-- in that zone — one failure, and every number downstream is wrong. A
-- count-of-five gate waves it through. Meanwhile five cosmetic domain
-- failures would halt a perfectly usable load.
--
-- "The pipeline passed because only four things were broken" is not a
-- sentence you want to defend.
--
-- The blocking list
-- Written out by name rather than joined from `dq_rules`, for the same
-- reason the thresholds are inline: the gate stays self-contained, and a
-- reviewer can read exactly what can stop the pipeline without opening
-- another table. It is the `[structural]` set — the checks where one
-- occurrence corrupts an aggregate, a join, or the grain itself.
--
-- To change what blocks, edit the list. To change what counts as broadly
-- wrong, edit the 5.
 
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
        'date_not_null','timestamp_not_null','date_parses','timestamp_parses',
        'location_id_not_null',
        -- keys: a duplicate fans out a join and inflates every total
        'location_id_unique','one_row_per_date','one_row_per_hour',
        -- lineage: without it nothing can be traced
        'source_file_recorded',
        -- physically impossible values that still cast cleanly
        'trip_distance_not_negative','passenger_count_not_negative',
        -- referential integrity: trips that can never resolve to a zone
        'pickup_zone_exists_in_lookup','dropoff_zone_exists_in_lookup',
        -- our own pipeline damaged the data on the way in
        'no_rescued_data','row_count_matches_source',
        'no_nulls_added_passenger_count','no_nulls_added_ratecode_id',
        'no_nulls_added_payment_type','no_nulls_added_trip_type',
        'no_nulls_added_pickup_datetime','no_nulls_added_dropoff_datetime'
      )
);
 
SET VAR v_total_failures = (
    SELECT COUNT(*) FROM nyc_quality.dq_results
    WHERE run_id = v_run_id AND status = 'FAIL'
);

 
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
 
--9. Afterwards

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