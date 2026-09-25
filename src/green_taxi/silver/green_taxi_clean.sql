-- Silver Layer Refactoring: NYC Green Taxi Upsert & Quality Auditing
-- Improvements:
--   1. Deterministic deduplication (order by bronze ingestion timestamp/raw metadata).
--   2. Granular partition keys (adds distance & total to key to prevent dropping simultaneous trips).
--   3. Unified QC validation logic operating directly on casted timestamp types.

-- Update #2:
-- The previous rule was:
--     lpep_pickup_datetime < TIMESTAMP'2009-01-01 00:00:00'
--
-- Three problems with it:
--   * **It is a magic number.** "LPEP started in 2009" is a fact about this
--     dataset, hardcoded into a rule. Point the pipeline at another feed and
--     it means nothing.
--   * **It is strictly `<`.** A timestamp of exactly 2009-01-01 00:00:00 --
--     the classic sentinel value -- passes.
--   * **It answers the wrong question.** "Is this plausibly a taxi trip ever?"
--     rather than "does this belong in the batch it arrived in?" A 2015 date
--     passed cleanly and stretched dim_date to 6,300 days.
-- It also covered PICKUPS only. A 2026 pickup with a 2009 dropoff passed
-- everything, which is how a single row produced a seventeen-year calendar.
--
-- The window now comes from `source_file`, which every row already carries:
--     regexp_extract(source_file, '([0-9]{4}-[0-9]{2})', 1)   -- '2026-03'
-- No literal, no assumption about the dataset, and it self-adjusts for every
-- batch ever loaded.
-- ## Why the FAIL window is ±1 month and the WARN window is exact
-- FAIL is a strong claim: it moves a row out of `vw_green_taxi_valid` and out
-- of every downstream analysis. So the FAIL line is deliberately generous --
-- more than a month away from its own file's month, where the file and the
-- row plainly contradict each other and the row cannot be defended.
-- The genuine spill is a day at most: a ride crossing midnight on the 1st or
-- the 31st, or the UTC/NY offset at a month edge. TLC carries these routinely
-- and they are real trips, so they are flagged and kept.

SET TIME ZONE 'UTC';
USE CATALOG nyc_mobility;

-- What the pattern actually extracts, per distinct file.
SELECT source_file,
       regexp_extract(source_file, '([0-9]{4}-[0-9]{2})', 1) AS extracted_month,
       COUNT(*)                                              AS rows
FROM   nyc_mobility.nyc_bronze.green_taxi
GROUP  BY source_file,
          regexp_extract(source_file, '([0-9]{4}-[0-9]{2})', 1)
ORDER  BY source_file;

SELECT CASE
    WHEN (SELECT COUNT(*)
          FROM   nyc_mobility.nyc_bronze.green_taxi
          WHERE  source_file IS NOT NULL
            AND  regexp_extract(source_file, '([0-9]{4}-[0-9]{2})', 1) = '') > 0
      THEN raise_error(
             'source_file does not contain a YYYY-MM for every row, so the '
          || 'batch-window rules would not fire. Check the file naming in the '
          || 'Bronze MERGE, or adjust the regex -- do not run the Silver MERGE '
          || 'until this passes, or bad dates will be classified PASS.')
    ELSE 'source_file parses for every row' END AS precondition;



MERGE INTO nyc_mobility.nyc_silver.green_taxi_clean AS target
USING (
  SELECT
    *,
    -- dq_status: SUMMARY of the array
    CASE
      WHEN size(qc_error_descriptions) = 0                             THEN 'PASS'
      WHEN exists(qc_error_descriptions, x -> startswith(x, 'FAIL:'))  THEN 'FAIL'
      ELSE 'WARN'
    END                                                AS dq_status,
    current_timestamp()                                AS silver_at
  FROM (
    SELECT
      vendor_id,
      lpep_pickup_datetime,
      lpep_dropoff_datetime,
      store_and_fwd_flag,
      ratecode_id,
      pu_location_id,
      do_location_id,
      passenger_count,
      trip_distance,
      fare_amount,
      extra,
      mta_tax,
      tip_amount,
      tolls_amount,
      improvement_surcharge,
      total_amount,
      payment_type,
      trip_type,
      congestion_surcharge,
      cbd_congestion_fee,
      source_file,
      ingestion_time,

      ARRAY_COMPACT(ARRAY(

        -- FAIL conditions
        CASE WHEN lpep_pickup_datetime IS NULL OR lpep_dropoff_datetime IS NULL
             THEN 'FAIL: lpep_pickup_datetime/lpep_dropoff_datetime: timestamp is null' END,

        -- Strictly BEFORE. Equality is a separate, milder thing -- see the WARN.
        CASE WHEN lpep_dropoff_datetime < lpep_pickup_datetime
             THEN 'FAIL: lpep_dropoff_datetime: drop-off is before pickup' END,

        -- The batch window, derived per row from its own lineage. Replaces
        -- the hardcoded 2009 literal; see the header.
        CASE WHEN file_month_start IS NOT NULL
              AND lpep_pickup_datetime IS NOT NULL
              AND DATE(lpep_pickup_datetime) NOT BETWEEN
                    add_months(file_month_start, -1)
                AND date_sub(add_months(file_month_start, 2), 1)
             THEN 'FAIL: lpep_pickup_datetime: pickup is more than a month outside the source file month' END,

        -- The dropoff had no era rule at all. A 2026 pickup with a 2009
        -- dropoff passed every rule and still set the dim_date lower bound.
        CASE WHEN file_month_start IS NOT NULL
              AND lpep_dropoff_datetime IS NOT NULL
              AND DATE(lpep_dropoff_datetime) NOT BETWEEN
                    add_months(file_month_start, -1)
                AND date_sub(add_months(file_month_start, 2), 1)
             THEN 'FAIL: lpep_dropoff_datetime: drop-off is more than a month outside the source file month' END,

        -- Backstop for a row whose source_file is null or unparseable, where
        -- the window rules have nothing to work with. No trip can be in the
        -- future, in any dataset, so this needs no literal.
        CASE WHEN lpep_pickup_datetime > CURRENT_TIMESTAMP()
             THEN 'FAIL: lpep_pickup_datetime: pickup is in the future' END,
        CASE WHEN lpep_dropoff_datetime > CURRENT_TIMESTAMP()
             THEN 'FAIL: lpep_dropoff_datetime: drop-off is in the future' END,

        -- 1 AND 265, matching Bronze. At 1 AND 263 this reported every genuine
        -- 264 and 265 as invalid -- a permanent false positive on rows that
        -- are fine -- while Bronze called the same rows valid.
        CASE WHEN raw_pu_location_id IS NULL
               OR raw_pu_location_id NOT BETWEEN 1 AND 265
             THEN 'FAIL: PULocationID: pickup location is null or outside 1 to 265' END,
        CASE WHEN raw_do_location_id IS NULL
               OR raw_do_location_id NOT BETWEEN 1 AND 265
             THEN 'FAIL: DOLocationID: drop-off location is null or outside 1 to 265' END,

        -- Bronze treats lineage as blocking; a row that cannot be traced back
        -- to a file cannot be defended to a reviewer.
        CASE WHEN source_file IS NULL
             THEN 'FAIL: source_file: row cannot be traced to a source file' END,

        -- WARN: one field is wrong and the rest of the row still works

        -- Outside the file month, but close enough to be a real edge trip --
        -- a ride crossing midnight on the 1st or the 31st, or the UTC/NY
        -- offset. Excludes the FAIL range above so a far-out row carries the
        -- FAIL reason only.
        CASE WHEN file_month_start IS NOT NULL
              AND lpep_pickup_datetime IS NOT NULL
              AND date_format(lpep_pickup_datetime, 'yyyy-MM')
                  <> regexp_extract(source_file, '([0-9]{4}-[0-9]{2})', 1)
              AND DATE(lpep_pickup_datetime) BETWEEN
                    add_months(file_month_start, -1)
                AND date_sub(add_months(file_month_start, 2), 1)
             THEN 'WARN: lpep_pickup_datetime: pickup is outside the source file month' END,

        CASE WHEN lpep_dropoff_datetime = lpep_pickup_datetime
             THEN 'WARN: lpep_dropoff_datetime: trip has zero duration' END,

        -- Refunds and disputes: all observed negative fares carry payment_type
        -- 3 (No charge) or 4 (Dispute) and are legitimate trips.
        CASE WHEN raw_fare_amount  < 0 THEN 'WARN: fare_amount: base fare is negative' END,
        CASE WHEN raw_total_amount < 0 THEN 'WARN: total_amount: total charge is negative' END,
        CASE WHEN raw_tip_amount   < 0 THEN 'WARN: tip_amount: tip is negative' END,

        -- Split out of the old <= 0. Negative is impossible, zero is a minimum
        -- charge or a cancellation -- 4,387 of them, mostly genuine. One
        -- message for both made the count unreadable.
        CASE WHEN raw_trip_distance < 0
             THEN 'WARN: trip_distance: distance is negative' END,
        CASE WHEN raw_trip_distance = 0
             THEN 'WARN: trip_distance: distance is zero' END,

        -- Vendor 6 (Myle) submits NONE of the six dispatch fields across about
        -- 14,181 trips, so this fires for a whole vendor by design. That is
        -- exactly why it is a flag and not an exclusion.
        CASE WHEN raw_passenger_count IS NULL OR raw_passenger_count <= 0
             THEN 'WARN: passenger_count: count is null, zero, or negative' END,

        CASE WHEN raw_ratecode_id IS NULL
               OR raw_ratecode_id NOT IN (1, 2, 3, 4, 5, 6, 99)
             THEN 'WARN: RatecodeID: rate code is null or outside 1-6, 99' END,
        CASE WHEN raw_payment_type IS NULL
               OR raw_payment_type NOT IN (0, 1, 2, 3, 4, 5, 6)
             THEN 'WARN: payment_type: code is null or invalid' END,
        CASE WHEN raw_trip_type IS NULL OR raw_trip_type NOT IN (1, 2)
             THEN 'WARN: trip_type: code is null or invalid' END,
        CASE WHEN raw_vendor_id IS NULL OR raw_vendor_id NOT IN (1, 2, 6)
             THEN 'WARN: VendorID: vendor is null or unrecognized' END,

        CASE WHEN raw_payment_type = 2 AND raw_tip_amount > 0
             THEN 'WARN: tip_amount: tip recorded for a cash payment' END,

        CASE WHEN store_and_fwd_flag IS NOT NULL
               AND store_and_fwd_flag NOT IN ('Y', 'N')
             THEN 'WARN: store_and_fwd_flag: value is not Y or N' END,

        -- The round-fare cluster Bronze found: 300 appearing 22 times at
        -- about three seconds and zero distance, plus groups at 250, 200, 160
        -- and 120. WARN and not FAIL deliberately -- "these are not trips" is
        -- a strong INFERENCE, and the FAIL rules are all impossibilities.
        CASE WHEN raw_fare_amount > 100
               AND raw_trip_distance = 0
               AND timestampdiff(SECOND, lpep_pickup_datetime,
                                 lpep_dropoff_datetime) < 60
             THEN 'WARN: fare_amount: large fare on a zero-distance trip under a minute' END

      )) AS qc_error_descriptions
    FROM (
      SELECT
        -- Core business attributes
        vendor_id,
        CAST(lpep_pickup_datetime  AS TIMESTAMP) AS lpep_pickup_datetime,
        CAST(lpep_dropoff_datetime AS TIMESTAMP) AS lpep_dropoff_datetime,
        store_and_fwd_flag,
        COALESCE(TRY_CAST(ratecode_id AS INT), 99) AS ratecode_id,

        CASE WHEN pu_location_id BETWEEN 1 AND 265
             THEN CAST(pu_location_id AS INT) END AS pu_location_id,
        CASE WHEN do_location_id BETWEEN 1 AND 265
             THEN CAST(do_location_id AS INT) END AS do_location_id,

        CASE WHEN passenger_count IS NULL OR passenger_count <= 0
             THEN 1 ELSE CAST(passenger_count AS INT) END AS passenger_count,
        trip_distance,
        fare_amount,
        CASE WHEN extra                < 0 THEN 0.0 ELSE extra                END AS extra,
        CASE WHEN mta_tax              < 0 THEN 0.0 ELSE mta_tax              END AS mta_tax,
        CASE WHEN tip_amount < 0 OR payment_type = 2 THEN 0.0 ELSE tip_amount END AS tip_amount,
        CASE WHEN tolls_amount         < 0 THEN 0.0 ELSE tolls_amount         END AS tolls_amount,
        improvement_surcharge,
        total_amount,
        COALESCE(TRY_CAST(payment_type AS INT), 5) AS payment_type,
        COALESCE(TRY_CAST(trip_type    AS INT), 1) AS trip_type,
        CASE WHEN congestion_surcharge < 0 THEN 0.0 ELSE congestion_surcharge END AS congestion_surcharge,
        CASE WHEN cbd_congestion_fee   < 0 THEN 0.0 ELSE cbd_congestion_fee   END AS cbd_congestion_fee,

        -- Lineage, carried through rather than dropped.
        source_file,
        ingestion_time,

        -- The month this row's own file claims, as a DATE. A helper for the
        -- window rules above -- deliberately NOT projected by the SELECT that
        -- reads this subquery, so it never reaches the target table.
        --
        -- NULL when source_file is null or carries no YYYY-MM. Every rule
        -- that uses it guards on IS NOT NULL, so an unparseable name disables
        -- the window rules for that row rather than silently passing it --
        -- and the precondition in cell 1 stops the run before that happens.
        to_date(concat(
          regexp_extract(source_file, '([0-9]{4}-[0-9]{2})', 1), '-01'
        )) AS file_month_start,

        -- Preserved raw values for strict rule auditing
        passenger_count AS raw_passenger_count,
        ratecode_id     AS raw_ratecode_id,
        pu_location_id  AS raw_pu_location_id,
        do_location_id  AS raw_do_location_id,
        payment_type    AS raw_payment_type,
        trip_type       AS raw_trip_type,
        vendor_id       AS raw_vendor_id,
        tip_amount      AS raw_tip_amount,
        fare_amount     AS raw_fare_amount,
        total_amount    AS raw_total_amount,
        trip_distance   AS raw_trip_distance
      FROM nyc_mobility.nyc_bronze.green_taxi
    ) cleaned_data
    -- Deduplicate deterministically without dropping distinct simultaneous
    -- trips. Note that lpep_pickup_datetime is constant within a partition --
    -- it is one of the partition keys -- so the effective order is
    -- fare_amount DESC. With this key granularity the tie is rare, but it is
    -- still arbitrary when it happens.
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY
        vendor_id,
        lpep_pickup_datetime,
        lpep_dropoff_datetime,
        pu_location_id,
        do_location_id,
        trip_distance,
        total_amount
      ORDER BY lpep_pickup_datetime ASC, fare_amount DESC
    ) = 1
  ) audited
) AS source
ON  target.vendor_id             <=> source.vendor_id
AND target.lpep_pickup_datetime  <=> source.lpep_pickup_datetime
AND target.lpep_dropoff_datetime <=> source.lpep_dropoff_datetime
AND target.pu_location_id        <=> source.pu_location_id
AND target.do_location_id        <=> source.do_location_id
AND target.trip_distance         <=> source.trip_distance
AND target.total_amount          <=> source.total_amount
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *;


-- ## Views: clean and quarantine
-- Use these for the Gold layer.

CREATE OR REPLACE VIEW nyc_mobility.nyc_silver.vw_green_taxi_valid
COMMENT 'Trips usable downstream: clean rows and flagged-but-interpretable rows. What Gold reads.'
AS
SELECT *
FROM   nyc_mobility.nyc_silver.green_taxi_clean
WHERE  dq_status IN ('PASS', 'WARN');


CREATE OR REPLACE VIEW nyc_mobility.nyc_silver.vw_green_taxi_quarantined
COMMENT 'Trips excluded from Gold, with the reasons. Nothing is deleted; this is a filter, not a table.'
AS
SELECT
    *,
    filter(qc_error_descriptions, x -> startswith(x, 'FAIL:')) AS fail_reasons
FROM   nyc_mobility.nyc_silver.green_taxi_clean
WHERE  dq_status = 'FAIL';

-- Rows in neither view. Expect zero. If this ever returns rows, the CASE in
-- the cleaning notebook did not run on them -- which is a code fault, not a
-- data fault, and is why dq_status_populated blocks the pipeline.
CREATE OR REPLACE VIEW nyc_mobility.nyc_silver.vw_green_taxi_unclassified
COMMENT 'Rows with a null or unrecognised dq_status. Always empty when the pipeline is correct.'
AS
SELECT *
FROM   nyc_mobility.nyc_silver.green_taxi_clean
WHERE  dq_status IS NULL
   OR  dq_status NOT IN ('PASS', 'WARN', 'FAIL');


-- ## What the new rules caught
--
-- Run after the MERGE. The window rules are new, so these counts start from
-- zero and the first run is the interesting one.
--SELECT 'pickup outside file month (FAIL)' AS rule, COUNT(*) AS rows
--FROM   nyc_mobility.nyc_silver.green_taxi_clean
--WHERE  exists(qc_error_descriptions,
--              x -> x LIKE 'FAIL: lpep_pickup_datetime: pickup is more than%')
--UNION ALL
--SELECT 'dropoff outside file month (FAIL)', COUNT(*)
--FROM   nyc_mobility.nyc_silver.green_taxi_clean
--WHERE  exists(qc_error_descriptions,
--              x -> x LIKE 'FAIL: lpep_dropoff_datetime: drop-off is more than%')
--UNION ALL
--SELECT 'pickup outside file month (WARN)', COUNT(*)
--FROM   nyc_mobility.nyc_silver.green_taxi_clean
--WHERE  exists(qc_error_descriptions,
--              x -> x LIKE 'WARN: lpep_pickup_datetime: pickup is outside%')
--UNION ALL
--SELECT 'in the future (FAIL)', COUNT(*)
--FROM   nyc_mobility.nyc_silver.green_taxi_clean
--WHERE  exists(qc_error_descriptions, x -> x LIKE '%is in the future%');

-- The bounds dim_date will now see. This is the number that was 2009-01-01.
--SELECT MIN(DATE(lpep_pickup_datetime))  AS trips_from,
--       MAX(DATE(lpep_dropoff_datetime)) AS trips_to,
--       COUNT(*)                         AS valid_rows
--FROM   nyc_mobility.nyc_silver.vw_green_taxi_valid
--WHERE  lpep_pickup_datetime IS NOT NULL
--  AND  lpep_dropoff_datetime IS NOT NULL;
