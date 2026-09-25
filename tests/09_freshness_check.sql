-- Freshness — is the pipeline still running?
--

-- ## The gap this closes
-- Every other check in this project answers "is the data that arrived any
-- good". None of them answers "did anything arrive". A table that stopped
-- being loaded three weeks ago passes all 291 checks, because every row in it
-- is still perfectly valid -- there are just no new ones.
--
-- `dq_run_log` has the same blind spot from the other side: it only gets a row
-- when a run HAPPENS. A job that never started leaves no trace, and absence
-- cannot be detected from a table that only records presence. So the check has
-- to be time-based: not "did the run fail" but "how long since the last one".
--
-- ## Run it on a schedule, not after a load
--
-- Running it in the pipeline defeats the point -- it would only ever fire
-- immediately after a successful load, when freshness is guaranteed. Put it on
-- a daily Databricks schedule of its own. It reads four tables and writes ten
-- rows; it costs nothing to run often.

SET TIME ZONE 'UTC';
USE CATALOG nyc_mobility;

DECLARE OR REPLACE VARIABLE v_run_id  STRING;
DECLARE OR REPLACE VARIABLE v_run_ts  TIMESTAMP;
DECLARE OR REPLACE VARIABLE v_as_of   STRING;

SET VAR v_run_id = uuid();
SET VAR v_run_ts = current_timestamp();

-- The month the CHECK ran, not the month the data covers. Clear-down is on
-- (layer, batch_month) like every other layer, so re-running today replaces
-- today's verdict and the month keeps one row per check. `dq_run_log` keeps
-- every individual execution, which is where the day-by-day trend lives.
SET VAR v_as_of = date_format(current_date(), 'yyyy-MM');


-- ## Thresholds
--
-- These are an SLA, not a data rule, so they are stated as days rather than
-- percentages. The batch here is monthly, so a load is expected roughly every
-- 30 days; `v_sla_days` is set well past that because the failure this catches
-- is "nothing has happened in a long time", not "we are a day late".
--
-- Tune to the real cadence. On a daily feed this would be 2, not 45.

DECLARE OR REPLACE VARIABLE v_sla_days     INT;
DECLARE OR REPLACE VARIABLE v_qc_sla_days  INT;
DECLARE OR REPLACE VARIABLE v_lag_days     INT;

SET VAR v_sla_days    = 45;   -- a monthly load is late past this
SET VAR v_qc_sla_days = 45;   -- QC should run whenever a load does
SET VAR v_lag_days    = 2;    -- how far a downstream layer may trail the one above

DECLARE OR REPLACE VARIABLE v_strict_pct   DOUBLE;
DECLARE OR REPLACE VARIABLE v_advisory_pct DOUBLE;
SET VAR v_strict_pct   = 0.0;
SET VAR v_advisory_pct = 100.0;

DELETE FROM nyc_quality.dq_results
WHERE layer = 'freshness' AND batch_month = v_as_of;

DELETE FROM nyc_quality.dq_run_log
WHERE layer = 'freshness' AND batch_month = v_as_of;


-- # 1. Age of each layer
--
-- `failed_rows` carries the AGE IN DAYS, not a row count. That is a deliberate
-- departure from every other check in this project, and worth stating plainly:
-- the number in that column is what you want to read and trend, and
-- `threshold_pct` holds the SLA in days rather than a percentage.
--
-- `total_rows` is 1 throughout -- these are scalar facts about a table, not
-- rates over rows -- so `failed_pct` is meaningless here and is written as
-- NULL rather than a misleading 100.
--
-- A layer with no rows at all yields NULL from MAX(), which becomes SKIP: the
-- table is empty, which `table_not_empty` in its own layer already reports.
-- Saying it twice in two vocabularies helps nobody.

INSERT INTO nyc_quality.dq_results (
    run_id, run_ts, layer, table_name, check_category, check_name,
    failed_rows, total_rows, failed_pct, threshold_pct, status,
    batch_month, min_failed_rows, warn_pct
)
WITH ages AS (
    SELECT
        (SELECT datediff(current_date(), MAX(to_date(ingestion_time)))
         FROM   nyc_bronze.green_taxi)                        AS bronze_taxi,
        (SELECT datediff(current_date(), MAX(to_date(ingestion_timestamp)))
         FROM   nyc_bronze.weather)                           AS bronze_weather,
        (SELECT datediff(current_date(), MAX(to_date(silver_at)))
         FROM   nyc_silver.green_taxi_clean)                  AS silver_taxi,
        (SELECT datediff(current_date(), MAX(to_date(silver_at)))
         FROM   nyc_silver.weather_clean)                     AS silver_weather,
        (SELECT datediff(current_date(), MAX(to_date(created_at)))
         FROM   nyc_gold.fact_taxi_trip)                      AS gold_fact
),
checks AS (
    SELECT 'nyc_bronze.green_taxi'        AS table_name, 'bronze_taxi_fresh'    AS check_name, bronze_taxi    AS age FROM ages
    UNION ALL SELECT 'nyc_bronze.weather',              'bronze_weather_fresh',    bronze_weather FROM ages
    UNION ALL SELECT 'nyc_silver.green_taxi_clean',     'silver_taxi_fresh',       silver_taxi    FROM ages
    UNION ALL SELECT 'nyc_silver.weather_clean',        'silver_weather_fresh',    silver_weather FROM ages
    UNION ALL SELECT 'nyc_gold.fact_taxi_trip',         'gold_fact_fresh',         gold_fact      FROM ages
)
SELECT
    v_run_id, v_run_ts, 'freshness', table_name,
    'timeliness', check_name,
    age, 1, CAST(NULL AS DOUBLE),
    CAST(v_sla_days AS DOUBLE),
    CASE WHEN age IS NULL          THEN 'SKIP'
         WHEN age <= v_sla_days    THEN 'PASS'
         ELSE 'FAIL' END,
    v_as_of, 0, 0.0
FROM checks;


-- # 2. Did QC itself run?
--
-- The check that watches the watchers. Every layer writes to `dq_run_log`, so
-- the age of its newest row is how long since that layer was last checked.
--
-- This is the half of "execution" monitoring that `dq_run_log` cannot answer
-- on its own: the log tells you how the runs that happened went, and says
-- nothing at all about the ones that did not.

INSERT INTO nyc_quality.dq_results (
    run_id, run_ts, layer, table_name, check_category, check_name,
    failed_rows, total_rows, failed_pct, threshold_pct, status,
    batch_month, min_failed_rows, warn_pct
)
WITH last_run AS (
    SELECT layer AS checked_layer, datediff(current_date(), MAX(to_date(run_ts))) AS age
    FROM   nyc_quality.dq_run_log
    WHERE  layer IN ('preload', 'bronze', 'silver', 'gold', 'at_rest')
    GROUP  BY layer
),
-- An anti-join, so a layer that has NEVER run is reported rather than simply
-- absent from the results. "No row" and "ran today" must not look the same.
expected AS (
    SELECT * FROM VALUES ('preload'), ('bronze'), ('silver'), ('gold'), ('at_rest')
    AS t(checked_layer)
)
SELECT
    v_run_id, v_run_ts, 'freshness',
    concat('dq_run_log/', e.checked_layer),
    'timeliness', concat(e.checked_layer, '_qc_ran_recently'),
    COALESCE(r.age, 9999), 1, CAST(NULL AS DOUBLE),
    CAST(v_qc_sla_days AS DOUBLE),
    CASE WHEN r.age IS NULL            THEN 'FAIL'   -- never run at all
         WHEN r.age <= v_qc_sla_days   THEN 'PASS'
         ELSE 'FAIL' END,
    v_as_of, 0, 0.0
FROM       expected e
LEFT  JOIN last_run r ON r.checked_layer = e.checked_layer;


-- # 3. Are the layers in step?
--
-- The cross-layer signal, and the one a per-table age cannot give you.
--
-- Bronze loading while Silver stands still is the interesting failure: both
-- tables are individually fresh enough to pass section 1, every row in both is
-- valid, and the only symptom is that the gap between them keeps growing. A
-- broken task in the middle of a job looks exactly like this.
--
-- Measured in days of lag, tolerated at `v_lag_days`, because a load and the
-- transform after it legitimately land on either side of midnight.
--
-- Negative lag -- a downstream layer newer than the one above it -- is normal:
-- Silver is written after Bronze. GREATEST(..., 0) floors it so only real lag
-- is reported.

INSERT INTO nyc_quality.dq_results (
    run_id, run_ts, layer, table_name, check_category, check_name,
    failed_rows, total_rows, failed_pct, threshold_pct, status,
    batch_month, min_failed_rows, warn_pct
)
WITH t AS (
    SELECT
        (SELECT MAX(to_date(ingestion_time)) FROM nyc_bronze.green_taxi)       AS b_taxi,
        (SELECT MAX(to_date(silver_at))      FROM nyc_silver.green_taxi_clean) AS s_taxi,
        (SELECT MAX(to_date(ingestion_timestamp)) FROM nyc_bronze.weather)     AS b_wx,
        (SELECT MAX(to_date(silver_at))      FROM nyc_silver.weather_clean)    AS s_wx,
        (SELECT MAX(to_date(created_at))     FROM nyc_gold.fact_taxi_trip)     AS g_fact
),
checks AS (
    SELECT 'silver_taxi_keeps_up_with_bronze' AS check_name,
           GREATEST(datediff(b_taxi, s_taxi), 0) AS lag FROM t
    UNION ALL SELECT 'silver_weather_keeps_up_with_bronze',
           GREATEST(datediff(b_wx, s_wx), 0) FROM t
    UNION ALL SELECT 'gold_keeps_up_with_silver',
           GREATEST(datediff(s_taxi, g_fact), 0) FROM t
)
SELECT
    v_run_id, v_run_ts, 'freshness', 'pipeline',
    'timeliness', check_name,
    lag, 1, CAST(NULL AS DOUBLE),
    CAST(v_lag_days AS DOUBLE),
    CASE WHEN lag IS NULL        THEN 'SKIP'
         WHEN lag <= v_lag_days  THEN 'PASS'
         ELSE 'FAIL' END,
    v_as_of, 0, 0.0
FROM checks;


-- # 4. Audit log

INSERT INTO nyc_quality.dq_run_log (
    run_id, run_ts, layer, tables_checked, checks_run,
    checks_passed, checks_warned, checks_failed, overall_status, finished_at,
    batch_month, checks_skipped
)
SELECT
    v_run_id, v_run_ts, 'freshness',
    COUNT(DISTINCT table_name),
    COUNT(*),
    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END),
    SUM(CASE WHEN status = 'WARN' THEN 1 ELSE 0 END),
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END),
    CASE WHEN SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) > 0 THEN 'FAIL'
         WHEN SUM(CASE WHEN status = 'WARN' THEN 1 ELSE 0 END) > 0 THEN 'WARN'
         ELSE 'PASS' END,
    current_timestamp(),
    v_as_of,
    SUM(CASE WHEN status = 'SKIP' THEN 1 ELSE 0 END)
FROM   nyc_quality.dq_results
WHERE  layer = 'freshness' AND batch_month = v_as_of;


-- # 5. Results
--
-- `failed_rows` is DAYS here, and `threshold_pct` is the SLA in days. Aliased
-- in the output so nobody reads them as counts and percentages.

SELECT table_name,
       check_name,
       failed_rows   AS age_days,
       threshold_pct AS sla_days,
       status
FROM   nyc_quality.dq_results
WHERE  layer = 'freshness' AND batch_month = v_as_of
ORDER  BY CASE status WHEN 'FAIL' THEN 0 WHEN 'WARN' THEN 1 ELSE 2 END,
          failed_rows DESC;

-- The one-line answer, for a dashboard tile or an alert body.
SELECT CASE
    WHEN (SELECT COUNT(*) FROM nyc_quality.dq_results
          WHERE layer = 'freshness' AND batch_month = v_as_of AND status = 'FAIL') = 0
      THEN 'pipeline is current'
    ELSE CONCAT('STALE: ',
                (SELECT concat_ws(', ', collect_list(check_name))
                 FROM   nyc_quality.dq_results
                 WHERE  layer = 'freshness' AND batch_month = v_as_of
                   AND  status = 'FAIL'))
END AS freshness;

