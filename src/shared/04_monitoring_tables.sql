%sql
-- Data Quality Setup (per batch)
--
--  **Run once.** v1 users: the ALTERs below are additive, nothing is dropped
--  and no existing row is lost.

SET TIME ZONE 'America/New_York';
CREATE SCHEMA IF NOT EXISTS nyc_mobility.nyc_quality;

USE CATALOG nyc_mobility;
USE SCHEMA nyc_quality;

-- dq_results — one row per check, per batch

CREATE TABLE IF NOT EXISTS dq_results (
    run_id          STRING     COMMENT 'groups every check from one pipeline run',
    run_ts          TIMESTAMP  COMMENT 'when the run started',
    layer           STRING     COMMENT 'bronze | silver | gold',
    table_name      STRING     COMMENT 'table the check ran against',
    check_category  STRING     COMMENT 'completeness | uniqueness | validity | consistency | business',
    check_name      STRING     COMMENT 'what is being asserted, phrased positively',
    failed_rows     BIGINT     COMMENT 'rows breaking the rule',
    total_rows      BIGINT     COMMENT 'rows examined',
    failed_pct      DOUBLE     COMMENT '100 * failed_rows / total_rows',
    threshold_pct   DOUBLE     COMMENT 'tolerated failure rate; 0 means none tolerated',
    status          STRING     COMMENT 'PASS | WARN | FAIL | SKIP'
    -- threshold_pct is the FAIL line; warn_pct (added below) is the WARN line
)
USING DELTA
COMMENT 'One row per data quality check per batch, across all layers.';

-- Two additive columns.
--
--   batch_month      which month the check was scoped to. Without it, every
--                    query over this table is a mix of months and the
--                    per-month dashboard cannot exist.
--
--   min_failed_rows  an absolute floor under the percentage. A 0% threshold
--                    on a 44,000-row table fails on one row, which is how a
--                    gate earns a reputation for noise. The floor says
--                    "below this many rows it is a WARN regardless of the
--                    percentage", so a rule can stay strict about RATE and
--                    still not stop the pipeline over a single bad row.
ALTER TABLE dq_results ADD COLUMNS (
    batch_month     STRING COMMENT 'YYYY-MM this check was scoped to',
    min_failed_rows BIGINT COMMENT 'at or below this many failed rows, never FAIL',
    warn_pct        DOUBLE COMMENT 'at or below this rate the check PASSes; above it WARNs'
);

-- dq_run_log — one row per layer per run
CREATE TABLE IF NOT EXISTS dq_run_log (
    run_id          STRING,
    run_ts          TIMESTAMP,
    layer           STRING,
    tables_checked  INT,
    checks_run      INT,
    checks_passed   INT,
    checks_warned   INT,
    checks_failed   INT,
    overall_status  STRING     COMMENT 'FAIL if a blocking check failed, else WARN if any warned, else PASS',
    finished_at     TIMESTAMP
)
USING DELTA
COMMENT 'Audit log: one row per layer per batch.';

ALTER TABLE dq_run_log ADD COLUMNS (
    batch_month     STRING COMMENT 'YYYY-MM this run covered',
    checks_skipped  INT    COMMENT 'checks not evaluated because the batch was empty'
);

-- dq_rules — catalogue of rules and the reasoning behind each threshold
CREATE TABLE IF NOT EXISTS dq_rules (
    layer            STRING  COMMENT 'bronze | silver | gold',
    table_name       STRING,
    check_category   STRING,
    check_name       STRING  COMMENT 'matches check_name in dq_results',
    threshold_pct    DOUBLE  COMMENT 'documented here; the running copy lives inline in the check notebook',
    rule_description STRING,
    rationale        STRING  COMMENT 'basis tag plus why this rule and why this threshold',
    silver_action    STRING  COMMENT 'QUARANTINE | FLAG | IGNORE',
    blocking         BOOLEAN COMMENT 'TRUE if a FAIL on this rule stops the pipeline',
    denominator_scope STRING
)
USING DELTA
COMMENT 'Catalogue of every data quality rule, with the reasoning behind each threshold.';

ALTER TABLE dq_rules ADD COLUMNS (
    min_failed_rows BIGINT COMMENT 'absolute floor that accompanies threshold_pct',
    warn_pct        DOUBLE COMMENT 'lower of the two thresholds; below it the check PASSes'
);


-- ## Views
--
-- First two answers: "how did the batch I just ran do"; 
-- the third answers "how has this check behaved across every
-- batch", which is the per-month dashboard.

-- 1. The run log row for the most recent run of each layer.
CREATE OR REPLACE VIEW vw_latest_dq_run AS
SELECT r.*
FROM   dq_run_log r
JOIN  (SELECT layer, MAX(run_ts) AS max_ts FROM dq_run_log GROUP BY layer) m
  ON  r.layer = m.layer AND r.run_ts = m.max_ts;

-- 2. Every check from the batch that ran most recently, per layer.
--    This is what "only shows the results of the current batch" means:
--    the table keeps every month, the view shows the one just run.
CREATE OR REPLACE VIEW vw_current_batch_dq AS
SELECT d.*
FROM   dq_results d
JOIN  (SELECT layer, MAX(run_ts) AS max_ts FROM dq_results GROUP BY layer) m
  ON  d.layer = m.layer AND d.run_ts = m.max_ts;

-- 3. All batches, one row per check per month — the dashboard source.
--    A check that was re-run for a month appears once: the newest run for
--    that (layer, batch_month, table, check) wins.
CREATE OR REPLACE VIEW vw_dq_by_month AS
SELECT * EXCEPT (rn)
FROM (
    SELECT d.*,
           ROW_NUMBER() OVER (
               PARTITION BY layer, batch_month, table_name, check_name
               ORDER BY run_ts DESC) AS rn
    FROM dq_results d
)
WHERE rn = 1;
