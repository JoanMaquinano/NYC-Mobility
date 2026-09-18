-- Data Quality Setup

--  **Run once.** Creates the two tables every quality check writes to.
-- 
--  Two tables:`dq_results` is the detail (one row per check per
--  run) and `dq_run_log` is the audit log (one row per layer per run).
--  Everything else — dashboards, trend lines, "which check fails most" —
--  is a query over these two, not another table.
-- 
--  The same two tables serve Bronze, Silver and Gold. The `layer` column
--  is what separates them, so adding Silver later means writing more rows,
--  not more tables.

SET TIME ZONE 'America/New_York';
CREATE SCHEMA IF NOT EXISTS `nyc-mobility`.nyc_quality;

USE CATALOG `nyc-mobility`;
USE SCHEMA nyc_quality;

-- dq_results — one row per check, per run

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
    status          STRING     COMMENT 'PASS | WARN | FAIL'
)
USING DELTA
COMMENT 'One row per data quality check per run, across all layers.';


-- dq run log - one row per layer per run
CREATE TABLE IF NOT EXISTS dq_run_log (
    run_id          STRING,
    run_ts          TIMESTAMP,
    layer           STRING,
    tables_checked  INT,
    checks_run      INT,
    checks_passed   INT,
    checks_warned   INT,
    checks_failed   INT,
    overall_status  STRING     COMMENT 'FAIL if any check failed, else WARN if any warned, else PASS',
    finished_at     TIMESTAMP
)
USING DELTA
COMMENT 'Audit log: one row per layer per pipeline run.';


--dq rule table
CREATE TABLE IF NOT EXISTS dq_rules (
    layer            STRING  COMMENT 'bronze | silver | gold',
    table_name       STRING,
    check_category   STRING  COMMENT 'completeness | uniqueness | validity | consistency | business',
    check_name       STRING  COMMENT 'matches check_name in dq_results',
    threshold_pct    DOUBLE  COMMENT 'documented here; the running copy lives inline in the check notebook',
    rule_description STRING  COMMENT 'what the check asserts, in one line',
    rationale        STRING  COMMENT 'basis tag plus why this rule and why this threshold',
    silver_action    STRING  COMMENT 'QUARANTINE | FLAG | IGNORE',
    blocking         BOOLEAN COMMENT 'TRUE if a FAIL on this rule stops the pipeline; mirrors the gate list in the check notebook',
    denominator_scope STRING COMMENT 'what total_rows counts: table_rows | scalar | vendor_rows | distinct_ids | source_rows | source_files | expected_days | expected_hours | observed_days'
)
USING DELTA
COMMENT 'Catalogue of every data quality rule, with the reasoning behind each threshold.';


-- The most recent run per layer, with its headline counts.
CREATE OR REPLACE VIEW `nyc-mobility`.nyc_quality.vw_latest_dq_run AS
SELECT r.*
FROM   `nyc-mobility`.nyc_quality.dq_run_log r
JOIN  (SELECT layer, MAX(run_ts) AS max_ts
       FROM   `nyc-mobility`.nyc_quality.dq_run_log
       GROUP  BY layer) m
  ON  r.layer = m.layer AND r.run_ts = m.max_ts;

-- Every check from the latest run of each layer.
CREATE OR REPLACE VIEW `nyc-mobility`.nyc_quality.vw_latest_dq_results AS
SELECT d.*
FROM   `nyc-mobility`.nyc_quality.dq_results d
JOIN  (SELECT layer, MAX(run_ts) AS max_ts
       FROM   `nyc-mobility`.nyc_quality.dq_results
       GROUP  BY layer) m
  ON  d.layer = m.layer AND d.run_ts = m.max_ts;


