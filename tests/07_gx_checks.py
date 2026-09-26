"""Run the pipeline's quality rules through Great Expectations (GX).

Flow: Spark metrics -> configured GX suites -> Delta results -> GX release gate.
Only small aggregate results reach pandas; business data stays in Spark.
Add this file under src/quality/ and nyc_mobility_gx_job.yml under resources/jobs/.
No supporting Python or SQL files are required. Databricks provides Spark;
this script installs its GX dependency if needed (package-index access required).

The job runs this file three times, with --layer bronze, silver, and gold.
Results use the existing nyc_quality tables (initialize them with the existing
src/shared/04_monitoring_tables.sql if needed). Bronze and Silver retain their
report-only defaults; set their gx_*_enforce job parameters to true to block.
The new workflow uses the same tables as the original: run them separately.
"""

import argparse
import importlib
import logging
import math
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version

# 0. Install the dependency before importing it. GX also installs pandas and its
#    other dependencies. Spark comes from Databricks; do not install PySpark here.
GX_VERSION = "1.23.1"


def ensure_dependencies():
    """Install the pinned GX version only when the environment needs it."""
    try:
        if version("great-expectations") == GX_VERSION:
            return
    except PackageNotFoundError:
        pass
    print(f"Installing great-expectations=={GX_VERSION}...")
    # Use this task's Python interpreter. A failed installation stops the task.
    subprocess.check_call(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            f"great-expectations=={GX_VERSION}",
        ]
    )
    importlib.invalidate_caches()


if __name__ == "__main__":
    ensure_dependencies()

import great_expectations as gx
import pandas as pd

LOGGER = logging.getLogger(__name__)
DEFAULT_SOURCE = "/Volumes/workspace/default/ftw-b12-de/groups/week-08/group-d/green-taxi/"
DEFAULT_ENFORCEMENT = {"bronze": False, "silver": False, "gold": True}


# Default blocking rules; dq_rules may override these flags.
BLOCKING_CHECKS = {
    "bronze": {
        "pickup_datetime_not_null",
        "dropoff_datetime_not_null",
        "pickup_zone_not_null",
        "dropoff_zone_not_null",
        "date_not_null",
        "date_parses",
        "location_id_not_null",
        "table_not_empty",
        "location_id_unique",
        "one_row_per_hour",
        "source_file_recorded",
        "trip_distance_not_negative",
        "passenger_count_not_negative",
        "pickup_zone_exists_in_lookup",
        "dropoff_zone_exists_in_lookup",
        "row_count_matches_source",
        "every_landed_file_is_loaded",
        "no_nulls_added_vendor_id",
        "no_nulls_added_pu_location_id",
        "no_nulls_added_do_location_id",
        "no_nulls_added_ratecode_id",
        "no_nulls_added_payment_type",
        "no_nulls_added_trip_type",
        "no_nulls_added_passenger_count",
        "no_nulls_added_store_and_fwd_flag",
        "no_nulls_added_pickup_datetime",
        "no_nulls_added_dropoff_datetime",
        "no_nulls_added_trip_distance",
        "no_nulls_added_fare_amount",
        "no_nulls_added_total_amount",
        "no_nulls_added_extra",
        "no_nulls_added_mta_tax",
        "no_nulls_added_tip_amount",
        "no_nulls_added_tolls_amount",
        "no_nulls_added_improvement_surcharge",
        "no_nulls_added_congestion_surcharge",
    },
    "silver": {
        "table_not_empty",
        "dq_status_populated",
        "qc_array_not_null",
        "source_file_recorded",
        "silver_at_recorded",
        "weather_hour_not_null",
        "one_row_per_merge_key",
        "one_row_per_hour",
        "location_id_unique",
        "dq_status_in_domain",
        "qc_entries_carry_severity_prefix",
        "zones_within_1_to_265",
        "location_id_in_range",
        "pass_rows_carry_no_issues",
        "fail_rows_carry_a_fail_issue",
        "warn_rows_carry_only_warn_issues",
        "null_timestamps_are_quarantined",
        "reversed_trips_are_quarantined",
        "unresolvable_zones_are_quarantined",
        "out_of_era_pickups_are_quarantined",
        "untraceable_rows_are_quarantined",
        "rows_reconcile_with_bronze",
        "revenue_preserved",
        "every_bronze_file_present",
        "pickup_zone_exists_in_lookup",
        "dropoff_zone_exists_in_lookup",
        "location_id_not_null",
        "lookup_has_265_zones",
        "two_unknown_zones_present",
        "all_expected_days_present",
    },
    "gold": {
        "table_not_empty",
        "trip_key_not_null",
        "one_row_per_trip_key",
        "date_key_not_null",
        "date_key_unique",
        "weather_key_not_null",
        "weather_key_unique",
        "location_id_not_null",
        "location_id_unique",
        "pickup_zone_resolves",
        "dropoff_zone_resolves",
        "rows_reconcile_with_silver",
        "revenue_preserved",
    },
}


def quote_identifier(value):
    """Catalog names may contain hyphens; never interpolate an unquoted name."""
    return "`" + value.replace("`", "``") + "`"


# 1. Compute shared metrics, keeping the original business predicates in SQL.
#    These queries only SELECT counts. They do not assign PASS/FAIL or write results.
def collect_metrics(spark, layer, source_path):
    metrics = []
    for group_name, query in METRIC_QUERIES[layer]:
        parameters = {"source_path": source_path} if ":source_path" in query else {}
        rows = spark.sql(query, args=parameters).collect()
        if not rows:
            raise ValueError(f"No quality metrics returned by {group_name}")
        metrics.extend(row.asDict() for row in rows)
        LOGGER.info("%s: collected %s checks", group_name, len(rows))
    return metrics


def configure_checks(metrics, layer, overrides=()):
    """Validate rule configuration and prepare the values GX will evaluate.

    SQL supplies default thresholds; dq_rules can override threshold and blocking.
    Unknown catalogue rows are ignored because that table also documents other rules.
    Duplicate matching entries and invalid settings fail loudly.
    """
    metric_keys = {(m["table_name"], m["check_name"]) for m in metrics}
    configured = {}
    for rule in overrides:
        key = (rule["table_name"], rule["check_name"])
        if key not in metric_keys:
            continue
        if key in configured:
            raise ValueError(f"Duplicate dq_rules entry: {key}")
        configured[key] = rule

    checks, seen = [], set()
    for metric in metrics:
        check = dict(metric)
        key = (check["table_name"], check["check_name"])
        if key in seen:
            raise ValueError(f"Duplicate metric: {key}")
        seen.add(key)
        check["blocking"] = check["check_name"] in BLOCKING_CHECKS[layer]
        if key in configured:
            rule = configured[key]
            if not isinstance(rule["blocking"], bool):
                raise ValueError(f"Missing/invalid blocking flag: {key}")
            check.update(threshold_pct=rule["threshold_pct"], blocking=rule["blocking"])
        if check["threshold_pct"] is None:
            raise ValueError(f"Missing threshold: {key}")
        threshold = float(check["threshold_pct"])
        if not math.isfinite(threshold) or not 0 <= threshold <= 100:
            raise ValueError(f"Threshold must be between 0 and 100: {key}")
        check["threshold_pct"] = threshold

        # SUM over an empty table is NULL, whereas COUNT is zero. Normalize only
        # that case. A missing metric on a nonempty table must never become PASS.
        total, failed = check["total_rows"], check["failed_rows"]
        if total == 0 and failed is None:
            failed = 0
        if total is None or failed is None or total < 0 or failed < 0:
            raise ValueError(f"Missing or negative counts: {key}")
        if int(total) != total or int(failed) != failed:
            raise ValueError(f"Counts must be whole numbers: {key}")
        check.update(total_rows=int(total), failed_rows=int(failed))
        # Counts may exceed the denominator: for example, extra loaded rows
        # relative to source rows. Do not cap percentages or hide that defect.
        check["failed_pct"] = 100.0 * failed / total if total else (0.0 if failed == 0 else None)
        check["gx_value"] = check["failed_pct"] if check["failed_pct"] is not None else math.inf
        checks.append(check)

    missing = BLOCKING_CHECKS[layer] - {c["check_name"] for c in checks}
    if missing:
        raise ValueError(f"Blocking rules produced no metrics: {sorted(missing)}")
    return checks


# 2. GX owns the pass/fail decision. One suite is built for each table and layer.
def evaluate_suite(context, source, name, values, limits):
    """Validate one summary row. limits maps a column to (maximum, strict_max)."""
    suite = gx.ExpectationSuite(name=name)
    for column, (maximum, strict) in limits.items():
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column=column,
                min_value=0,
                max_value=maximum,
                strict_max=strict,
            )
        )
    suite = context.suites.add(suite)
    asset = source.add_dataframe_asset(name=name)
    batch = asset.add_batch_definition_whole_dataframe(name="summary")
    validation = context.validation_definitions.add(
        gx.ValidationDefinition(
            name=name,
            data=batch,
            suite=suite,
        )
    )
    result = validation.run(batch_parameters={"dataframe": pd.DataFrame([values])})
    outcomes = {}
    for outcome in result.results:
        column = outcome.expectation_config.kwargs["column"]
        error = outcome.exception_info or {}
        if error.get("raised_exception") or outcome.success is None:
            raise RuntimeError(f"GX execution error for {name}.{column}: {error}")
        outcomes[column] = bool(outcome.success)
    if set(outcomes) != set(values):
        raise RuntimeError(f"GX did not return every expectation in {name}")
    return outcomes


def validate_checks(context, source, layer, checks):
    for table in sorted({c["table_name"] for c in checks}):
        table_checks = [c for c in checks if c["table_name"] == table]
        values = {c["check_name"]: c["gx_value"] for c in table_checks}
        # 100% means advisory even if a reconciliation ratio exceeds 100%.
        # All other thresholds compare the actual, unrounded percentage.
        limits = {
            c["check_name"]: (None if c["threshold_pct"] == 100 else c["threshold_pct"], False) for c in table_checks
        }
        outcomes = evaluate_suite(context, source, f"{layer}_{table}", values, limits)
        for check in table_checks:
            check["status"] = (
                "FAIL" if not outcomes[check["check_name"]] else "WARN" if check["failed_rows"] else "PASS"
            )


# 3. Preserve the layer-level policy without rereading any business tables.
def validate_gate(context, source, layer, checks):
    values = {"blocking_failures": sum(c["blocking"] and c["status"] == "FAIL" for c in checks)}
    limits = {"blocking_failures": (0, False)}
    if layer in ("bronze", "silver"):
        values["failed_checks"] = sum(c["status"] == "FAIL" for c in checks)
        limits["failed_checks"] = (5, True)  # The old gate stops at five failures.
    if layer == "silver":
        by_name = {(c["table_name"], c["check_name"]): c for c in checks}
        quarantine = by_name[("green_taxi_clean", "quarantine_rate_within_limit")]
        files = by_name[("green_taxi_clean", "every_bronze_file_present")]
        values.update(quarantine_pct=quarantine["gx_value"], unprocessed_files=files["failed_rows"])
        # Unlike the individual 5% rule, the release gate rejects EXACTLY 5%.
        limits.update(quarantine_pct=(5, True), unprocessed_files=(0, False))
    outcomes = evaluate_suite(context, source, f"{layer}_release_gate", values, limits)
    return [name for name, success in outcomes.items() if not success]


# 4. Append the original metric counts, not GX's one-row summary counts.
def save_results(spark, catalog, layer, run_id, run_ts, checks, gate_failures):
    quality = f"{quote_identifier(catalog)}.nyc_quality"
    result_schema = """run_id string, run_ts timestamp, layer string, table_name string,
        check_category string, check_name string, failed_rows long, total_rows long,
        failed_pct double, threshold_pct double, status string"""
    rows = [
        (
            run_id,
            run_ts,
            layer,
            c["table_name"],
            c["check_category"],
            c["check_name"],
            c["failed_rows"],
            c["total_rows"],
            c["failed_pct"],
            c["threshold_pct"],
            c["status"],
        )
        for c in checks
    ]
    spark.createDataFrame(rows, result_schema).write.mode("append").saveAsTable(f"{quality}.dq_results")

    overall = "FAIL" if gate_failures else "WARN" if any(c["status"] != "PASS" for c in checks) else "PASS"
    log_schema = """run_id string, run_ts timestamp, layer string, tables_checked int,
        checks_run int, checks_passed int, checks_warned int, checks_failed int,
        overall_status string, finished_at timestamp"""
    log = (
        run_id,
        run_ts,
        layer,
        len({c["table_name"] for c in checks}),
        len(checks),
        sum(c["status"] == "PASS" for c in checks),
        sum(c["status"] == "WARN" for c in checks),
        sum(c["status"] == "FAIL" for c in checks),
        overall,
        datetime.now(timezone.utc),
    )
    spark.createDataFrame([log], log_schema).write.mode("append").saveAsTable(f"{quality}.dq_run_log")
    return overall


def run(spark, layer, catalog="nyc_mobility", source_path=DEFAULT_SOURCE, run_id=None, enforce=None):
    """Validate one completed layer. Business data is read-only throughout."""
    run_id = run_id or str(uuid.uuid4())
    run_ts = datetime.now(timezone.utc)
    enforce = DEFAULT_ENFORCEMENT[layer] if enforce is None else enforce
    spark.conf.set("spark.sql.session.timeZone", "America/New_York")
    spark.sql(f"USE CATALOG {quote_identifier(catalog)}")
    quality = f"{quote_identifier(catalog)}.nyc_quality"
    for name in ("dq_results", "dq_run_log"):
        if not spark.catalog.tableExists(f"{quality}.{name}"):
            raise ValueError("Run src/shared/04_monitoring_tables.sql before GX validation")

    LOGGER.info("Starting %s run %s; enforcement=%s", layer, run_id, enforce)
    rules = []
    if spark.catalog.tableExists(f"{quality}.dq_rules"):
        rules = [
            row.asDict()
            for row in spark.sql(
                f"SELECT table_name, check_name, threshold_pct, blocking "
                f"FROM {quality}.dq_rules WHERE layer = :layer",
                args={"layer": layer},
            ).collect()
        ]
    checks = configure_checks(collect_metrics(spark, layer, source_path), layer, rules)
    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_pandas(name="quality_metrics")
    validate_checks(context, source, layer, checks)
    gate_failures = validate_gate(context, source, layer, checks)
    overall = save_results(spark, catalog, layer, run_id, run_ts, checks, gate_failures)
    LOGGER.info("%s: %s checks; quality=%s; gate failures=%s", layer, len(checks), overall, gate_failures)
    for check in checks:
        if check["status"] != "PASS":
            LOGGER.warning(
                "%s.%s: %s (%s / %s)",
                check["table_name"],
                check["check_name"],
                check["status"],
                check["failed_rows"],
                check["total_rows"],
            )
    # Save both reports first, so failed pipeline tasks still leave evidence.
    if gate_failures and enforce:
        raise RuntimeError(f"{layer} GX gate failed for run {run_id}: {', '.join(gate_failures)}")
    if gate_failures:
        LOGGER.warning("REPORT-ONLY: gate failed, but enforcement is disabled")
    return overall


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--layer", required=True, choices=("bronze", "silver", "gold"))
    parser.add_argument("--catalog", default="nyc_mobility")
    parser.add_argument("--source-path", default=DEFAULT_SOURCE)
    parser.add_argument("--run-id", help="Use the same job run ID for all three layers")
    parser.add_argument("--enforce", choices=("true", "false"), default=None)
    args = parser.parse_args()
    # Databricks provides Spark. Keeping this import here also allows local GX tests.
    from pyspark.sql import SparkSession

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run(
        SparkSession.builder.getOrCreate(),
        args.layer,
        args.catalog,
        args.source_path,
        args.run_id,
        None if args.enforce is None else args.enforce == "true",
    )


# 5. Embedded rule calculations: all 217 checks live in this file.
# Spark computes these counts; GX above decides whether they pass.
METRIC_QUERIES = {
    "bronze": [
        (
            "01_green_taxi",
            r"""
-- Bronze: green taxi. Metrics for GX; no status or writes here.
-- Migrated from tests/02_bronze_all_qc.sql; predicates and denominators retained.
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
    'green_taxi' AS table_name, check_category, check_name,
    failed_rows, total_rows, threshold_pct
FROM checks;
""",
        ),
        (
            "02_taxi_zones",
            r"""
-- Bronze: taxi zones. Metrics for GX; no status or writes here.
-- Migrated from tests/02_bronze_all_qc.sql; predicates and denominators retained.
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
    'taxi_zones' AS table_name, check_category, check_name,
    failed_rows, total_rows, threshold_pct
FROM checks;
""",
        ),
        (
            "03_weather",
            r"""
-- Bronze: weather. Metrics for GX; no status or writes here.
-- Migrated from tests/02_bronze_all_qc.sql; predicates and denominators retained.
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
    'weather' AS table_name, check_category, check_name,
    failed_rows, total_rows, threshold_pct
FROM checks;
""",
        ),
        (
            "04_zone_references",
            r"""
-- Bronze: zone references. Metrics for GX; no status or writes here.
-- Migrated from tests/02_bronze_all_qc.sql; predicates and denominators retained.
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
    'green_taxi' AS table_name, check_category, check_name,
    failed_rows, total_rows, threshold_pct
FROM checks;
""",
        ),
        (
            "05_load_fidelity",
            r"""
-- Bronze: load fidelity. Metrics for GX; no status or writes here.
-- Migrated from tests/02_bronze_all_qc.sql; predicates and denominators retained.
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
    FROM read_files(:source_path,
                    format => 'parquet')
),
files AS (
    -- Name-level reconciliation, not a count.
    SELECT
        (SELECT COUNT(*) FROM (
            SELECT DISTINCT _metadata.file_name AS f
            FROM   read_files(:source_path,
                              format => 'parquet')
            EXCEPT
            SELECT DISTINCT element_at(split(source_file, '/'), -1)
            FROM   nyc_bronze.green_taxi
        ))                                                             AS n_unloaded,
        (SELECT COUNT(DISTINCT _metadata.file_name)
         FROM read_files(:source_path,
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
    'green_taxi' AS table_name, check_category, check_name,
    failed_rows, total_rows, threshold_pct
FROM fidelity;
""",
        ),
    ],
    "silver": [
        (
            "01_green_taxi",
            r"""
-- Silver: green taxi. Metrics for GX; no status or writes here.
-- Migrated from tests/03_silver_all_qc.sql; predicates and denominators retained.
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
    'green_taxi_clean' AS table_name, check_category, check_name,
    failed_rows, total_rows, threshold_pct
FROM checks;
""",
        ),
        (
            "02_taxi_zones",
            r"""
-- Silver: taxi zones. Metrics for GX; no status or writes here.
-- Migrated from tests/03_silver_all_qc.sql; predicates and denominators retained.
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
    'taxi_zones_clean' AS table_name, check_category, check_name,
    failed_rows, total_rows, threshold_pct
FROM checks;
""",
        ),
        (
            "03_weather",
            r"""
-- Silver: weather. Metrics for GX; no status or writes here.
-- Migrated from tests/03_silver_all_qc.sql; predicates and denominators retained.
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
    'weather_clean' AS table_name, check_category, check_name,
    failed_rows, total_rows, threshold_pct
FROM checks;
""",
        ),
        (
            "04_join_coverage",
            r"""
-- Silver: join coverage. Metrics for GX; no status or writes here.
-- Migrated from tests/03_silver_all_qc.sql; predicates and denominators retained.
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
    'green_taxi_clean' AS table_name, check_category, check_name,
    failed_rows, total_rows, threshold_pct
FROM checks;
""",
        ),
    ],
    "gold": [
        (
            "01_dimensions",
            r"""
-- Gold: dimensions. Metrics for GX; no status or writes here.
-- Migrated from tests/04_gold_all_qc.sql; predicates and denominators retained.
WITH d AS (
    SELECT
        COUNT(*)                                                            AS total_rows,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END                            AS t_empty,
        SUM(CASE WHEN date_key IS NULL THEN 1 ELSE 0 END)                   AS c_key,
        COUNT(date_key) - COUNT(DISTINCT date_key)                          AS u_key,
        SUM(CASE WHEN CAST(date_format(full_date, 'yyyyMMdd') AS INT) <> date_key
                 THEN 1 ELSE 0 END)                                         AS x_key_derived,
        -- dayofweek() is 1 = Sunday .. 7 = Saturday in Spark, NOT ISO.
        SUM(CASE WHEN is_weekend <> (day_of_week IN (1, 7)) THEN 1 ELSE 0 END) AS x_weekend
    FROM nyc_gold.dim_date
),
w AS (
    SELECT
        COUNT(*)                                                            AS total_rows,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END                            AS t_empty,
        SUM(CASE WHEN weather_key IS NULL THEN 1 ELSE 0 END)                AS c_key,
        COUNT(weather_key) - COUNT(DISTINCT weather_key)                    AS u_key,
        SUM(CASE WHEN date_format(weather_timestamp, 'yyyyMMddHH') <> weather_key
                 THEN 1 ELSE 0 END)                                         AS x_key_derived,
        SUM(CASE WHEN weather_condition IS NULL THEN 1 ELSE 0 END)          AS c_condition,
        -- temp_max and temp_min are the day's, temp_avg is this hour's, so the
        -- hour must sit inside its own day's range. A window written over the
        -- wrong partition shows up here and nowhere else.
        SUM(CASE WHEN temp_avg_c IS NOT NULL
                  AND (temp_avg_c > temp_max_c OR temp_avg_c < temp_min_c)
                 THEN 1 ELSE 0 END)                                         AS x_temp_range
    FROM nyc_gold.dim_weather
),
z AS (
    SELECT
        COUNT(*)                                                            AS total_rows,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END                            AS t_empty,
        SUM(CASE WHEN location_id IS NULL THEN 1 ELSE 0 END)                AS c_key,
        COUNT(location_id) - COUNT(DISTINCT location_id)                    AS u_key,
        SUM(CASE WHEN borough IS NULL OR TRIM(borough) = '' THEN 1 ELSE 0 END) AS c_borough,
        CASE WHEN COUNT(*) = 265 THEN 0 ELSE 1 END                          AS b_265
    FROM nyc_gold.dim_taxi_zone
),
checks AS (
    SELECT 'dim_date' AS tbl, 'completeness' AS check_category, 'table_not_empty' AS check_name,
           0.0 AS threshold_pct, t_empty AS failed_rows, 1 AS total_rows FROM d
    UNION ALL SELECT 'dim_date', 'completeness', 'date_key_not_null',  0.0, c_key,         total_rows FROM d
    UNION ALL SELECT 'dim_date', 'uniqueness',   'date_key_unique',    0.0, u_key,         total_rows FROM d
    UNION ALL SELECT 'dim_date', 'consistency',  'key_matches_full_date', 0.0, x_key_derived, total_rows FROM d
    UNION ALL SELECT 'dim_date', 'consistency',  'is_weekend_matches_day_of_week', 0.0, x_weekend, total_rows FROM d

    UNION ALL SELECT 'dim_weather', 'completeness', 'table_not_empty',    0.0, t_empty, 1 FROM w
    UNION ALL SELECT 'dim_weather', 'completeness', 'weather_key_not_null', 0.0, c_key,  total_rows FROM w
    UNION ALL SELECT 'dim_weather', 'uniqueness',   'weather_key_unique', 0.0, u_key,    total_rows FROM w
    UNION ALL SELECT 'dim_weather', 'consistency',  'key_matches_timestamp', 0.0, x_key_derived, total_rows FROM w
    UNION ALL SELECT 'dim_weather', 'completeness', 'weather_condition_not_null', 0.0, c_condition, total_rows FROM w
    UNION ALL SELECT 'dim_weather', 'consistency',  'hour_temp_within_day_range', 0.0, x_temp_range, total_rows FROM w

    UNION ALL SELECT 'dim_taxi_zone', 'completeness', 'table_not_empty',  0.0, t_empty, 1 FROM z
    UNION ALL SELECT 'dim_taxi_zone', 'completeness', 'location_id_not_null', 0.0, c_key, total_rows FROM z
    UNION ALL SELECT 'dim_taxi_zone', 'uniqueness',   'location_id_unique', 0.0, u_key,  total_rows FROM z
    UNION ALL SELECT 'dim_taxi_zone', 'completeness', 'borough_not_blank', 0.0, c_borough, total_rows FROM z
    UNION ALL SELECT 'dim_taxi_zone', 'business',     'lookup_has_265_zones', 0.0, b_265, 1 FROM z
)

SELECT
    tbl AS table_name, check_category, check_name,
    failed_rows, total_rows, threshold_pct
FROM checks;
""",
        ),
        (
            "02_fact_trip",
            r"""
-- Gold: fact trip. Metrics for GX; no status or writes here.
-- Migrated from tests/04_gold_all_qc.sql; predicates and denominators retained.
WITH silver AS (
    -- The same key the fact MERGE builds, recomputed here rather than trusted.
    --
    -- Reads vw_green_taxi_valid, NOT green_taxi_clean. The fact table is built
    -- from the valid view, so the expected count has to come from the same
    -- place. Comparing against the clean table counts the FAIL rows the view
    -- deliberately excludes, and the reconciliation fails by exactly the number
    -- of rows the pipeline is working correctly to leave out -- a check that
    -- breaks the moment the thing it checks is fixed.
    SELECT
        COUNT(DISTINCT md5(concat_ws('|',
            COALESCE(CAST(vendor_id AS STRING), '<NULL>'),
            COALESCE(CAST(lpep_pickup_datetime AS STRING), '<NULL>'),
            COALESCE(CAST(lpep_dropoff_datetime AS STRING), '<NULL>'),
            COALESCE(CAST(pu_location_id AS STRING), '<NULL>'),
            COALESCE(CAST(do_location_id AS STRING), '<NULL>'),
            COALESCE(CAST(trip_distance AS STRING), '<NULL>'),
            COALESCE(CAST(total_amount AS STRING), '<NULL>')
        )))                                                        AS n_expected,
        ROUND(SUM(total_amount), 2)                                AS revenue
    FROM   nyc_silver.vw_green_taxi_valid
    WHERE  lpep_pickup_datetime IS NOT NULL
      AND  lpep_dropoff_datetime IS NOT NULL
),
grain AS (
    SELECT COALESCE(SUM(n - 1), 0) AS extra_rows
    FROM (
        SELECT COUNT(*) AS n
        FROM   nyc_gold.fact_taxi_trip
        GROUP  BY trip_key
        HAVING COUNT(*) > 1
    )
),
f AS (
    SELECT
        COUNT(*)                                                   AS total_rows,
        CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END                   AS t_empty,
        ROUND(SUM(total_amount), 2)                                AS revenue,
        SUM(CASE WHEN trip_key IS NULL THEN 1 ELSE 0 END)          AS c_key,
        SUM(CASE WHEN lpep_dropoff_datetime < lpep_pickup_datetime
                 THEN 1 ELSE 0 END)                                AS x_time_order,
        -- unix_timestamp(), not timestampdiff(): the Gold build computes this
        -- column from epoch seconds, so the check must recompute it the same
        -- way. The two functions disagree by exactly the DST shift on the 11
        -- trips that straddle 02:00 on spring-forward day -- a check written
        -- with timestampdiff tests the two functions against each other and
        -- reports correct rows as defects.
        SUM(CASE WHEN ABS(trip_duration_minutes
                          - (unix_timestamp(lpep_dropoff_datetime)
                             - unix_timestamp(lpep_pickup_datetime)) / 60.0) > 0.02
                 THEN 1 ELSE 0 END)                                AS x_duration,
        -- Rows Silver classified FAIL. Zero once the fact MERGE reads
        -- vw_green_taxi_valid instead of green_taxi_clean.
        SUM(CASE WHEN exists(qc_error_descriptions, x -> startswith(x, 'FAIL:'))
                 THEN 1 ELSE 0 END)                                AS x_quarantined,
        SUM(CASE WHEN size(qc_error_descriptions) > 0 THEN 1 ELSE 0 END) AS b_warned
    FROM nyc_gold.fact_taxi_trip
),
checks AS (
    SELECT 'completeness' AS check_category, 'table_not_empty' AS check_name,
           0.0 AS threshold_pct, t_empty AS failed_rows, 1 AS total_rows FROM f
    UNION ALL SELECT 'completeness', 'trip_key_not_null',    0.0, c_key,        total_rows FROM f
    UNION ALL SELECT 'uniqueness',   'one_row_per_trip_key', 0.0,
                     (SELECT extra_rows FROM grain), total_rows FROM f
    UNION ALL SELECT 'consistency',  'pickup_before_dropoff', 0.0, x_time_order, total_rows FROM f
    UNION ALL SELECT 'consistency',  'duration_matches_timestamps', 0.0, x_duration, total_rows FROM f

    UNION ALL SELECT 'consistency',  'rows_reconcile_with_silver', 0.0,
        CASE WHEN (SELECT n_expected FROM silver) = (SELECT total_rows FROM f)
             THEN 0 ELSE 1 END, 1 FROM f
    UNION ALL SELECT 'consistency',  'revenue_preserved', 0.0,
        CASE WHEN ABS(COALESCE((SELECT revenue FROM silver), 0)
                    - COALESCE((SELECT revenue FROM f), 0))
                  <= GREATEST(1.0, 0.001 * ABS(COALESCE((SELECT revenue FROM silver), 0)))
             THEN 0 ELSE 1 END, 1 FROM f

    -- Non-blocking on purpose: it reports a defect in the Gold build, and
    -- stopping the pipeline does not fix a FROM clause.
    UNION ALL SELECT 'consistency',  'quarantined_rows_in_gold', 0.0, x_quarantined, total_rows FROM f
    -- Advisory: can only ever WARN.
    UNION ALL SELECT 'business',     'trips_carrying_silver_warnings', 100.0, b_warned, total_rows FROM f
)

SELECT
    'fact_taxi_trip' AS table_name, check_category, check_name,
    failed_rows, total_rows, threshold_pct
FROM checks;
""",
        ),
        (
            "03_at_rest_integrity",
            r"""
-- Gold: at rest integrity. Metrics for GX; no status or writes here.
-- Migrated from tests/04_gold_all_qc.sql; predicates and denominators retained.
WITH pu AS (
    SELECT COUNT(*) AS unmatched,
           (SELECT COUNT(DISTINCT pickup_location_id) FROM nyc_gold.fact_taxi_trip) AS total
    FROM (
        SELECT DISTINCT f.pickup_location_id
        FROM       nyc_gold.fact_taxi_trip f
        LEFT  JOIN nyc_gold.dim_taxi_zone  z ON f.pickup_location_id = z.location_id
        WHERE f.pickup_location_id IS NOT NULL AND z.location_id IS NULL
    )
),
do_ AS (
    SELECT COUNT(*) AS unmatched,
           (SELECT COUNT(DISTINCT dropoff_location_id) FROM nyc_gold.fact_taxi_trip) AS total
    FROM (
        SELECT DISTINCT f.dropoff_location_id
        FROM       nyc_gold.fact_taxi_trip f
        LEFT  JOIN nyc_gold.dim_taxi_zone  z ON f.dropoff_location_id = z.location_id
        WHERE f.dropoff_location_id IS NOT NULL AND z.location_id IS NULL
    )
),
-- fact.pickup_date is a DATE; dim_date's key is date_key INT, so the join is
-- on full_date. Worth stating: the column comment calls pickup_date a foreign
-- key to dim_date, and it is -- just not to the column named _key.
--
-- Counted in TRIPS, not in distinct dates, and that is the whole reason these
-- two are not blocking. dim_date is derived from the dates present in the
-- weather feed, by design. TLC files reliably carry a handful of trips dated
-- years outside the file month -- Silver flags them WARN and keeps them -- so
-- those dates have no calendar row and never will.
--
-- Over ~100 distinct dates, that handful is about 8 percent: a permanent FAIL
-- at any tolerance, purely because the denominator is small. The same defect
-- over 133,367 trips is 0.008 percent. A month genuinely missing from the
-- calendar is about a third of the rows either way, so the row denominator
-- still catches the regression while tolerating the convention.
pd AS (
    SELECT
        SUM(CASE WHEN f.pickup_date IS NOT NULL AND d.full_date IS NULL
                 THEN 1 ELSE 0 END)                             AS unmatched,
        COUNT(*)                                                AS total
    FROM       nyc_gold.fact_taxi_trip f
    LEFT  JOIN nyc_gold.dim_date       d ON f.pickup_date = d.full_date
),
dd AS (
    SELECT
        SUM(CASE WHEN f.dropoff_date IS NOT NULL AND d.full_date IS NULL
                 THEN 1 ELSE 0 END)                             AS unmatched,
        COUNT(*)                                                AS total
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
        WHERE f.weather_key IS NOT NULL AND w.weather_key IS NULL
    )
),
rows_hit AS (
    SELECT
        COUNT(*)                                                     AS n_trips,
        SUM(CASE WHEN zp.location_id IS NULL THEN 1 ELSE 0 END)      AS t_pu,
        SUM(CASE WHEN f.weather_key IS NULL THEN 1 ELSE 0 END)       AS t_no_weather
    FROM       nyc_gold.fact_taxi_trip f
    LEFT  JOIN nyc_gold.dim_taxi_zone  zp ON f.pickup_location_id = zp.location_id
),
checks AS (
    SELECT 'at_rest_integrity' AS check_category, 'pickup_zone_resolves' AS check_name,
           0.0 AS threshold_pct,
           (SELECT unmatched FROM pu) AS failed_rows,
           (SELECT total FROM pu)     AS total_rows
    UNION ALL SELECT 'at_rest_integrity', 'dropoff_zone_resolves', 0.0,
           (SELECT unmatched FROM do_), (SELECT total FROM do_)
    -- 5.0, not 0.0, and not blocking: see the note on the pd CTE above.
    UNION ALL SELECT 'at_rest_integrity', 'pickup_date_resolves', 5.0,
           (SELECT unmatched FROM pd), (SELECT total FROM pd)
    UNION ALL SELECT 'at_rest_integrity', 'dropoff_date_resolves', 5.0,
           (SELECT unmatched FROM dd), (SELECT total FROM dd)
    -- Only keys that were actually set are checked; a NULL weather_key is "no weather
    -- matched", which is the advisory below, not a broken reference.
    UNION ALL SELECT 'at_rest_integrity', 'weather_key_resolves', 0.0,
           (SELECT unmatched FROM wk), (SELECT total FROM wk)

    -- The same defects counted in trips. Advisory: one defect should not stop
    -- the pipeline twice.
    UNION ALL SELECT 'at_rest_integrity', 'trips_with_unmatched_pickup_zone', 100.0,
           (SELECT t_pu FROM rows_hit), (SELECT n_trips FROM rows_hit)
    UNION ALL SELECT 'at_rest_integrity', 'trips_without_weather', 100.0,
           (SELECT t_no_weather FROM rows_hit), (SELECT n_trips FROM rows_hit)

    -- Every dimension row is reachable. Not an error -- an unused zone is
    -- normal -- but a dimension where MOST rows are unused usually means the
    -- key convention drifted between the dimension and the fact.
    UNION ALL SELECT 'at_rest_integrity', 'weather_hours_used_by_a_trip', 100.0,
        (SELECT COUNT(*) FROM (
            SELECT w.weather_key FROM nyc_gold.dim_weather w
            LEFT JOIN (SELECT DISTINCT weather_key FROM nyc_gold.fact_taxi_trip) f
                   ON w.weather_key = f.weather_key
            WHERE f.weather_key IS NULL)),
        (SELECT COUNT(*) FROM nyc_gold.dim_weather)
)

SELECT
    'fact_taxi_trip' AS table_name, check_category, check_name,
    failed_rows, total_rows, threshold_pct
FROM checks;
""",
        ),
    ],
}

if __name__ == "__main__":
    main()

