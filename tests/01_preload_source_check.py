## Preload checks — NYC Mobility
#Runs **after the files land and before the Bronze MERGE**. Its job is to
#answer one question: *is this file fit to load?* If the answer is no, it
#raises, the Bronze task never runs, and Bronze stays clean.
#Three things can only be done here:

#1. **Schema drift.** Nothing in the pipeline currently checks that the
#  expected columns are present. If TLC renames a column, the MERGE's
#   declared schema produces a column of NULLs and the first hint is a
#   `no_nulls_added_*` failure after the damage is done.
#2. **Cast loss, named.** `no_nulls_added_*` in Bronze reports a *count* of
#   values lost to a CAST. Here the offending values are still available, so
#   the check can show them.
#3. **Refusing the file.** `row_count_matches_source` is not recoverable by
#   re-running — the Bronze MERGE matches on `source_file`, so once a partial
#   load exists, re-running inserts nothing. Rejecting before the write
#   avoids that trap entirely.

## Silent truncation: the one Bronze cannot see at all
#`CAST(2.7 AS INT)` is `2`. No error, no NULL — so `no_nulls_added_*` reports
#a clean pass while the value quietly changes. The source columns
#`passenger_count`, `RatecodeID`, `payment_type` and `trip_type` are numeric
#in the Parquet and INT in Bronze, so this is a live risk, and the
#`no_fractional_loss_*` checks below are the only thing in the pipeline
#looking for it.

#Results are written to `nyc_quality.dq_results` with `layer = 'preload'`.
#That is what the `layer` column is for, and it means the per-month
#dashboard and `vw_dq_by_month` pick these up with no change — a check that
#moves from Bronze to preload keeps its history instead of restarting it.

from pyspark.sql import functions as F
from dataclasses import dataclass, field
from datetime import datetime
import uuid, re

VOLUME      = "/Volumes/workspace/default/ftw-b12-de/groups/week-08/group-d"
TAXI_DIR    = f"{VOLUME}/green-taxi"
WEATHER_DIR = f"{VOLUME}/weather"
ZONES_DIR   = f"{VOLUME}/taxi-zone-lookup"

CATALOG       = "nyc_mobility"
RESULTS_TABLE = f"{CATALOG}.nyc_quality.dq_results"
RUNLOG_TABLE  = f"{CATALOG}.nyc_quality.dq_run_log"
LAYER         = "preload"

spark.sql("SET TIME ZONE 'America/New_York'")

# ------------------------------------------------------------ batch detection
#
# ## One optional widget, and a default that is usually right
#
# `batch_month` (YYYY-MM) overrides everything. Leave it blank and the month
# is worked out from the two places that already know.
#
# ## Why not "the newest file in the volume"
#
# Ingestion is a separate pipeline. Files can sit in the volume for days
# before anything processes them, so the newest file is the newest DOWNLOAD,
# not the next thing to load. With March, April and May all sitting there,
# that rule picks May every run and March never gets processed.
#
# ## Why not "the newest month in Bronze" either
#
# Bronze records what has been DONE. Detecting from it would re-check a month
# that is already loaded, and the pipeline would never advance.
#
# ## The difference is the answer
#
#     landed (volume)  -  loaded (Bronze)  =  outstanding
#
# Take the OLDEST of what is outstanding and the pipeline walks March, April,
# May in order, one per run, however far ahead the downloads have got.

MONTH_NAMES = ["january", "february", "march", "april", "may", "june",
               "july", "august", "september", "october", "november", "december"]

# Named `year_month` to match the ingestion notebook and the Bronze/Silver
# checks, so one job parameter drives every task instead of each one having
# its own name for the same thing.
dbutils.widgets.text("year_month", "", "year_month (YYYY-MM, blank = next unprocessed)")
PARAM_MONTH = dbutils.widgets.get("year_month").strip()

def list_files(path):
    """(name, modificationTime) for each file. Empty if the folder is missing.

    The modification time is what lets a re-downloaded file be spotted below;
    everything else only needs the names, which list_names returns."""
    try:
        return sorted((f.name, f.modificationTime)
                      for f in dbutils.fs.ls(path) if not f.name.endswith("/"))
    except Exception:
        return []

def list_names(path):
    """File names in a volume folder. Empty list if the folder is missing."""
    return [name for name, _ in list_files(path)]

TAXI_FILES = list_files(TAXI_DIR)

def loaded_months():
    """{month: latest ingestion_time} for what Bronze already holds. Empty on
    the first run, before the Bronze DDL has been executed -- which is not an
    error, it is the state that means 'nothing has been loaded yet'."""
    try:
        rows = spark.sql(f"""
            SELECT regexp_extract(source_file, '([0-9]{{4}}-[0-9]{{2}})', 1) AS m,
                   MAX(ingestion_time)                                       AS loaded_at
            FROM   {CATALOG}.nyc_bronze.green_taxi
            GROUP  BY 1
        """).collect()
        return {r["m"]: r["loaded_at"] for r in rows if r["m"]}
    except Exception:
        return {}

landed = {}
for name, mtime in TAXI_FILES:
    for m in re.findall(r"(\d{4}-\d{2})", name):
        landed[m] = mtime
if not landed:
    raise FileNotFoundError(
        f"No green_tripdata_YYYY-MM.parquet files in {TAXI_DIR} — nothing to check.")

already = loaded_months()
pending = sorted(m for m in landed if m not in already)

# A file replaced after its month was loaded. NOT treated as pending: the
# Bronze MERGE matches on source_file, so it would insert nothing and the
# re-run would be a silent no-op. Reported instead, because a corrected file
# sitting unprocessed is worth knowing about and nothing else would say so.
restated = sorted(m for m, t in already.items()
                  if t is not None and m in landed
                  and landed[m] > int(t.timestamp() * 1000))

if PARAM_MONTH:
    if not re.fullmatch(r"\d{4}-\d{2}", PARAM_MONTH) or not 1 <= int(PARAM_MONTH[5:]) <= 12:
        raise ValueError(f"year_month must be YYYY-MM (e.g. 2026-03), got: {PARAM_MONTH!r}")
    BATCH_MONTH, MONTH_SOURCE = PARAM_MONTH, "widget"
elif pending:
    BATCH_MONTH, MONTH_SOURCE = pending[0], f"next unprocessed of {len(pending)}"
else:
    # Everything landed is loaded. Re-check the newest month rather than
    # exiting quietly: the checks replace their own rows, so a re-run is the
    # idempotency test, and a green run that checked nothing is too easy to
    # mistake for a green run that checked something.
    BATCH_MONTH, MONTH_SOURCE = max(landed), "re-check (nothing outstanding)"

YEAR, MONTH_NUM = BATCH_MONTH.split("-")
TAXI_FILE    = f"green_tripdata_{BATCH_MONTH}.parquet"
WEATHER_FILE = f"weather_{MONTH_NAMES[int(MONTH_NUM) - 1]}_{YEAR}.csv"

RUN_ID = str(uuid.uuid4())
RUN_TS = datetime.now()

if restated:
    print(f"!! re-downloaded since loading : {restated}")
    print(f"   the MERGE matches on source_file, so these will NOT reload.")
    print(f"   DELETE the month from Bronze first if you want the new file in.\n")

# The two folders are independent. Showing both makes it obvious when weather
# is behind taxi, which is allowed but worth seeing before the run explains it
# as a FAIL.
weather_months = set()
for name in list_names(WEATHER_DIR):
    m = re.search(r"weather_([a-z]+)_(\d{4})", name.lower())
    if m and m.group(1) in MONTH_NAMES:
        weather_months.add(f"{m.group(2)}-{MONTH_NAMES.index(m.group(1)) + 1:02d}")

print(f"taxi files   : {sorted(landed)}")
print(f"weather files: {sorted(weather_months) or '(none)'}")
if BATCH_MONTH not in weather_months:
    print(f"   note: no weather file for {BATCH_MONTH} — "
          f"weather will be held back, taxi proceeds")
print(f"loaded       : {sorted(already) or '(bronze empty)'}")
print(f"outstanding  : {pending or '(none)'}")
print(f"batch_month  : {BATCH_MONTH}  ({MONTH_SOURCE})")
print(f"taxi file    : {TAXI_FILE}")
print(f"weather file : {WEATHER_FILE}")
print(f"run_id       : {RUN_ID}")
# STATIC_BATCH is derived in the next cell, once list_files is available.


## Threshold policy
#| Constant | Value | Meaning |
#|---|---|---|
#| `STRICT` | 0% | one occurrence corrupts a join or the grain, or the check is scalar |
#| `TOL` | 10% | the source is known to be imperfect and this much is tolerated |
#| `ADVISORY` | 100% | reported every run, can only ever WARN |
#| `CAST_WARN` / `CAST_FAIL` | 5% / 10% | the pair used for cast and parse checks |
#| `MIN_ROWS` | 5 | under this many failing rows it is a WARN whatever the rate |

#`blocking=True` means a FAIL raises and the Bronze MERGE never runs.
STRICT, TOL, ADVISORY = 0.0, 10.0, 100.0
CAST_WARN, CAST_FAIL  = 5.0, 10.0
MIN_ROWS              = 5

@dataclass
class Check:
    name:     str
    category: str                  # completeness | uniqueness | validity | consistency | business | schema
    fail:     str                  # SQL boolean, TRUE for a row that BREAKS the rule
    threshold: float = TOL         # the FAIL line
    warn:      float = 0.0         # the WARN line; 0 means any failure is at least a WARN
    min_rows:  int   = MIN_ROWS    # absolute floor under the percentage
    denom:     str   = "1=1"       # which rows count toward total_rows
    blocking:  bool  = False

def status_of(failed, total, threshold, warn, min_rows, is_empty_check=False):
    """The Bronze status ladder, in Python. Kept in this shape on purpose --
    if the two ever disagree, the difference should be visible side by side."""
    if total == 0 and not is_empty_check:
        return "SKIP"
    if failed == 0:
        return "PASS"
    pct = 100.0 * failed / total if total else 0.0
    if pct <= warn:
        return "PASS"          # within tolerance, still recorded
    if failed <= min_rows:
        return "WARN"
    if pct <= threshold:
        return "WARN"
    return "FAIL"

# ## Which sources the run cannot proceed without
#
# Mirrors `v_required_tables` in the Bronze notebook. green_taxi is the fact
# table: no trips, no pipeline. Weather and zones enrich it, so a missing or
# broken file for either holds that source back and leaves the trip path
# alone.
#
# This is why the folders do NOT have to contain the same months. Taxi can be
# five months ahead of weather; the months with both get both, the months with
# only taxi get taxi and a recorded FAIL saying weather was not there.
REQUIRED_TABLES = {"green_taxi"}

# ## Tables with no month in them
#
# taxi_zones is a 265-row reference file that does not change from month to
# month. Stamping its results with the trip month would claim it was
# re-validated each time, produce an identical row set every run, and give the
# month-over-month trend a flat line that cannot mean anything.
#
# So it gets a batch of its own -- but versioned, not flattened. Stamping a
# literal "static" would mean a re-check REPLACES the previous row set, and the
# day TLC ships an updated lookup the record of the old one is gone: you would
# know the current file is fine and have no way to see it used to have 265
# zones and now has 267.
#
# Keying on the file's modification date gives both. Unchanged file, same key,
# re-check replaces its own rows -- no accumulation. Replaced file, new key,
# new row set, and the old one stays.
#
# The `static-` prefix keeps it obviously not a month, so it sorts apart in
# vw_dq_by_month and cannot be read as one.
#
# Caveat worth knowing: a re-upload of an IDENTICAL file changes the
# modification time and so creates a new version row. Hashing the rows would
# avoid that; recording the re-upload is arguably the more honest of the two.
#
# The referential-integrity checks stay monthly, because the TRIPS vary --
# but those are recorded against green_taxi, not here.
STATIC_TABLES = {"taxi_zones"}

_zone_files = list_files(ZONES_DIR)
if _zone_files:
    _zone_mtime  = max(mt for _, mt in _zone_files)
    STATIC_BATCH = "static-" + datetime.fromtimestamp(
        _zone_mtime / 1000).strftime("%Y-%m-%d")
else:
    # No lookup file at all. The checks will report that; the key just needs
    # to be something that does not collide with a real version.
    STATIC_BATCH = "static-missing"

def batch_of(table):
    return STATIC_BATCH if table in STATIC_TABLES else BATCH_MONTH

RESULTS = []   # accumulates dicts; written once at the end

def record(table, category, name, failed, total, threshold=STRICT, warn=0.0,
           min_rows=0, blocking=False, is_empty_check=False):
    """A single check result. Used directly for scalar and file-level checks."""
    failed, total = int(failed), int(total)
    st = status_of(failed, total, threshold, warn, min_rows, is_empty_check)
    RESULTS.append(dict(
        run_id=RUN_ID, run_ts=RUN_TS, layer=LAYER, table_name=table,
        check_category=category, check_name=name,
        failed_rows=failed, total_rows=total,
        failed_pct=round(100.0 * failed / total, 4) if total else None,
        threshold_pct=threshold, status=st,
        batch_month=batch_of(table), min_failed_rows=min_rows, warn_pct=warn))
    if blocking and st == "FAIL":
        BLOCKING_FAILURES.append(f"{table}.{name}")
    return st

BLOCKING_FAILURES = []

def run_checks(df, table, checks):
    """Every row-level check for one source in ONE pass over the file.

    Each check contributes two aggregates -- how many rows broke it, and how
    many rows it looked at. The second is not always COUNT(*): the vendor
    accounting rules only apply to that vendor's rows, and measuring them
    against the whole file would dilute a real problem into invisibility."""
    if not checks:
        return
    aggs = []
    for i, c in enumerate(checks):
        aggs.append(F.sum(F.expr(
            f"CASE WHEN ({c.denom}) AND ({c.fail}) THEN 1 ELSE 0 END")).alias(f"f{i}"))
        aggs.append(F.sum(F.expr(
            f"CASE WHEN ({c.denom}) THEN 1 ELSE 0 END")).alias(f"t{i}"))
    row = df.agg(*aggs).collect()[0]
    for i, c in enumerate(checks):
        record(table, c.category, c.name,
               row[f"f{i}"] or 0, row[f"t{i}"] or 0,
               c.threshold, c.warn, c.min_rows, c.blocking)

# Columns that genuinely exist in the source and are deliberately not loaded.
# Listing them here keeps `no_unexpected_columns` meaningful: it should fire on
# a column nobody has seen before, not on one the loader has always ignored.
#
#   ehail_fee -- a legacy e-hail field in the TLC schema, effectively always
#                NULL. It is not in the Bronze DDL and the MERGE does not
#                reference it.
IGNORED_COLUMNS = {"green_taxi": {"ehail_fee"},
                   "weather":    set(),
                   "taxi_zones": set()}

def skip_all(table, checks):
    """Record every check for a source that could not be read, as SKIP.

    Without this the source simply has fewer rows in dq_results than the
    others, and "absent" reads the same as "never defined". A SKIP row says
    plainly that the check exists and did not run."""
    for c in checks:
        record(table, c.category, c.name, 0, 0, c.threshold, c.warn, c.min_rows)

def check_schema(df, table, expected, blocking=True):
    """Columns the loader names must exist. This is the check the pipeline did
    not have: a renamed source column produces a column of NULLs in Bronze and
    nothing says why."""
    actual  = set(df.columns)
    ignored = IGNORED_COLUMNS.get(table, set())
    missing = [c for c in expected if c not in actual]
    extra   = [c for c in actual if c not in expected and c not in ignored]
    record(table, "schema", "expected_columns_present",
           len(missing), len(expected), STRICT, 0.0, 0, blocking)
    # Extra columns are news, not a defect -- the loader names its columns, so
    # a new one is ignored rather than mis-loaded. Columns in IGNORED_COLUMNS
    # are known and deliberately not loaded, so they do not count as news.
    record(table, "schema", "no_unexpected_columns",
           len(extra), max(len(actual), 1), ADVISORY, 0.0, 0, False)
    if missing:
        print(f"  !! {table} missing columns: {missing}")
    if extra:
        print(f"  -- {table} extra columns: {extra}")

def dup_count(df, keys):
    """Duplicate rows on a key, counted as 'rows beyond the first'."""
    n  = df.count()
    nd = df.select(*keys).distinct().count()
    return n - nd, n


## green_taxi — the landed Parquet
#Checks run against the **source column names** (`VendorID`, `PULocationID`,
#`RatecodeID`), not the Bronze ones. That is the point: at this stage the
#rename has not happened yet, and a check written against the Bronze name
#would silently match nothing.

## The six cast columns
#These are the only columns the Bronze MERGE converts. Each gets two checks:
#| Check | Catches |
#|---|---|
#| `cast_ok_*` | the value will not convert and becomes NULL |
#| `no_fractional_loss_*` | the value converts but changes — `2.7` to `2` |
#The second has no counterpart in Bronze and cannot have one. A truncated
#value is not NULL, so `no_nulls_added_*` passes while the number is wrong.

TAXI_COLUMNS = [
    "VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime", "store_and_fwd_flag",
    "RatecodeID", "PULocationID", "DOLocationID", "passenger_count", "trip_distance",
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "payment_type", "trip_type",
    "congestion_surcharge", "cbd_congestion_fee",
]

# source column -> Bronze target type
CAST_COLUMNS = {
    "lpep_pickup_datetime":  "TIMESTAMP",
    "lpep_dropoff_datetime": "TIMESTAMP",
    "RatecodeID":            "INT",
    "passenger_count":       "INT",
    "payment_type":          "INT",
    "trip_type":             "INT",
}

DURATION = ("timestampdiff(SECOND, CAST(lpep_pickup_datetime AS TIMESTAMP), "
            "CAST(lpep_dropoff_datetime AS TIMESTAMP))")

taxi_checks = [
    # -- completeness ------------------------------------------------------
    Check("pickup_datetime_not_null",   "completeness", "lpep_pickup_datetime IS NULL",  STRICT),
    Check("dropoff_datetime_not_null",  "completeness", "lpep_dropoff_datetime IS NULL", STRICT),
    Check("pickup_zone_not_null",       "completeness", "PULocationID IS NULL",          STRICT),
    Check("dropoff_zone_not_null",      "completeness", "DOLocationID IS NULL",          STRICT),
    Check("vendor_id_not_null",         "completeness", "VendorID IS NULL",              TOL),
    Check("trip_distance_not_null",     "completeness", "trip_distance IS NULL",         TOL),
    Check("fare_amount_not_null",       "completeness", "fare_amount IS NULL",           TOL),
    Check("total_amount_not_null",      "completeness", "total_amount IS NULL",          TOL),
    # Vendor 6 (Myle) never populates the dispatch fields, so counting its rows
    # here would report a structural absence as missing data every run.
    Check("passenger_count_not_null_excl_myle", "completeness",
          "passenger_count IS NULL", TOL, denom="VendorID <> 6 OR VendorID IS NULL"),

    # -- validity ----------------------------------------------------------
    Check("pickup_month_matches_source_file", "validity",
          f"date_format(CAST(lpep_pickup_datetime AS TIMESTAMP), 'yyyy-MM') <> '{BATCH_MONTH}'", TOL),
    Check("trip_distance_not_negative",  "validity", "trip_distance   < 0", STRICT),
    Check("passenger_count_not_negative","validity", "passenger_count < 0", STRICT),
    Check("fare_amount_not_negative",    "validity", "fare_amount     < 0", TOL),
    Check("total_amount_not_negative",   "validity", "total_amount    < 0", TOL),
    Check("passenger_count_plausible",   "validity", "passenger_count > 9", TOL),
    Check("passenger_count_not_zero",    "validity", "passenger_count = 0", TOL),
    # p99.9 is about 31 miles; one 111,005-mile trip was observed. 200 flags
    # the clear outliers without touching a normal trip.
    Check("trip_distance_plausible",     "validity", "trip_distance > 200", TOL),
    Check("pickup_zone_in_range",        "validity", "PULocationID NOT BETWEEN 1 AND 265", TOL),
    Check("dropoff_zone_in_range",       "validity", "DOLocationID NOT BETWEEN 1 AND 265", TOL),
    Check("vendor_id_in_domain",         "validity", "VendorID   NOT IN (1,2,6)", TOL),
    Check("ratecode_in_domain",          "validity", "RatecodeID NOT IN (1,2,3,4,5,6,99)", TOL),
    Check("payment_type_in_domain",      "validity", "payment_type NOT IN (0,1,2,3,4,5,6)", TOL),
    Check("trip_type_in_domain",         "validity", "trip_type  NOT IN (1,2)", TOL),
    Check("store_and_fwd_flag_in_domain","validity", "store_and_fwd_flag NOT IN ('Y','N')", TOL),

    # -- consistency -------------------------------------------------------
    Check("dropoff_after_pickup", "consistency",
          "CAST(lpep_dropoff_datetime AS TIMESTAMP) < CAST(lpep_pickup_datetime AS TIMESTAMP)", TOL),
    Check("duration_not_zero", "consistency",
          "CAST(lpep_dropoff_datetime AS TIMESTAMP) = CAST(lpep_pickup_datetime AS TIMESTAMP)", TOL),
    Check("duration_under_24_hours", "consistency", f"{DURATION} > 86400", TOL),
    Check("myle_dispatch_fields_stay_null", "consistency",
          "VendorID = 6 AND passenger_count IS NOT NULL", STRICT),
    # The six dispatch fields are null as a SET, never individually.
    Check("dispatch_fields_null_as_a_set", "consistency",
          """(CASE WHEN passenger_count      IS NULL THEN 1 ELSE 0 END
            + CASE WHEN RatecodeID           IS NULL THEN 1 ELSE 0 END
            + CASE WHEN payment_type         IS NULL THEN 1 ELSE 0 END
            + CASE WHEN trip_type            IS NULL THEN 1 ELSE 0 END
            + CASE WHEN store_and_fwd_flag   IS NULL THEN 1 ELSE 0 END
            + CASE WHEN congestion_surcharge IS NULL THEN 1 ELSE 0 END) NOT IN (0, 6)""", STRICT),

    # -- business ----------------------------------------------------------
    # One accounting identity per vendor, each against that vendor's own rows.
    # Vendor 2 (Curb) puts everything in total_amount; vendor 1 (Creative
    # Mobile) itemises the three surcharges without adding them in.
    Check("total_equals_sum_of_charges_v2", "business",
          """ABS(total_amount - (COALESCE(fare_amount,0) + COALESCE(extra,0)
             + COALESCE(mta_tax,0) + COALESCE(tip_amount,0) + COALESCE(tolls_amount,0)
             + COALESCE(improvement_surcharge,0) + COALESCE(congestion_surcharge,0)
             + COALESCE(cbd_congestion_fee,0))) > 0.01""", TOL, denom="VendorID = 2"),
    Check("total_equals_sum_of_charges_v1", "business",
          """ABS(total_amount - (COALESCE(fare_amount,0) + COALESCE(extra,0)
             + COALESCE(mta_tax,0) + COALESCE(tip_amount,0)
             + COALESCE(tolls_amount,0))) > 0.01""", TOL, denom="VendorID = 1"),
    # Myle's fare_amount stays near 2.75 at every distance -- it is a
    # placeholder, not a metered fare. 10 is above the observed max of 9.
    Check("myle_fare_stays_placeholder", "business",
          "fare_amount > 10", STRICT, denom="VendorID = 6"),
    # Round fares on trips that barely happened: 300 appears 22 times at ~3
    # seconds and zero distance. Invisible inside fare_implies_some_distance.
    Check("fare_plausible_for_duration", "business",
          f"fare_amount > 100 AND trip_distance = 0 AND {DURATION} < 60", TOL),
    Check("no_tip_recorded_on_cash", "business",
          "payment_type = 2 AND tip_amount > 0", TOL),
    Check("fare_implies_some_distance", "business",
          "fare_amount > 0 AND trip_distance = 0", TOL),
    Check("implied_speed_under_100mph", "business",
          f"try_divide(trip_distance, {DURATION} / 3600.0) > 100", TOL),
]

# -- the cast checks, generated ---------------------------------------------
for col, typ in CAST_COLUMNS.items():
    taxi_checks.append(Check(
        f"cast_ok_{col}", "validity",
        f"{col} IS NOT NULL AND try_cast({col} AS {typ}) IS NULL",
        CAST_FAIL, CAST_WARN, min_rows=0, blocking=True))
    if typ == "INT":
        # try_cast(2.7 AS INT) is 2, not NULL -- no error, no null, wrong value.
        taxi_checks.append(Check(
            f"no_fractional_loss_{col}", "validity",
            f"try_cast({col} AS DOUBLE) IS NOT NULL "
            f"AND try_cast({col} AS DOUBLE) <> ROUND(try_cast({col} AS DOUBLE))",
            CAST_FAIL, CAST_WARN, min_rows=0, blocking=True))

print(f"green_taxi: {len(taxi_checks)} row-level checks defined")


## weather and taxi_zones
### Weather is read as raw text on purpose
#The loader reads the CSV with a declared all-STRING schema, so that is how
#it is read here. Letting Spark infer types would hide the exact failure
#these checks exist to find: inference turns an unparseable value into NULL
#silently, and `temperature_parses` would then have nothing left to catch.

#Blank is not the same as absent — except in a table loaded entirely as
#STRING, where it is. `IS NULL` does not see `''` or `'   '`, so an empty
#cell would pass every completeness check *and* fail every parse check: the
#column reported as present and unreadable at once, which is the wrong
#diagnosis twice. Normalising once at read time means every check below
#inherits it.

### `one_row_per_hour` is advisory here, blocking in Bronze
# The weather MERGE deduplicates on `date` on
#the way in, so duplicates in the CSV are something it handles, not
#something to refuse the file over. In Bronze the same name means "the
#dedupe did not work", which is a real defect. It is reported here so the
#number is visible before the load, not to gate on.


WEATHER_COLUMNS = ["date", "temperature_2m", "apparent_temperature",
                   "precipitation_probability", "rain", "weather_code", "cloud_cover",
                   "visibility", "wind_speed_10m", "wind_gusts_10m", "month",
                   "latitude", "longitude", "source_series"]

ZONE_COLUMNS = ["LocationID", "Borough", "Zone", "service_zone"]

NUMERIC_WEATHER = ["temperature_2m", "apparent_temperature", "precipitation_probability",
                   "rain", "weather_code", "cloud_cover", "visibility",
                   "wind_speed_10m", "wind_gusts_10m"]

weather_checks = [
    Check("date_not_null",   "completeness", "`date` IS NULL", STRICT, min_rows=0, blocking=True),
    Check("date_parses",     "validity",
          "`date` IS NOT NULL AND try_cast(`date` AS TIMESTAMP) IS NULL",
          STRICT, min_rows=0, blocking=True),
    Check("month_not_null",  "completeness", "`month` IS NULL", TOL),
    Check("hour_within_batch_month", "validity",
          f"try_cast(`date` AS TIMESTAMP) IS NOT NULL AND "
          f"date_format(try_cast(`date` AS TIMESTAMP), 'yyyy-MM') <> '{BATCH_MONTH}'",
          ADVISORY),   # advisory: the timestamps are UTC, so boundary hours spill
    Check("month_agrees_with_date", "consistency",
          """try_cast(`date` AS TIMESTAMP) IS NOT NULL AND `month` IS NOT NULL
             AND trim(`month`) <> date_format(try_cast(`date` AS TIMESTAMP), 'yyyy-MM')
             AND COALESCE(try_cast(trim(`month`) AS INT), -1)
                 <> month(try_cast(`date` AS TIMESTAMP))""", TOL),
    Check("gusts_at_least_wind_speed", "consistency",
          "try_cast(wind_gusts_10m AS DOUBLE) < try_cast(wind_speed_10m AS DOUBLE)", TOL),
    Check("temperature_plausible", "validity",
          "try_cast(temperature_2m AS DOUBLE) NOT BETWEEN -30 AND 50", TOL),
    Check("precip_probability_0_to_100", "validity",
          "try_cast(precipitation_probability AS DOUBLE) NOT BETWEEN 0 AND 100", TOL),
    Check("cloud_cover_0_to_100", "validity",
          "try_cast(cloud_cover AS DOUBLE) NOT BETWEEN 0 AND 100", TOL),
    Check("rain_not_negative",       "validity", "try_cast(rain AS DOUBLE) < 0", TOL),
    Check("visibility_not_negative", "validity", "try_cast(visibility AS DOUBLE) < 0", TOL),
    Check("wind_speed_not_negative", "validity", "try_cast(wind_speed_10m AS DOUBLE) < 0", TOL),
    # WMO 4677 present-weather codes actually used by Open-Meteo.
    Check("weather_code_in_wmo_domain", "validity",
          """try_cast(weather_code AS DOUBLE) IS NOT NULL
             AND (try_cast(weather_code AS DOUBLE) <> ROUND(try_cast(weather_code AS DOUBLE))
               OR CAST(try_cast(weather_code AS DOUBLE) AS INT) NOT IN
                  (0,1,2,3,45,48,51,53,55,56,57,61,63,65,66,67,
                   71,73,75,77,80,81,82,85,86,95,96,99))""", TOL),
]

# Per-column pairs: missing, and present-but-unreadable. Same 5/10 pair the
# Bronze notebook gives these, since they answer the same question.
for col in NUMERIC_WEATHER:
    short = (col.replace("_2m", "").replace("_10m", "")
                .replace("apparent_temperature", "apparent_temp")
                .replace("precipitation_probability", "precip_probability")
                .replace("temperature", "temperature"))
    weather_checks.append(Check(f"{short}_not_null", "completeness", f"{col} IS NULL", TOL))
    weather_checks.append(Check(f"{short}_parses", "validity",
        f"{col} IS NOT NULL AND try_cast({col} AS DOUBLE) IS NULL",
        CAST_FAIL, CAST_WARN))

zone_checks = [
    Check("location_id_not_null",  "completeness", "LocationID IS NULL", STRICT,
          min_rows=0, blocking=True),
    Check("borough_not_null",      "completeness", "Borough IS NULL", TOL),
    Check("zone_name_not_null",    "completeness", "`Zone` IS NULL", TOL),
    Check("service_zone_not_null", "completeness", "service_zone IS NULL", TOL),
    Check("location_id_in_range",  "validity",
          """LocationID IS NOT NULL AND (try_cast(LocationID AS INT) IS NULL
             OR try_cast(LocationID AS INT) NOT BETWEEN 1 AND 265)""", TOL),
    Check("borough_in_domain", "validity",
          """Borough NOT IN ('Manhattan','Queens','Brooklyn','Bronx',
                             'Staten Island','EWR','Unknown','N/A')""", TOL),
    Check("service_zone_in_domain", "validity",
          "service_zone NOT IN ('Boro Zone','Yellow Zone','Airports','EWR','N/A')", TOL),
]

print(f"weather: {len(weather_checks)} | taxi_zones: {len(zone_checks)} row-level checks")


#RUN
#File presence is checked before the read, not caught from it. A missing
#file that surfaces as a Spark path exception gives a stack trace and no
#`dq_results` row; checked first, it is an ordinary recorded FAIL with a
#name, and the run log still describes the batch.

from pyspark.sql.types import StringType, StructType, StructField

def columns_used(check, candidates):
    """Which of the expected columns a check's SQL actually names. Word
    boundaries, so `extra` does not match inside another identifier."""
    text = f"{check.fail} {check.denom}"
    return [c for c in candidates if re.search(rf"\b{re.escape(c)}\b", text)]

def source_is_present(table, folder, filename):
    """Blocking only for a required source. A missing weather or zones file is
    a recorded FAIL that holds that source back, not a reason to refuse the
    whole batch."""
    names = list_names(folder)
    present = filename in names
    record(table, "schema", "expected_file_present",
           0 if present else 1, 1, STRICT, 0.0, 0,
           table in REQUIRED_TABLES, is_empty_check=True)
    if not present:
        print(f"  !! {table}: {filename} not found in {folder}")
        print(f"     folder contains: {names or '(empty)'}")
    return present

def not_empty(table, df):
    n = df.count()
    record(table, "completeness", "file_not_empty",
           0 if n else 1, 1, STRICT, 0.0, 0,
           table in REQUIRED_TABLES, is_empty_check=True)
    return n

# ---------------------------------------------------------------- green_taxi
print("green_taxi")
if source_is_present("green_taxi", TAXI_DIR, TAXI_FILE):
    taxi = spark.read.parquet(f"{TAXI_DIR}/{TAXI_FILE}")
    n_taxi = not_empty("green_taxi", taxi)
    if n_taxi:
        check_schema(taxi, "green_taxi", TAXI_COLUMNS,
                     blocking="green_taxi" in REQUIRED_TABLES)
        # Only run the value checks for columns that actually arrived -- a
        # check against a missing column raises, and the schema check has
        # already reported the real problem.
        have = set(taxi.columns)
        runnable, skipped = [], []
        for c in taxi_checks:
            (runnable if all(col in have for col in columns_used(c, TAXI_COLUMNS))
             else skipped).append(c)
        run_checks(taxi, "green_taxi", runnable)
        # A check whose column did not arrive is SKIP, not PASS. The schema
        # check above is what reports the missing column; these say honestly
        # that they had nothing to look at.
        for c in skipped:
            record("green_taxi", c.category, c.name, 0, 0)
        if skipped:
            print(f"  skipped (column absent): {[c.name for c in skipped]}")
        # Whole-row duplicates inside the file. The Bronze MERGE is INSERT-only
        # on source_file, so a duplicate here becomes a duplicate there.
        dupes, total = dup_count(taxi, taxi.columns)
        record("green_taxi", "uniqueness", "no_exact_duplicate_rows",
               dupes, total, TOL, 0.0, MIN_ROWS)
        print(f"  {n_taxi:,} rows")

# ------------------------------------------------------------------- weather
#
# The weather folder does not have to hold the same months as green-taxi.
# When this month's file is not there, the checks are recorded as SKIP and
# `expected_file_present` carries the finding -- the batch is not refused.
print("weather")
if source_is_present("weather", WEATHER_DIR, WEATHER_FILE):
    # All-STRING, exactly as the loader reads it. See the note above.
    # The header is read on its own first. The loader declares an all-STRING
    # schema, and a declared schema hides drift completely -- the DataFrame
    # would carry the expected column names whatever the file actually says.
    weather_header = (spark.read.option("header", "true")
                      .csv(f"{WEATHER_DIR}/{WEATHER_FILE}").limit(0).columns)

    schema = StructType([StructField(c, StringType(), True) for c in WEATHER_COLUMNS])
    weather_raw = (spark.read
                   .option("header", "true")
                   .schema(schema)
                   .csv(f"{WEATHER_DIR}/{WEATHER_FILE}"))
    # '' and '   ' become NULL once, here, rather than in every check.
    weather = weather_raw.select(*[
        F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c))).alias(c)
        for c in weather_raw.columns])
    n_weather = not_empty("weather", weather)
    if n_weather:
        check_schema(spark.createDataFrame([], StructType(
            [StructField(c, StringType(), True) for c in weather_header])),
            "weather", WEATHER_COLUMNS,
            blocking="weather" in REQUIRED_TABLES)
        run_checks(weather, "weather", weather_checks)
        # Advisory, not a gate -- the MERGE deduplicates on date by design.
        dupes, total = dup_count(weather.where(F.col("date").isNotNull()), ["date"])
        record("weather", "uniqueness", "one_row_per_hour",
               dupes, total, ADVISORY, 0.0, 0)
        print(f"  {n_weather:,} rows, {dupes} duplicate hour(s) the MERGE will collapse")

else:
    skip_all("weather", weather_checks)
    print(f"  {WEATHER_FILE} not present — weather checks SKIPPED, taxi continues")

# ---------------------------------------------------------------- taxi_zones
print("taxi_zones")
zone_files = [f for f in list_names(ZONES_DIR) if f.lower().endswith(".csv")]
record("taxi_zones", "schema", "expected_file_present",
       0 if zone_files else 1, 1, STRICT, 0.0, 0,
       "taxi_zones" in REQUIRED_TABLES, is_empty_check=True)
if zone_files:
    zones = (spark.read.option("header", "true").option("inferSchema", "false")
             .csv(f"{ZONES_DIR}/"))
    n_zones = not_empty("taxi_zones", zones)
    if n_zones:
        check_schema(zones, "taxi_zones", ZONE_COLUMNS,
                     blocking="taxi_zones" in REQUIRED_TABLES)
        run_checks(zones, "taxi_zones", zone_checks)
        # The lookup is a primary key or it is nothing: one duplicate fans out
        # every trip that joins to it.
        dupes, total = dup_count(zones.where(F.col("LocationID").isNotNull()), ["LocationID"])
        record("taxi_zones", "uniqueness", "location_id_unique",
               dupes, total, STRICT, 0.0, 0, blocking=True)
        # 265 rows and the three airport zones (1, 132, 138) are facts about
        # this reference file, so they are scalar checks, not rates.
        record("taxi_zones", "business", "lookup_has_265_zones",
               0 if n_zones == 265 else 1, 1, STRICT, 0.0, 0)
        airports = (zones.where(F.expr("try_cast(LocationID AS INT) IN (1,132,138)"))
                    .select("LocationID").distinct().count())
        record("taxi_zones", "business", "airport_zones_present",
               0 if airports == 3 else 1, 1, STRICT, 0.0, 0)
        print(f"  {n_zones:,} rows")

if not zone_files:
    skip_all("taxi_zones", zone_checks)
    print("  no CSV in the zones folder — zone checks SKIPPED")

skipped_sources = sorted({r["table_name"] for r in RESULTS
                          if r["check_name"] == "expected_file_present"
                          and r["status"] == "FAIL"})
if skipped_sources:
    print(f"\nheld back (file not present): {skipped_sources}")

print(f"\n{len(RESULTS)} checks recorded")


## Write the results
#Into `nyc_quality.dq_results` with `layer = 'preload'`, and the clear-down
#is on `(layer, batch_month)` — the same key the Bronze notebook uses. Two
#runs of the same month leave one set of rows, not two conflicting verdicts.

#Columns are aligned to the table by name rather than by position, so a
#future `ALTER TABLE ADD COLUMNS` in the setup notebook does not silently
#shift every value one place to the left.

from pyspark.sql.types import (StructType, StructField, StringType,
                               LongType, DoubleType, TimestampType)

# Explicit schema rather than inference. failed_pct is None whenever a check
# had no rows to look at, and if the first dict Spark sees carries that None
# the column is inferred as NullType and the write fails on a type it cannot
# reconcile -- a failure that depends on dictionary order, which is the worst
# kind to debug.
RESULTS_SCHEMA = StructType([
    StructField("run_id",          StringType()),
    StructField("run_ts",          TimestampType()),
    StructField("layer",           StringType()),
    StructField("table_name",      StringType()),
    StructField("check_category",  StringType()),
    StructField("check_name",      StringType()),
    StructField("failed_rows",     LongType()),
    StructField("total_rows",      LongType()),
    StructField("failed_pct",      DoubleType()),
    StructField("threshold_pct",   DoubleType()),
    StructField("status",          StringType()),
    StructField("batch_month",     StringType()),
    StructField("min_failed_rows", LongType()),
    StructField("warn_pct",        DoubleType()),
])

# ## One verdict row per source
#
# Mirrors `batch_cleared_for_silver` in the Bronze notebook. Without it, a
# Bronze task has to re-derive "was my source fit to load" from a scatter of
# individual check rows; with it, there is one row to read.
#
# It matters now that Bronze is three SQL files rather than one notebook.
# Each can guard itself:
#
#     SELECT CASE WHEN (SELECT status
#                       FROM   nyc_mobility.nyc_quality.dq_results
#                       WHERE  layer = 'preload'
#                         AND  batch_month = :year_month
#                         AND  table_name  = 'weather'
#                         AND  check_name  = 'source_cleared_for_bronze') <> 'PASS'
#            THEN raise_error('weather was not cleared by preload for ' || :year_month)
#            ELSE 'cleared' END AS gate;
#
# All three tasks depend on preload succeeding; each then loads only if its
# own source passed. A weather file that never arrived stops the weather MERGE
# and nothing else.
#
# Written into dq_results under check_category 'gate', and excluded from the
# run-log counts below so it does not inflate them.
for _tbl in ("green_taxi", "weather", "taxi_zones"):
    _blocked = [n for n in BLOCKING_FAILURES if n.startswith(f"{_tbl}.")]
    RESULTS.append(dict(
        run_id=RUN_ID, run_ts=RUN_TS, layer=LAYER, table_name=_tbl,
        check_category="gate", check_name="source_cleared_for_bronze",
        failed_rows=len(_blocked), total_rows=1,
        failed_pct=100.0 if _blocked else 0.0,
        threshold_pct=0.0, status="FAIL" if _blocked else "PASS",
        batch_month=batch_of(_tbl), min_failed_rows=0, warn_pct=0.0))

if not RESULTS:
    raise RuntimeError("No checks were recorded — nothing ran. Check the paths above.")

# The results table must have every column this notebook writes. Checking it
# here, by name, turns a cryptic failure into an instruction: without it the
# first sign of an out-of-date table is the DELETE below reporting
# UNRESOLVED_COLUMN on batch_month, which says nothing about what to do.
table_cols = {f.name for f in spark.table(RESULTS_TABLE).schema.fields}
needed     = {f.name for f in RESULTS_SCHEMA.fields}
absent     = sorted(needed - table_cols)
if absent:
    raise RuntimeError(
        f"{RESULTS_TABLE} is missing {absent}. Run the DQ setup notebook "
        f"(dq_setup v2) first — it adds these with ALTER TABLE, which is "
        f"additive and does not touch existing rows.")

runlog_needed = {"batch_month", "checks_skipped"}
runlog_absent = sorted(runlog_needed -
                       {f.name for f in spark.table(RUNLOG_TABLE).schema.fields})
if runlog_absent:
    raise RuntimeError(
        f"{RUNLOG_TABLE} is missing {runlog_absent}. Run the DQ setup notebook first.")

target_cols = [f.name for f in spark.table(RESULTS_TABLE).schema.fields]
results_df = spark.createDataFrame(
    [[r[f.name] for f in RESULTS_SCHEMA.fields] for r in RESULTS], RESULTS_SCHEMA)

# Any column the TABLE has that this notebook does not produce is written as
# NULL rather than failing -- a Silver-only column added later is not this
# notebook's problem.
_types = dict(spark.table(RESULTS_TABLE).dtypes)
out = results_df.select(*[
    F.col(c).cast(_types[c]) if c in results_df.columns
    else F.lit(None).cast(_types[c]).alias(c)
    for c in target_cols])

# Two clear-downs, because the static tables live in a batch of their own.
# One DELETE on BATCH_MONTH alone would leave the previous zone row set behind
# and the append would double it.
spark.sql(f"""
    DELETE FROM {RESULTS_TABLE}
    WHERE layer = '{LAYER}' AND batch_month = '{BATCH_MONTH}'
""")
spark.sql(f"""
    DELETE FROM {RESULTS_TABLE}
    WHERE layer = '{LAYER}' AND batch_month = '{STATIC_BATCH}'
      AND table_name IN ({", ".join(repr(t) for t in sorted(STATIC_TABLES))})
""")
out.write.mode("append").saveAsTable(RESULTS_TABLE)

# ---- run log -------------------------------------------------------------
# SKIP is excluded from the WARN test: a skipped check did not find anything
# wrong, it did not run, and counting it would put an incomplete batch
# permanently on WARN for reasons unrelated to quality.
# Gate rows are a summary of the others, so counting them would double-count.
_checks = [r for r in RESULTS if r["check_category"] != "gate"]
counts = {s: sum(1 for r in _checks if r["status"] == s)
          for s in ("PASS", "WARN", "FAIL", "SKIP")}
overall = ("FAIL" if BLOCKING_FAILURES
           else "WARN" if counts["WARN"] or counts["FAIL"]
           else "PASS")

spark.sql(f"""
    DELETE FROM {RUNLOG_TABLE}
    WHERE layer = '{LAYER}' AND batch_month = '{BATCH_MONTH}'
""")
spark.createDataFrame([dict(
    run_id=RUN_ID, run_ts=RUN_TS, layer=LAYER,
    tables_checked=len({r["table_name"] for r in _checks}),
    checks_run=len(_checks),
    checks_passed=counts["PASS"], checks_warned=counts["WARN"],
    checks_failed=counts["FAIL"], overall_status=overall,
    finished_at=datetime.now(),
    batch_month=BATCH_MONTH, checks_skipped=counts["SKIP"])]) \
    .select(*[F.col(c).cast(dict(spark.table(RUNLOG_TABLE).dtypes)[c])
              for c in [f.name for f in spark.table(RUNLOG_TABLE).schema.fields]]) \
    .write.mode("append").saveAsTable(RUNLOG_TABLE)

print(f"{BATCH_MONTH}: {counts}  overall={overall}")


#Results
print(f"zones version: {STATIC_BATCH}")

# Per-source verdicts — what each Bronze task will read.
display(spark.sql(f"""
    SELECT table_name, status AS cleared_for_bronze, failed_rows AS blocking_failures
    FROM   {RESULTS_TABLE}
    WHERE  layer = '{LAYER}'
      AND  batch_month IN ('{BATCH_MONTH}', '{STATIC_BATCH}')
      AND  check_name = 'source_cleared_for_bronze'
    ORDER  BY table_name
"""))

display(spark.sql(f"""
    SELECT table_name, status, COUNT(*) AS checks
    FROM   {RESULTS_TABLE}
    WHERE  layer = '{LAYER}'
      AND  batch_month IN ('{BATCH_MONTH}', '{STATIC_BATCH}')
      AND  check_category <> 'gate'
    GROUP  BY table_name, status
    ORDER  BY table_name, status
"""))

# Everything that is not a clean pass, worst first.
display(spark.sql(f"""
    SELECT table_name, check_category, check_name,
           failed_rows, total_rows, failed_pct, warn_pct, threshold_pct, status
    FROM   {RESULTS_TABLE}
    WHERE  layer = '{LAYER}'
      AND  batch_month IN ('{BATCH_MONTH}', '{STATIC_BATCH}')
      AND  status <> 'PASS' AND check_category <> 'gate'
    ORDER  BY CASE status WHEN 'FAIL' THEN 0 WHEN 'WARN' THEN 1 ELSE 2 END,
              failed_pct DESC
"""))

# Passed, but not cleanly. Only the cast and parse checks can land here --
# everything else has warn_pct = 0. Read it like a WARN: fine this month, and
# a number that climbs month over month is a declared type going wrong.
display(spark.sql(f"""
    SELECT table_name, check_name, failed_rows, total_rows, failed_pct, warn_pct
    FROM   {RESULTS_TABLE}
    WHERE  layer = '{LAYER}'
      AND  batch_month IN ('{BATCH_MONTH}', '{STATIC_BATCH}')
      AND  status = 'PASS' AND failed_rows > 0
    ORDER  BY failed_pct DESC
"""))


#GATE 
#Raising here means
# the Bronze task never starts and Bronze is never written to, so there is
# nothing to clean up afterwards.
# That matters specifically because of how the Bronze MERGE is keyed. It
# matches on `source_file`, so once any row from a file exists,
# `WHEN NOT MATCHED` never fires again — a half-finished load cannot be
# completed by re-running, only by deleting the month and starting over.
# Refusing the file is the cheap fix; noticing afterwards is the expensive one.

## What blocks
#| | |
#|---|---|
#| file missing or empty | there is nothing to load |
#| expected column absent | the MERGE would write a column of NULLs |
#| cast failure over 10% | the declared type is wrong for this file |
#| silent truncation over 10% | values are changing, not failing |
#| NULL or duplicate primary key | fans out or vanishes from every join |

#Everything else is recorded and the load proceeds. Set
#`GATE_ENFORCE = False` to run in report-only mode while you are still
#settling the thresholds — but a gate left that way indefinitely is not a gate.
GATE_ENFORCE = True

# A blocking failure says "this SOURCE is not fit to load". Whether that stops
# the run depends on whether the pipeline needs that source -- the same split
# the Bronze notebook makes with v_required_tables. green_taxi stopping is
# fatal; weather or zones stopping holds that source back and lets the trips
# through.
stopped = sorted({name.split(".", 1)[0] for name in BLOCKING_FAILURES})
stopped_required = [t for t in stopped if t in REQUIRED_TABLES]
stopped_optional = [t for t in stopped if t not in REQUIRED_TABLES]

fails = [r for r in RESULTS if r["status"] == "FAIL"]
print(f"batch_month       : {BATCH_MONTH}")
print(f"blocking failures : {len(BLOCKING_FAILURES)}")
print(f"total failures    : {len(fails)}")

for name in BLOCKING_FAILURES:
    r = next(x for x in RESULTS if f'{x["table_name"]}.{x["check_name"]}' == name)
    tag = "REQUIRED" if r["table_name"] in REQUIRED_TABLES else "held back"
    pct = f"{r['failed_pct']}%" if r["failed_pct"] is not None else "n/a"
    print(f"  [{tag}] {name}: {r['failed_rows']:,} of {r['total_rows']:,} "
          f"({pct}) against a {r['threshold_pct']}% limit")

if stopped_optional:
    print(f"\nheld back from Bronze : {stopped_optional}")
    print("  the trip path continues; load these once the source is fixed.")

# ## Hand the verdicts to the job
#
# Task values are how a per-source verdict reaches the Bronze tasks without
# any of them being edited. Each Bronze SQL file stays exactly as it is; the
# job puts an If/else condition in front of it that reads the value below.
#
# Gating belongs in orchestration rather than inside the load: a guard pasted
# into three SQL files is three copies of the same policy to keep in step, and
# it puts a quality decision in the middle of a statement whose job is to
# move rows.
#
# `batch_month` is published too, so the job can pass it straight into the
# Bronze and QC tasks as their `year_month` parameter -- nothing downstream
# has to work the month out again.
try:
    for _t in ("green_taxi", "weather", "taxi_zones"):
        dbutils.jobs.taskValues.set(
            key=f"cleared_{_t}",
            value="PASS" if _t not in stopped else "FAIL")
    dbutils.jobs.taskValues.set(key="batch_month",   value=BATCH_MONTH)
    dbutils.jobs.taskValues.set(key="zones_version", value=STATIC_BATCH)
    dbutils.jobs.taskValues.set(key="weather_file",  value=WEATHER_FILE)
    print("\ntask values published:",
          {f"cleared_{t}": ("PASS" if t not in stopped else "FAIL")
           for t in ("green_taxi", "weather", "taxi_zones")},
          "| batch_month:", BATCH_MONTH)
except Exception as e:
    # Running the notebook by hand rather than as a job task. The verdicts are
    # already in dq_results either way; only the orchestration hand-off is
    # unavailable, and saying so beats a stack trace.
    print(f"\n(not running as a job task, so no task values published: {e})")

if GATE_ENFORCE and stopped_required:
    raise Exception(
        f"Preload gate FAILED for {BATCH_MONTH} — required source(s) not fit to "
        f"load: {', '.join(stopped_required)}. Bronze was NOT written. "
        f"See {RESULTS_TABLE} for run {RUN_ID}.")

print(f"\nPreload gate PASSED for {BATCH_MONTH} "
      f"({len(fails)} failure(s) recorded) — safe to load "
      f"{sorted(set(REQUIRED_TABLES) | {t for t in ('green_taxi','weather','taxi_zones') if t not in stopped})}.")
