# Data Quality Framework

## Overview

The NYC Mobility project applies data quality controls throughout the Medallion Architecture to ensure that Gold-layer data is trusted, complete, consistent, and analytics-ready.

```text
Bronze
→ Source ingestion validation

Silver
→ Cleaning, standardization, transformation, and business rule validation

Gold
→ Aggregation validation and analytical readiness checks
```

The framework aims to prevent:

- Missing critical data
- Duplicate records
- Invalid values
- Broken relationships
- Inconsistent analytical results
- Incorrect KPI calculations

---

# Data Quality Dimensions

## Completeness

Required fields contain expected values.

Examples:

- Pickup datetime exists
- Dropoff datetime exists
- Pickup LocationID exists
- Dropoff LocationID exists
- Weather observation date exists
- Taxi zone LocationID exists

---

## Uniqueness

Duplicate business records do not exist.

Examples:

- Duplicate taxi trips
- Duplicate weather observations
- Duplicate taxi zones
- Duplicate aggregate records

---

## Validity

Values comply with expected business rules.

Examples:

- trip_distance >= 0
- fare_amount >= 0
- passenger_count >= 0
- total_amount >= fare_amount
- weather measures fall within expected ranges

---

## Consistency

Relationships remain valid across datasets.

Examples:

- Taxi zones successfully map to trips
- Weather joins successfully map to trips
- Aggregated metrics reconcile to source data
- Gold tables align with Silver-layer outputs

---

## Timeliness

Data arrives according to defined processing schedules.

Examples:

- Monthly taxi files available
- Weather source files available
- Bronze ingestion completed successfully
- Silver and Gold processing completed successfully

---

# Validation Rules

| Check | Expected Result |
|---------|---------|
| Required Trip Fields | No missing required values |
| Duplicate Taxi Trips | Zero duplicates |
| Duplicate Taxi Zones | Zero duplicates |
| Duplicate Weather Records | Zero duplicates |
| Valid Trip Distance | trip_distance >= 0 |
| Valid Fare Amount | fare_amount >= 0 |
| Valid Passenger Count | passenger_count >= 0 |
| Taxi Zone Lookup Match | 100% successful mapping |
| Weather Join Coverage | Expected coverage achieved |
| Source Availability | All source files loaded successfully |
| Gold Aggregation Validation | Aggregates reconcile with source data |

---

# Layer-Specific Data Quality Checks

## Bronze Layer

### bronze_green_taxi

Checks:

- Source file exists
- Source row count validation
- Duplicate trip detection
- Required column validation
- Successful ingestion confirmation

---

### bronze_taxi_zones

Checks:

- Source file exists
- Duplicate LocationID detection
- Row count validation
- Schema validation

---

### bronze_weather

Checks:

- Weather file exists
- Duplicate weather timestamp detection
- Schema validation
- Weather row count validation
- Successful MERGE execution

---

## Silver Layer

### silver_green_taxi

Checks:

- Data type validation
- Null validation
- Business rule validation
- Negative distance validation
- Negative fare validation
- Derived field validation

---

### silver_taxi_zones

Checks:

- Duplicate LocationID detection
- Standardized column names
- Lookup integrity validation

---

### silver_weather

Checks:

- Weather attribute validation
- Type casting validation
- Duplicate timestamp validation
- Weather join readiness validation

---

## Gold Layer

### gold_trip_analytics

Checks:

- Aggregate reconciliation
- Trip count validation
- Revenue validation

---

### gold_weather_impact

Checks:

- Weather coverage validation
- Weather join validation
- Metric reconciliation

---

### gold_zone_performance

Checks:

- Revenue aggregation validation
- Zone ranking validation
- Location coverage validation

---

### gold_daily_kpis

Checks:

- Daily KPI reconciliation
- Revenue validation
- Trip count validation
- Weather attribute validation

---

# Severity Levels

| Severity | Description | Release Impact |
|-----------|-------------|----------------|
| Critical | Data cannot be trusted or processing failed | Release blocked |
| High | Material impact on analytical accuracy | Resolve before release |
| Medium | Subset of records affected | Document and monitor |
| Low | Minor issue with limited business impact | Monitor |

---

## Critical Examples

- Missing source data
- Pipeline failure
- Failed Bronze load
- Failed Silver load
- Failed Gold load
- Empty source table
- Large duplicate loads

---

## High Examples

- Failed weather joins
- Failed taxi zone mappings
- Invalid timestamps
- Invalid business key values
- Significant metric discrepancies

---

## Medium Examples

- Partial lookup failures
- Limited missing values
- Small reconciliation differences
- Minor transformation defects

---

## Low Examples

- Formatting inconsistencies
- Metadata issues
- Documentation gaps

---

# Exception Handling

| Severity | Action |
|-----------|---------|
| Critical | Stop processing and investigate |
| High | Resolve before release |
| Medium | Log and monitor |
| Low | Track for future cleanup |

---

# Audit Requirements

All identified issues should capture:

```text
Issue ID
Date Detected
Dataset
Severity
Description
Business Impact
Owner
Status
Resolution Date
```

---

# Validation Query Examples

## Duplicate Taxi Zone Check

```sql
SELECT
    location_id,
    COUNT(*)
FROM nyc_bronze.taxi_zones
GROUP BY location_id
HAVING COUNT(*) > 1;
```

Expected Result:

```text
0 records returned
```

---

## Negative Distance Check

```sql
SELECT COUNT(*)
FROM nyc_silver.green_taxi_clean
WHERE trip_distance < 0;
```

Expected Result:

```text
0 records returned
```

---

## Weather Coverage Check

```sql
SELECT COUNT(*)
FROM fact_taxi_trip f
LEFT JOIN weather_clean w
    ON DATE(f.pickup_datetime) = DATE(CAST(w.date AS TIMESTAMP))
WHERE w.date IS NULL;
```

Expected Result:

```text
0 records returned
```

---

## Taxi Zone Mapping Check

```sql
SELECT COUNT(*)
FROM fact_taxi_trip f
LEFT JOIN taxi_zones_clean z
    ON f.pickup_location_id = z.location_id
WHERE z.location_id IS NULL;
```

Expected Result:

```text
0 records returned
```

---

# Known Issues Resolved

### Weather Table Empty

Issue:

```text
bronze_weather contained 0 records.
```

Resolution:

```text
Loaded all 2,208 weather records from source CSV files.
```

Impact:

```text
Resolved multiple blocking DQ failures.
```

---

### Incomplete Green Taxi Load

Issue:

```text
Only one monthly file was loaded.
```

Resolution:

```text
Loaded all five source parquet files.
```

Result:

```text
211,012 total records loaded.
```

---

### Taxi Zone Duplication Risk

Issue:

```text
INSERT-based processing created duplicate taxi zone records.
```

Resolution:

```text
Replaced INSERT with MERGE for idempotent loading.
```

---

### Weather Validation Failure

Issue:

```text
Validation query referenced weather_description which is not stored in silver_weather.
```

Resolution:

```text
Removed weather_description from validation query.
```

---

### Weather Join Failure

Issue:

```text
Weather join referenced a nonexistent weather_timestamp_utc column.
```

Resolution:

```text
Updated join logic to use CAST(w.date AS TIMESTAMP) and generated weather_key from weather date.
```

---

# Current Data Quality Status

✅ Weather ingestion fixed

✅ Weather validation fixed

✅ Weather joins fixed

✅ Weather foreign key generation fixed

✅ Green taxi source data fully loaded

✅ Taxi zone ingestion converted to MERGE

⚠️ Historical duplicate taxi zone records may still require one-time cleanup before final validation

---

# Ongoing DQ Standards

- All recurring loads must be idempotent.
- MERGE should be used where duplicate loads are possible.
- Business keys must remain unique.
- Weather joins must occur at date grain.
- All Gold metrics must reconcile to Silver-layer source data.
- DQ checks must execute before Gold publication.