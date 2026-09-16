# Data Quality Framework

## Overview

The NYC Mobility project applies data quality controls throughout the Medallion Architecture to ensure that Gold-layer data is trusted, complete, consistent, and analytics-ready.

```text
Bronze
→ Source ingestion validation

Silver
→ Cleaning, standardization, deduplication

Gold
→ Business validation and analytical readiness checks
```

The framework aims to prevent:

- Missing critical data
- Duplicate records
- Invalid values
- Broken relationships
- Inconsistent analytical results

---

# Data Quality Dimensions

## Completeness

Required fields contain values.

Examples:

- Pickup datetime exists
- Dropoff datetime exists
- Pickup LocationID exists
- Dropoff LocationID exists

## Uniqueness

Duplicate business records do not exist.

Examples:

- Duplicate taxi trips
- Duplicate weather observations
- Duplicate taxi zones

## Validity

Values comply with expected business rules.

Examples:

- trip_distance >= 0
- fare_amount >= 0
- passenger_count >= 0

## Consistency

Relationships remain valid across datasets.

Examples:

- Taxi zones successfully map
- Weather joins succeed
- Fact records match dimensions

## Timeliness

Data arrives according to expectations.

Examples:

- Monthly taxi data available
- Weather API accessible
- Traffic advisory scrape completed

---

# Validation Rules

| Check | Expected Result |
|---------|---------|
| Required Trip Fields | No missing required values |
| Duplicate Taxi Trips | Zero duplicates |
| Valid Trip Distance | trip_distance >= 0 |
| Valid Fare Amount | fare_amount >= 0 |
| Taxi Zone Lookup Match | 100% successful mapping |
| Weather Join Coverage | Expected coverage achieved |
| Source Availability | All sources loaded successfully |

---

# Severity Levels

| Severity | Description | Release Impact |
|-----------|-------------|----------------|
| Critical | Data cannot be trusted or processing failed | Release blocked |
| High | Material impact on analytical accuracy | Review and resolve before release |
| Medium | Subset of records affected | Document and schedule remediation |
| Low | Minor issue with limited business impact | Monitor and resolve during maintenance |

### Critical Examples

- Missing source data
- Pipeline failure
- Failed Gold load
- Large duplicate load

### High Examples

- Failed dimension joins
- Invalid timestamps
- Missing lookup mappings

### Medium Examples

- Partial join failures
- Limited missing values
- Minor transformation issues

### Low Examples

- Formatting issues
- Documentation gaps
- Metadata inconsistencies

---

# Exception Handling

| Severity | Action |
|-----------|---------|
| Critical | Stop processing and investigate |
| High | Review and resolve before release |
| Medium | Log issue and monitor |
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

## Duplicate Trip Check

```sql
SELECT
    trip_business_key,
    COUNT(*)
FROM fact_green_taxi_trip
GROUP BY trip_business_key
HAVING COUNT(*) > 1;
```

Expected Result:

```text
0 records returned
```

## Negative Distance Check

```sql
SELECT COUNT(*)
FROM fact_green_taxi_trip
WHERE trip_distance < 0;
```

Expected Result:

```text
0 records returned
`*`

## Weather Coverage Check

```sql
SELECT COUNT(*)
FROM fact_green_taxi_trip f
LEFT JOIN dim_weather w
    ON f.trip_date = w.weather_date
WHERE w.weather_date IS NULL;
```

Expected Result:

```text
0 records returned
```

---

# Open Items

The following items are still to be confirmed:

```text
Business key definition
Weather coverage thresholds
Traffic advisory validation rules
Duplicate thresholds
Missing value thresholds
Release blocking rules
DQ reporting approach
```
