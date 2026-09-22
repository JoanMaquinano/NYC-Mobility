# Data Quality Framework

## Overview

The NYC Mobility project applies data quality controls across the Medallion Architecture to ensure that data remains trusted, traceable, consistent, and analytics-ready from ingestion through reporting.

```text
Bronze
→ Load Fidelity

Silver
→ Transformation Integrity

Gold
→ Star Schema Integrity
```

Each layer answers a different question:

| Layer | Primary Question |
|---------|---------|
| Bronze | Did we load the source data correctly? |
| Silver | Did our transformations behave correctly? |
| Gold | Does the dimensional model remain valid? |

A failed data quality check does not automatically stop a pipeline. Whether processing continues depends on the rule threshold and whether the rule is designated as blocking.

---

# DQ Status Model

Three different objects can be classified as PASS, WARN, or FAIL.

| Object | Purpose |
|----------|----------|
| Check | Result of an individual DQ rule |
| Data Row | Quality status of a specific Silver record |
| Layer | Quality status of an entire Bronze, Silver, or Gold run |

These statuses are independent.

For example:

```text
Layer = WARN

Rows:
PASS = 120,000
WARN = 5,000
FAIL = 200
```

This is expected behavior when source data contains issues that do not invalidate the pipeline.

---

# Threshold Strategy

Each rule calculates the percentage of records that violate a condition.

```text
failed_pct = 0
→ PASS

failed_pct > 0 and <= threshold
→ WARN

failed_pct > threshold
→ FAIL
```

Only three threshold levels are used throughout the framework.

| Threshold | Meaning |
|------------|------------|
| 0.0 | Impossible condition |
| 5.0 | Source imperfections tolerated |
| 100.0 | Informational metric only |

Examples:

```text
Dropoff before pickup
→ 0.0

Extreme trip distance
→ 5.0

Coverage statistics
→ 100.0
```

---

# Pipeline Gating

A failed check only stops the pipeline when:

1. The rule exceeds its threshold.
2. The rule is designated as blocking.

```text
Rule FAIL
    +
Blocking Rule
    =
Pipeline Stops

Rule FAIL
    +
Non-Blocking Rule
    =
Pipeline Continues
```

Important:

- All blocking rules use threshold 0.0.
- Not all 0.0-threshold rules are blocking.
- Non-blocking failures are logged for investigation.

---

# Bronze Layer

## Purpose

Bronze validates ingest quality and load fidelity.

Question:

```text
Did the table receive exactly what was present in the source file?
```

### Datasets

| Table | Purpose |
|---------|---------|
| green_taxi | Taxi trip source ingestion |
| weather | Weather source ingestion |
| taxi_zones | Taxi zone lookup ingestion |

### Key Validations

#### Load Completeness

- Source files loaded successfully
- Every landed file ingested
- Row counts match source files
- No unexpected data loss during ingestion

#### Required Fields

Required business keys must exist:

- pickup timestamp
- dropoff timestamp
- pickup zone
- dropoff zone
- weather date
- taxi zone location_id
- source_file

#### Load Fidelity

Bronze focuses on preservation, not cleansing.

Examples:

- No nulls introduced by ingestion
- Schema correctly mapped
- Source values preserved

#### Structural Integrity

- Tables are not empty
- Taxi zone location_id values remain unique
- Weather contains one record per hour
- Zone references can resolve

### What Stops Processing

Examples:

- Missing source rows
- Empty tables
- Null business keys
- Duplicate weather hours
- Invalid taxi zone mappings

### What Only Warns

Examples:

- Extreme fares
- Extreme distances
- Extreme temperatures
- Suspicious but possible source values

Bronze prioritizes loading source data faithfully rather than correcting it.

---

# Silver Layer

## Purpose

Silver validates transformation correctness.

Question:

```text
Did our transformation logic behave correctly?
```

### Datasets

| Table | Purpose |
|---------|---------|
| green_taxi_clean | Clean taxi trips |
| weather_clean | Clean weather observations |
| taxi_zones_clean | Clean taxi zone lookup |

### Row-Level Classification

Each row receives a DQ status.

```sql
PASS
WARN
FAIL
```

Based on:

```sql
qc_error_descriptions
```

### FAIL Conditions

Records are classified as FAIL when:

1. Pickup or dropoff timestamp is null
2. Dropoff occurs before pickup
3. Invalid taxi zone reference
4. Pickup date lies outside valid operating period
5. Source lineage is missing

### WARN Conditions

Examples:

- Unusual trip distances
- Unusual fare values
- Zero passengers
- Source anomalies

WARN rows remain available for analytics.

### Views

| View | Contents |
|---------|---------|
| vw_green_taxi_valid | PASS and WARN rows |
| vw_green_taxi_quarantined | FAIL rows |
| vw_green_taxi_unclassified | Diagnostic view |

### Key Validations

#### Reconciliation

- Silver row counts reconcile to Bronze
- Revenue totals reconcile to Bronze
- Every Bronze file appears in Silver

#### Classification Integrity

Validation ensures:

- PASS rows contain no issues
- FAIL rows contain FAIL-level issues
- WARN rows contain only WARN-level issues
- dq_status aligns with issue arrays

#### Quarantine Logic

Validation confirms that rows expected to fail are quarantined.

Examples:

- Null timestamps
- Reversed trips
- Invalid dates
- Invalid zone references
- Missing lineage

#### Grain Integrity

- One row per merge key
- One row per weather hour
- One row per taxi zone

### What Stops Processing

Examples:

- Missing rows after transformation
- Failed reconciliation
- Misclassified records
- Broken grain
- Incomplete taxi zone lookup

### What Only Warns

Examples:

- High quarantine rate
- High deduplication rate
- Weather coverage limitations
- Numerical outliers

---

# Gold Layer

## Purpose

Gold validates dimensional model integrity.

Question:

```text
Can the star schema support reliable analytics?
```

### Datasets

| Table | Purpose |
|---------|---------|
| fact_taxi_trip | Trip-level fact table |
| dim_weather | Weather dimension |
| dim_date | Date dimension |
| dim_taxi_zone | Taxi zone dimension |

### Key Validations

#### Dimension Integrity

- Keys are unique
- Keys are not null
- Dimension grain remains valid

Examples:

```text
date_key_unique
weather_key_unique
location_id_unique
```

#### Fact Integrity

- One row per trip_key
- Revenue preserved from Silver
- Fact row counts reconcile to Silver

#### Relationship Integrity

- Pickup zones resolve
- Dropoff zones resolve
- Weather references resolve
- Date references resolve

#### Table Health

- Tables are populated
- Joins remain valid
- Expected dimensional relationships exist

### What Stops Processing

Examples:

- Duplicate dimension keys
- Null dimension keys
- Missing zone references
- Fact grain violations
- Failed reconciliation
- Empty Gold tables

### What Only Warns

Examples:

- Temporary weather key gaps
- Date range coverage issues
- Gold rows derived from WARN-level Silver records
- At-rest consistency checks

---

# Severity Model

| Severity | Meaning | Action |
|------------|------------|------------|
| FAIL | Blocking rule violation | Stop processing if rule is blocking |
| WARN | Non-blocking quality issue | Continue and monitor |
| PASS | Condition satisfied | No action required |

---

# Current DQ Architecture

```text
Source Files
     ↓
Bronze
     ↓
Load Fidelity Checks
     ↓
Silver
     ↓
Transformation Validation
     ↓
PASS / WARN / FAIL Classification
     ↓
Gold
     ↓
Star Schema Validation
     ↓
Analytics Consumption
```

---

# Current Gate Configuration

| Layer | Gate Mode |
|---------|---------|
| Bronze | Report Only |
| Silver | Report Only |
| Gold | Enforced |

In report-only mode:

- Rules execute normally.
- Violations are recorded.
- Failures are reported.
- Processing continues.

Once outstanding issues are resolved, gate enforcement can be re-enabled.

---

# Rule of Thumb

> Bronze protects the load.
>
> Silver protects the transformation.
>
> Gold protects the joins.
>
> Threshold 0.0 means "this should be impossible."
>
> Threshold 5.0 means "source systems are imperfect."
>
> Threshold 100.