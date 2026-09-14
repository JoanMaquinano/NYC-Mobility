# Data Quality Framework

## 1. DQ Strategy

The NYC Mobility project implements data quality controls throughout the Medallion Architecture to ensure that the Gold layer contains trusted, complete, consistent, and analytics-ready data.

Data quality checks are applied at each layer:

```text
Bronze
→ Source ingestion validation

Silver
→ Standardization, cleansing, deduplication, and transformation validation

Gold
→ Business-rule validation, relationship validation, and analytical readiness checks
```

The framework aims to prevent:

- Missing critical data
- Duplicate records
- Invalid values
- Broken table relationships
- Inconsistent business results
- Incorrect aggregations

---

## 2. DQ Dimensions

### Completeness

Ensures required fields contain valid values.

Examples:

- Pickup datetime exists
- Dropoff datetime exists
- Pickup LocationID exists
- Dropoff LocationID exists

---

### Uniqueness

Ensures duplicate business records do not exist.

Examples:

- Duplicate taxi trips
- Duplicate weather observations
- Duplicate taxi zones
- Duplicate traffic advisories

---

### Validity

Ensures data values comply with business rules and acceptable ranges.

Examples:

- Trip distance is not negative
- Fare amount is not negative
- Passenger count is not negative

---

### Consistency

Ensures relationships and mappings remain valid across datasets.

Examples:

- Taxi Zone IDs match lookup data
- Weather joins are successful
- Dimension keys exist for fact records

---

### Timeliness

Ensures data is loaded according to expected schedules.

Examples:

- Monthly taxi files are available
- Weather API ingestion succeeds
- Traffic advisory scraping completes successfully

---

## 3. Validation Rules

### Check Name

Required Trip Fields

**Description**

Verify that required trip fields are populated.

**Expected Result**

```text
No missing required values.
```

---

### Check Name

Duplicate Taxi Trips

**Description**

Verify duplicate trip records do not exist.

**Expected Result**

```text
Zero duplicate records.
```

---

### Check Name

Valid Trip Distance

**Description**

Verify trip distance values are valid.

**Expected Result**

```text
trip_distance >= 0
```

---

### Check Name

Valid Fare Amount

**Description**

Verify fare amounts are valid.

**Expected Result**

```text
fare_amount >= 0
```

---

### Check Name

Taxi Zone Lookup Match

**Description**

Verify all taxi location IDs successfully match Taxi Zone lookup data.

**Expected Result**

```text
100% successful zone mapping.
```

---

### Check Name

Weather Join Coverage

**Description**

Verify weather observations can be joined to mobility records.

**Expected Result**

```text
Expected weather join coverage achieved.
```

---

### Check Name

Source Availability

**Description**

Verify required files and source data are available.

**Expected Result**

```text
All required data successfully loaded.
```

---

## 4. Severity Levels

### Critical

**Definition**

Issues that prevent trusted analytics or indicate major data failure.

**Examples**

- Missing source data
- Failed fact table load
- Massive duplicate loads
- Gold tables not generated

**Required Action**

```text
Release blocked until resolved.
```

---

### High

**Definition**

Issues significantly impacting analytical accuracy.

**Examples**

- Failed dimension joins
- Missing location mappings
- Invalid timestamps

**Required Action**

```text
Must be reviewed and resolved before release.
```

---

### Medium

**Definition**

Issues affecting only a subset of records.

**Examples**

- Partial weather join failures
- Minor lookup mismatches
- Limited missing values

**Required Action**

```text
Document and schedule remediation.
```

---

### Low

**Definition**

Minor issues with little business impact.

**Examples**

- Formatting inconsistencies
- Metadata issues
- Non-critical fields missing

**Required Action**

```text
Monitor and address during maintenance.
```

---

## 5. Validation Queries

### Check Name

Required Trip Fields

**Description**

Verify critical trip fields contain values.

**SQL Query**

```sql
SELECT COUNT(*)
FROM fact_green_taxi_trip
WHERE*pickup_datetime IS NULL
   OR drop*ff_datetime IS NULL
   OR pu_locat*on_id IS NULL
   OR do_location_id*IS NULL;
```

**Expected Result**
*```text
0 records returned.
```

-*-

### Check Name

Duplicate Taxi *rips

**Description**

Verify duplicate taxi trips do not exist.

**SQL Query**

```sql
SELECT
    trip_business_key,
    COUNT(*)
FROM fact_green_taxi_trip
GROUP BY trip_business_key
HAVING COUNT(*) > 1;
```

**Expected Result**

```text
0 records returned.
```

---

### Check Name

Negative Distance

**Description**

Verify trip distance values are valid.

**SQL Query**

```sql
SELECT COUNT(*)
FROM fact_green_taxi_trip
WHERE trip_distance < 0;
```

**Expected Result**

```text
0 records returned.
```

---

### Check Name

Negative Fare Amount

**Description**

Verify fare values are valid.

**SQL Query**

```sql
SELECT COUNT(*)
FROM fact_green_taxi_trip
WHERE *are_amount < 0;
```

**Expected Re*ult**

```text
0 records returned.*```

---

### Check Name

Taxi Zon* Coverage

**Description**

Verify*all pickup zones exist in the Taxi*Zone dimension.

**SQL Query**

``*sql
SELECT COUNT(*)
FROM fact_gree*_taxi_trip f
LEFT JOIN dim_taxi_zo*e z
    ON f.pu_location_id = z.lo*ation_id
WHERE z.location_id IS NU*L;
```

**Expected Result**

```te*t
0 records returned.
```

---

##* Check Name

Weather Join Coverage*
**Description**

Verify weather r*cords are available for mobility a*alysis.

**SQL Query**

```sql
SELECT COUNT(*)
FROM fact_green_taxi_trip f
LEFT JOIN dim_weather w
    ON f.trip_date = w.weather_date
WHERE w.weather_date IS NULL;
```

**Expected Result**

```text
0 records returned.
```

---

## 6. Exception Handling

When a DQ issue is identified:

### Critical Issues

```text
Stop processing.
Create issue.
Investigate root cause.
Fix before release.
```

---

### High Issues

```*ext
Flag affected records.
Documen* impact.
Resolve before release.
`*`

---

### Medium Issues

```text*Log issue.
Monitor trend.
Include*in*DQ reporting.
```

*--

### Low Issues

```text*Track for future cleanup*
Monitor recurrence.
```

---

###*Audit Requirements

All identified*issues should record:

```text
Iss*e ID
Date Detected
Dataset
DQ Chec* Name
Severity
Description
Busines* Impact
Owner
Status
Resolution Da*e
```

---

## 7* DQ Reporting Process

### Step 1
*Run Bronze-layer validation.

```t*xt
Validate source ingestion succe*s.
Validate source availability.
`*`

---

### Step 2

Run Silver-lay*r validation.

```text
Validate cl*ansing.
Validate deduplication.
Va*idate standardization.
```

---

#*# Step 3

Run Gold-layer validatio*.

```text
Validate business rules*
Validate dimension joins.
Validat* aggregation readiness.
```

---

*## Step 4

Calculate DQ metrics.

*xamples:

```text
Total records pr*cessed
Duplicate count
Null count
*ailed joins
Pass rate
```

---

##* Step 5

Review results.

```text
*ritical issues → Release blocked

*igh issues → Must be reviewed

Med*um issues → Logged and monitored

*ow issues → Tracked for maintenanc*
```

---

### Step 6

Publish DQ *esults.

Potential reporting outpu*s:

```text
DQ Dashboard
SQL Repor*ing Tables
Notebook Reports
Markdo*n Reports
GitHub Issues
```

---

* Notes / To Be Confirmed

## Busin*ss Key Definition

Confirm busines* key for:

```text
fact_green_taxi*trip
```

Possible options:

```text
Source identifier
Composite business key
Hash-based key
```

---

## Weather Validation Rules

Confirm:

```text
Required weather attributes
Weather join threshold
Accepted null strategy
```

---

## Traffic Advisory Validation Rules

Confirm:

```text
Traffic advisory grain
Required advisory fields
Duplicate advisory strategy
```

---

## DQ Dashboard

Confirm final reporting method:

```text
Dashboard
Notebook
SQL Tables
Markdown Reports
```

---

## DQ Thresholds

Confirm acceptable limits for:

```text
Duplicate records
Missing values
Failed joins
Weather coverage
Traffic advisory coverage
```

---

## Final Table Coverage

Confirm all implemented Gold tables and update validation coverage accordingly.