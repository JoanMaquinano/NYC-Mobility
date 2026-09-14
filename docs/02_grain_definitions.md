# Grain Definitions

## 1. Purpose

This document defines the grain of each fact and dimension table within the NYC Mobility data model.

Clearly defining grain ensures:

- Consistent business meaning across tables
- Correct join behavior between facts and dimensions
- Prevention of duplicate records and double-counting
- Accurate aggregation and reporting

---

## 2. Fact Table Grain

### fact_green_taxi_trip

**Grain:** One row per taxi trip.

Each record represents a single completed Green Taxi trip reported by the NYC TLC source system.

Examples:

- One pickup and one dropoff event
- One fare transaction
- One trip distance measurement

Expected measures:

- Trip count
- Trip duration
- Trip distance
- Fare amount
- Tip amount
- Total amount

---

### fact_traffic_advisory *(if implemented)*

**Proposed Grain:** One row per traffic advisory.

Each record represents a single published traffic advisory event.

Potential measures:

- Advisory count
- Affected locations
- Advisory duration

> Final grain to be confirmed during implementation.

---

## 3. Dimension Table Grain

### dim_taxi_zone

**Grain:** One row per LocationID.

Each record represents a unique NYC Taxi Zone.

Example:

```text
LocationID = 87
Zone = Financial District North
Borough = Manhattan
```

---

### dim_weather

**Proposed Grain:** One row per weather observation.

Possible implementations:

```text
One row per hour
```

or

```text
One row per day
```

The final grain depends on the selected weather integration strategy.

---

### dim_date *(if implemented)*

**Grain:** One row per calendar date.

Example:

```text
2026-03-01
2026-03-02
2026-03-03
```

Typical attributes:

- Day
- Week
- Month
- Quarter
- Year

---

### dim_time *(if implemented)*

**Grain:** One row per hour or time period.

Example:

```text
00:00
01:00
...
23:00
```

Used for hourly mobility analysis.

---

## 4. Grain Validation

The following validation rules help ensure grain consistency.

### fact_green_taxi_trip

Validation:

```text
One business trip should produce one fact record.
```

Checks:

- No duplicate trips
- No duplicate business keys
- No duplicate trip identifiers

---

### dim_taxi_zone

Validation:

```text
One LocationID should appear only once.
```

Checks:

- No duplicate LocationID values
- One zone description per LocationID

---

### dim_weather

Validation:

```text
One weather observation per defined weather grain.
```

Checks:

- No duplicate weather timestamps
- No duplicate weather dates if daily grain is used

---

### Dimensional Joins

Validation:

```text
Every fact record should successfully join to its related dimensions.
```

Checks:

- Taxi trips join to taxi zones
- Taxi trips join to weather observations
- Taxi trips join to date dimensions

---

## 5. Common Risks

### Double-Counting Risk

Risk:

```text
Fact records join to duplicate dimension records.
```

Example:

```text
One taxi trip joins to two weather records.
```

Result:

```text
Trip counts and revenue become inflated.
```

---

### Duplicate Record Risk

Risk:

```text
Source data is loaded multiple times.
```

Example:

```text
March dataset ingested twice.
```

Result:

```text
Duplicate trips appear in Gold tables.
```

---

### Grain Mismatch Risk

Risk:

```text
Facts and dimensions use different levels of detail.
```

Example:

```text
One trip joins to multiple advisory records.
```

Result:

```text
Unexpected row multiplication.
```

---

### Weather Join Risk

Risk:

```text
Weather data is stored at a finer grain than taxi trips.
```

Example:

```text
Trip date joins to hourly weather records.
```

Result:

```text
One trip may match multiple weather observations.
```

---

### Traffic Advisory Join Risk

Risk:

```text
Traffic advisories may affect multiple locations and dates.
```

Result:

```text
Complex many-to-many joins may occur.
```

Careful business rules are required before integrating traffic advisories into analytical fact tables.

---

## Join Expectations

### Taxi Trip → Taxi Zone

Expected Relationship:

```text
Many trips
        →
One pickup zone

Many trips
        →
One dropoff zone
```

Cardinality:

```text
Many-to-One
```

---

### Taxi Trip → Weather

Expected Relationship:

```text
Many trips
        →
One weather observation
```

Cardinality:

```text
Many-to-One
```

---

### Taxi Trip → Date

Expected Relationship:

```text
Many trips
        →
One date
```

Cardinality:

```text
Many-to-One
```

---

# Notes / To Be Confirmed

## Weather Grain

Confirm whether:

```text
One row per hour
```

or

```text
One row per day
```

This decision impacts joins, aggregations, and weather-related KPIs.

---

## Weather Join Logic

Confirm whether weather is joined using:

```text
Pickup Date
Pickup Datetime
Pickup Hour
Dropoff Datetime
```

---

## Final Gold Fact Grain

Confirm whether the primary Gold fact table remains:

```text
One row per taxi trip
```

or becomes:

```text
One row per day
One row per zone per day
One row per zone per day per weather condition
```

---

## Traffic Advisory Grain

Confirm whether traffic advisories are stored as:

```text
One row per advisory
One row per affected location
One row per advisory date
```

---

## Date and Time Dimensions

Confirm whether:

```text
dim_date
```

and/or

```text
dim_time
```

will be implemented as separate dimensions.