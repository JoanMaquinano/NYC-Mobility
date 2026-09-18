# Grain Definitions

## 1. Purpose

This document defines the grain of each table within the NYC Mobility data platform.

Clearly defining grain ensures:

- Consistent business meaning across tables
- Correct join behavior
- Prevention of duplicate records and double-counting
- Accurate aggregation and reporting
- Reliable KPI calculations across Bronze, Silver, and Gold layers

---

## 2. Bronze Layer Grain

### bronze_green_taxi

**Grain:** One row per source taxi trip.

Each record represents a single Green Taxi trip from the source parquet files.

**Business Key:**
- VendorID
- lpep_pickup_datetime
- lpep_dropoff_datetime
- PULocationID
- DOLocationID

---

### bronze_taxi_zones

**Grain:** One row per taxi zone.

Each record represents a unique NYC TLC taxi zone.

**Business Key:**
- LocationID

---

### bronze_weather

**Grain:** One row per weather timestamp.

Each record represents a weather observation from Open-Meteo.

**Business Key:**
- date

---

## 3. Silver Layer Grain

### silver_green_taxi

**Grain:** One row per cleaned taxi trip.

Each record represents a validated and standardized taxi trip after business-rule processing.

---

### silver_taxi_zones

**Grain:** One row per taxi zone.

Each record represents a standardized taxi zone reference record.

---

### silver_weather

**Grain:** One row per weather timestamp.

Each record represents a cleaned weather observation prepared for analytical joins.

---

## 4. Gold Layer Grain

### gold_trip_analytics

**Grain:** One row per pickup_date × location_id × vendor_id.

**Purpose:** Trip volume and ride activity analysis.

Example:

```text
2026-03-01 | 87 | Vendor 2
```

**Measures:**
- trip_count
- passenger_count
- total_fare
- total_distance
- average_fare

---

### gold_weather_impact

**Grain:** One row per weather_date × location_id.

**Purpose:** Analyze relationships between weather conditions and taxi activity.

Example:

```text
2026-03-01 | 87
```

**Measures:**
- average_temperature
- precipitation_probability
- rain
- average_fare
- average_distance

**Notes:**
- Weather is joined at date level.
- Weather represents a single NYC weather location.
- Many locations may share the same weather observation for a given date.

---

### gold_zone_performance

**Grain:** One row per reporting_date × location_id.

**Purpose:** Compare taxi zone performance across NYC.

Example:

```text
2026-03-01 | 87
```

**Measures:**
- trip_count
- revenue
- average_trip_distance
- average_fare
- borough_ranking

---

### gold_daily_kpis

**Grain:** One row per calendar_date.

**Purpose:** Executive-level daily reporting.

Example:

```text
2026-03-01
```

**Measures:**
- total_trips
- total_revenue
- average_fare
- average_distance
- average_temperature
- weather_conditions

---

## 5. Grain Validation Rules

### bronze_green_taxi

**Validation**

```text
One source trip should produce one bronze record.
```

**Checks**
- Duplicate business key detection
- Source-to-target row count validation

---

### bronze_taxi_zones

**Validation**

```text
One LocationID should appear only once.
```

**Checks**
- No duplicate LocationID values

---

### bronze_weather

**Validation**

```text
One weather timestamp should appear only once.
```

**Checks**
- No duplicate weather timestamps
- MERGE prevents duplicate weather loads

---

### silver_green_taxi

**Validation**

```text
One valid business trip should produce one silver record.
```

**Checks**
- No duplicated trips
- Business-rule compliance

---

### silver_taxi_zones

**Validation**

```text
One LocationID should produce one zone record.
```

**Checks**
- No duplicate LocationID values

---

### silver_weather

**Validation**

```text
One weather timestamp should produce one weather record.
```

**Checks**
- No duplicate timestamps
- Valid weather attributes

---

## 6. Join Expectations

### Taxi Trips → Taxi Zones

**Expected Relationship**

```text
Many Trips
      →
One Pickup Zone

Many Trips
      →
One Dropoff Zone
```

**Cardinality**

```text
Many-to-One
```

---

### Taxi Trips → Weather

**Expected Relationship**

```text
Many Trips
      →
One Weather Date
```

**Cardinality**

```text
Many-to-One
```

**Business Rule**

```text
Trips are joined to weather using trip date.
```

---

### Gold Tables → Weather

**Expected Relationship**

```text
Many Location-Date Records
             →
One Weather Date
```

**Cardinality**

```text
Many-to-One
```

Because weather is sourced from a single NYC location, the same weather observation may be associated with multiple zones on the same date.

---

## 7. Common Risks

### Double-Counting Risk

**Risk**

```text
Fact records join to duplicate dimension records.
```

**Result**

```text
Trip counts, revenue, and KPIs become inflated.
```

**Mitigation**
- Enforce unique business keys.
- Use MERGE for idempotent loads.

---

### Duplicate Load Risk

**Risk**

```text
Source files are ingested multiple times.
```

**Result**

```text
Duplicate records appear in reporting tables