# Data Model

## 1. Overview

The NYC Mobility project integrates NYC Green Taxi trip records, weather data, and taxi zone reference data into a trusted, analytics-ready platform using a Medallion Architecture approach.

### Data Sources

- NYC Green Taxi Trip Records
- Open-Meteo Historical Forecast API
- NYC Taxi Zone Lookup

### Business Objectives

The model supports:

- Mobility demand analysis
- Trip behavior analysis
- Geographic performance reporting
- Weather impact analysis
- Executive KPI reporting

---

## 2. Modeling Approach

The project follows a Medallion Architecture design.

```text
Source Systems
      ↓
Bronze (Raw Data)
      ↓
Silver (Cleaned & Standardized)
      ↓
Gold (Business Analytics)
```

### Bronze Layer

Stores raw source data with minimal transformation.

Tables:

- bronze_green_taxi
- bronze_taxi_zones
- bronze_weather

### Silver Layer

Stores standardized and validated business data.

Tables:

- silver_green_taxi
- silver_taxi_zones
- silver_weather

### Gold Layer

Stores aggregated analytics-ready datasets aligned to business reporting requirements.

Tables:

- gold_trip_analytics
- gold_weather_impact
- gold_zone_performance
- gold_daily_kpis

---

## 3. High-Level Data Flow

```text
bronze_green_taxi
        │
        ▼
silver_green_taxi
        │
        ├──────────────┐
        │              │
        ▼              ▼

gold_trip_analytics
gold_zone_performance

silver_weather
        │
        ▼

gold_weather_impact
        │
        ▼

gold_daily_kpis

silver_taxi_zones
        │
        ├──────────────┐
        │              │
        ▼              ▼

gold_trip_analytics
gold_zone_performance
```

---

## 4. Conceptual Model

### Taxi Trips

The taxi trip dataset is the primary business event within the platform.

```text
One completed taxi trip
        =
One business event
```

Taxi trips provide:

- Demand metrics
- Revenue metrics
- Distance metrics
- Passenger metrics

---

### Weather Data

Weather data provides contextual enrichment for mobility analysis.

```text
Many trips
      →
One weather date
```

Weather is:

- City-wide
- Date-based
- Shared by all zones for a given date

---

### Taxi Zones

Taxi zones provide the geographic dimension used throughout reporting.

```text
LocationID
      →
Zone
      →
Borough
```

Used for:

- Pickup analysis
- Dropoff analysis
- Zone performance reporting

---

## 5. Table Grains

### Bronze Layer

#### bronze_green_taxi

```text
One row per taxi trip
```

#### bronze_taxi_zones

```text
One row per taxi zone
```

#### bronze_weather

```text
One row per weather timestamp
```

---

### Silver Layer

#### silver_green_taxi

```text
One row per cleaned taxi trip
```

#### silver_taxi_zones

```text
One row per taxi zone
```

#### silver_weather

```text
One row per weather timestamp
```

---

### Gold Layer

#### gold_trip_analytics

```text
One row per pickup_date × location_id × vendor_id
```

#### gold_weather_impact

```text
One row per weather_date × location_id
```

#### gold_zone_performance

```text
One row per reporting_date × location_id
```

#### gold_daily_kpis

```text
One row per calendar_date
```

---

## 6. Relationships

### Taxi Trips → Taxi Zones

Relationship:

```text
Many Trips
      →
One Pickup Zone

Many Trips
      →
One Dropoff Zone
```

Cardinality:

```text
Many-to-One
```

Business Rule:

```text
Multiple trips may originate from or end in the same taxi zone.
```

---

### Taxi Trips → Weather

Relationship:

```text
Many Trips
      →
One Weather Date
```

Cardinality:

```text
Many-to-One
```

Business Rule:

```text
Trips are joined to weather using trip date.
```

---

### Gold Tables → Weather

Relationship:

```text
Many Location-Date Records
          →
One Weather Date
```

Cardinality:

```text
Many-to-One
```

Business Rule:

```text
Weather represents a single NYC weather source shared across all taxi zones.
```

---

## 7. Design Decisions

### Medallion Architecture

The project separates ingestion, transformation, and reporting concerns using Bronze, Silver, and Gold layers.

Benefits:

- Improved maintainability
- Clear data lineage
- Simpler troubleshooting
- Stronger data quality controls

---

### Weather Integration Strategy

Weather data is modeled separately from taxi data.

Business Rules:

- Weather data is sourced from Open-Meteo Historical Forecast API.
- Weather is filtered to NYC during ingestion.
- Weather joins occur at date grain.
- Weather keys are generated from weather dates.
- Weather represents a single NYC location.
- Latitude and longitude are excluded because they provide no analytical value for the selected source.

Benefits:

- Reduces duplication
- Simplifies weather analysis
- Improves maintainability

---

### Geographic Reference Strategy

Taxi zones are maintained as a dedicated lookup table.

Benefits:

- Consistent geographic reporting
- Reduced data duplication
- Centralized location definitions

---

### Gold-Layer Aggregation Strategy

The project uses analytics-ready aggregate tables instead of a trip-level Gold fact table.

Benefits:

- Faster reporting
- Simplified dashboards
- Business-focused outputs
- Reduced reporting complexity

---

### Idempotent Processing

Recurring pipelines are designed to be safely rerun.

Implementation:

- Weather ingestion uses MERGE.
- Taxi zone ingestion uses MERGE.
- Duplicate prevention is enforced through business keys and DQ checks.

Benefits:

- Consistent outputs
- Easier recovery from failures
- Reliable incremental processing

---

## 8. Data Model Risks

### Duplicate Load Risk

Risk:

```text
The same source file is loaded multiple times.
```

Mitigation:

- MERGE-based loading
- Duplicate monitoring
- Data quality validation

---

### Grain Mismatch Risk

Risk:

```text
Tables are joined at different levels of detail.
```

Mitigation:

- Explicit grain definitions
- Join validation checks

---

### Weather Join Risk

Risk:

```text
One trip joins to multiple weather records.
```

Mitigation:

```text
Weather joins occur at date grain only.
```

---

### Double Counting Risk

Risk:

```text
Duplicate dimension records inflate metrics.
```

Mitigation:

- Business key validation
- Uniqueness checks
- MERGE-based ingestion

---

## 9. Repository Structure

### GitHub Repository

The project uses a source-oriented structure.

```text
sources/
├── green_taxi/
├── taxi_zones/
└── weather/
```

Benefits:

- Source ownership is clearer
- Easier development by domain
- Better organization of ingestion logic

---

### Databricks Catalog

The Databricks implementation uses a layer-oriented structure.

```text
nyc_bronze
nyc_silver
nyc_gold
```

Benefits:

- Aligns with Medallion Architecture
- Simplifies governance
- Improves lineage visibility

---

## 10. Current Implemented Model

### Bronze

- bronze_green_taxi
- bronze_taxi_zones
- bronze_weather

### Silver

- silver_green_taxi
- silver_taxi_zones
- silver_weather

### Gold

- gold_trip_analytics
- gold_weather_impact
- gold_zone_performance
- gold_daily_kpis

---

## 11. ERD

Insert final DrawSQL ERD here.

```text
                    silver_weather
                           │
                           │
                           ▼

silver_taxi_zones ──► gold_weather_impact
         │
         │
         ▼

gold_zone_performance

         ▲
         │
         │
silver_green_taxi
         │
         ├────────────► gold_trip_analytics
         │
         ├────────────► gold_zone_performance
         │
         └────────────► gold_daily_kpis

gold_weather_impact
         │
         ▼

gold_daily_kpis
```