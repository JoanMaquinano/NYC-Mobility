# Table Specifications

## Purpose

This document defines the structure, purpose, grain, keys, sources, and major columns for all implemented tables within the NYC Mobility data platform.

---

# bronze_green_taxi

## Table Information

**Purpose**

Stores raw NYC Green Taxi trip records ingested from source parquet files.

**Grain**

One row per taxi trip.

**Business Key**

- VendorID
- lpep_pickup_datetime
- lpep_dropoff_datetime
- PULocationID
- DOLocationID

**Source**

NYC Green Taxi Trip Records

**Load Frequency**

Monthly

---

## Major Columns

| Column Name |
|------------|
| vendor_id |
| lpep_pickup_datetime |
| lpep_dropoff_datetime |
| pu_location_id |
| do_location_id |
| passenger_count |
| trip_distance |
| fare_amount |
| tip_amount |
| tolls_amount |
| total_amount |
| payment_type |
| trip_type |
| congestion_surcharge |
| cbd_congestion_fee |
| ingestion_timestamp |
| source_file_month |

---

# bronze_taxi_zones

## Table Information

**Purpose**

Stores raw taxi zone reference data.

**Grain**

One row per taxi zone.

**Business Key**

- location_id

**Source**

NYC Taxi Zone Lookup

**Load Frequency**

As Needed

---

## Major Columns

| Column Name |
|------------|
| location_id |
| borough |
| zone |
| service_zone |
| ingestion_timestamp |

---

# bronze_weather

## Table Information

**Purpose**

Stores raw weather observations from Open-Meteo.

**Grain**

One row per weather timestamp.

**Business Key**

- date

**Source**

Open-Meteo Historical Forecast API

**Load Frequency**

Monthly Batch

---

## Major Columns

| Column Name |
|------------|
| date |
| temperature_2m |
| apparent_temperature |
| precipitation_probability |
| rain |
| weather_code |
| cloud_cover |
| visibility |
| wind_speed_10m |
| wind_gusts_10m |
| month |
| source_file_month |
| ingestion_timestamp |

**Notes**

- Weather represents a single NYC location.
- Latitude and longitude are intentionally excluded because they are constant for the selected weather source.

---

# silver_green_taxi

## Table Information

**Purpose**

Stores cleaned and standardized Green Taxi trip records.

**Grain**

One row per cleaned taxi trip.

**Source**

bronze_green_taxi

---

## Transformations

- Data type standardization
- Null handling
- Business rule filtering
- Derived trip duration metrics

---

# silver_taxi_zones

## Table Information

**Purpose**

Stores standardized taxi zone reference data.

**Grain**

One row per taxi zone.

**Business Key**

- location_id

**Source**

bronze_taxi_zones

---

## Major Columns

| Column Name |
|------------|
| location_id |
| borough |
| zone |
| service_zone |

---

# silver_weather

## Table Information

**Purpose**

Stores cleaned weather observations used for analytical joins.

**Grain**

One row per weather timestamp.

**Business Key**

- date

**Source**

bronze_weather

---

## Major Columns

| Column Name |
|------------|
| date |
| temperature_2m |
| apparent_temperature |
| precipitation_probability |
| rain |
| weather_code |
| cloud_cover |
| visibility |
| wind_speed_10m |
| wind_gusts_10m |

**Notes**

- `weather_description` is calculated during transformation logic but is not persisted in the table schema.

---

# gold_trip_analytics

## Table Information

**Purpose**

Supports trip volume and ride activity analysis.

**Grain**

One row per pickup_date × location_id × vendor_id.

**Source**

silver_green_taxi

---

## Metrics

| Metric |
|----------|
| trip_count |
| passenger_count |
| total_fare |
| total_distance |
| average_fare |

---

# gold_weather_impact

## Table Information

**Purpose**

Supports weather impact analysis on taxi operations.

**Grain**

One row per weather_date × location_id.

**Sources**

- silver_green_taxi
- silver_weather

---

## Metrics

| Metric |
|----------|
| average_temperature |
| precipitation_probability |
| rain |
| average_fare |
| average_distance |

**Notes**

- Weather joins occur at the date level.
- Weather represents a single NYC weather source shared across all taxi zones.
- Trip count was intentionally excluded to avoid overlap with other reporting tables.

---

# gold_zone_performance

## Table Information

**Purpose**

Supports taxi zone benchmarking and performance comparisons.

**Grain**

One row per reporting_date × location_id.

**Sources**

- silver_green_taxi
- silver_taxi_zones

---

## Metrics

| Metric |
|----------|
| trip_count |
| revenue |
| average_trip_distance |
| average_fare |
| borough_ranking |

---

# gold_daily_kpis

## Table Information

**Purpose**

Provides executive-level daily KPI reporting.

**Grain**

One row per calendar_date.

**Sources**

- gold_trip_analytics
- gold_weather_impact

---

## Metrics

| Metric |
|----------|
| total_trips |
| total_revenue |
| average_fare |
| average_distance |
| average_temperature |
| weather_conditions |

---

# Key Design Decisions

## Weather Integration

- Weather data is sourced from Open-Meteo Historical Forecast API.
- Weather data is filtered to NYC during ingestion.
- Weather joins occur at date grain.
- Weather key is derived from weather date.
- Latitude and longitude were removed from Bronze and Silver weather tables.

---

## Idempotent Loading

- Taxi zone loads use MERGE operations.
- Weather loads use MERGE operations.
- Reprocessing source files should not create duplicate business records.

---

## Catalog Standard

- Use nyc_mobility
- Avoid `workspace.default`

---

## Current Data Model

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