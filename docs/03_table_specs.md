# Table Specifications

## Purpose

This document defines the structure, purpose, grain, keys, sources, and column-level specifications for all tables within the NYC Mobility data model.

---

# fact_green_taxi_trip

## Table Information

**Purpose**

Stores Green Taxi trip transactions used for mobility demand and trip behavior analysis.

**Grain**

One row per taxi trip.

**Primary Key**

To be confirmed.

**Source**

NYC Green Taxi Trip Records

**Load Frequency**

Monthly

---

## Column Specifications

| Column Name | Data Type | Nullable | Description | Business Rule |
|------------|------------|------------|------------|------------|
| vendor_id | TBD | TBD | Taxi technology provider identifier | Values should match valid TLC vendor codes |
| lpep_pickup_datetime | TBD | TBD | Trip pickup timestamp | Represents trip start time |
| lpep_dropoff_datetime | TBD | TBD | Trip dropoff timestamp | Represents trip end time |
| pu_location_id | TBD | TBD | Pickup Taxi Zone ID | Must match Taxi Zone Lookup |
| do_location_id | TBD | TBD | Dropoff Taxi Zone ID | Must match Taxi Zone Lookup |
| passenger_count | TBD | TBD | Number of passengers | Should be non-negative |
| trip_distance | TBD | TBD | Distance travelled in miles | Should be non-negative |
| fare_amount | TBD | TBD | Base fare amount | Should be non-negative |
| tip_amount | TBD | TBD | Driver tip amount | May be null or zero |
| tolls_amount | TBD | TBD | Toll charges | May be zero |
| total_amount | TBD | TBD | Total trip amount | Should be greater than or equal to fare amount |
| payment_type | TBD | TBD | Payment method code | Must match valid TLC codes |
| trip_type | TBD | TBD | Street-hail or dispatch | Must match valid TLC codes |
| congestion_surcharge | TBD | TBD | Congestion surcharge | Source system value |
| cbd_congestion_fee | TBD | TBD | Congestion Relief Zone fee | Source system value |

---

# dim_taxi_zone

## Table Information

**Purpose**

Provides geographical reference data for pickup and dropoff analysis.

**Grain**

One row per LocationID.

**Primary Key**

location_id

**Source**

NYC Taxi Zone Lookup

**Load Frequency**

As Needed

---

## Column Specifications

| Column Name | Data Type | Nullable | Description | Business Rule |
|------------|------------|------------|------------|------------|
| location_id | TBD | No | Taxi Zone identifier | Must be unique |
| zone | TBD | No | Taxi Zone name | Source value |
| borough | TBD | No | Borough name | Source value |

---

# dim_weather

## Table Information

**Purpose**

Provides weather conditions used to enrich mobility analysis.

**Grain**

To be confirmed.

**Primary Key**

To be confirmed.

**Source**

Open-Meteo Historical Weather API

**Load Frequency**

Daily or batch ingestion

---

## Column Specifications

| Column Name | Data Type | Nullable | Description | Business Rule |
|------------|------------|------------|------------|------------|
| weather_date | TBD | TBD | Observation date | Used for weather joins |
| temperature_2m | TBD | TBD | Temperature measurement | Source value |
| precipitation | TBD | TBD | Precipitation value | Source value |
| wind_speed_10m | TBD | TBD | Wind speed measurement | Source value |
| weather_code | TBD | TBD | Weather condition code | Source value |

---

# dim_date

## Table Information

**Purpose**

Provides calendar attributes for reporting and aggregation.

**Grain**

One row per calendar date.

**Primary Key**

date_key

**Source**

System-generated

**Load Frequency**

Static / generated

---

## Column Specifications

| Column Name | Data Type | Nullable | Description | Business Rule |
|------------|------------|------------|------------|------------|
| date_key | TBD | No | Surrogate date key | Unique |
| calendar_date | TBD | No | Calendar date | Unique |
| day_of_week | TBD | No | Day name | Derived |
| week_number | TBD | No | Week number | Derived |
| month_number | TBD | No | Month number | Derived |
| year | TBD | No | Year value | Derived |

---

# fact_traffic_advisory

## Table Information

**Purpose**

Stores traffic advisories used to analyze mobility disruptions.

**Grain**

To be confirmed.

**Primary Key**

To be confirmed.

**Source**

NYC DOT Traffic Advisory Website

**Load Frequency**

Weekly

---

## Column Specifications

| Column Name | Data Type | Nullable | Description | Business Rule |
|------------|------------|------------|------------|------------|
| advisory_id | TBD | TBD | Advisory identifier | Unique if available |
| advisory_date | TBD | TBD | Advisory date | Source value |
| affected_location | TBD | TBD | Impacted area | Source value |
| advisory_description | TBD | TBD | Advisory details | Source value |

---

# Notes / To Be Confirmed

## Final Gold Tables

Confirm actual implemented tables:

- fact_green_taxi_trip
- dim_taxi_zone
- dim_weather
- dim_date
- fact_traffic_advisory

---

## Primary Keys

Confirm primary key strategy for:

- fact_green_taxi_trip
- dim_weather
- fact_traffic_advisory

---

## Data Types

Confirm actual Databricks data types for all columns.

Examples:

- STRING
- INT
- BIGINT
- DOUBLE
- DECIMAL
- DATE
- TIMESTAMP

---

## Nullable Rules

Confirm whether columns are:

- Required
- Optional
- Source-dependent

---

## Weather Columns

Confirm final weather attributes selected from Open-Meteo.

Examples:

- temperature_2m
- precipitation
- wind_speed_10m
- weather_code

---

## Traffic Advisory Columns

Confirm final advisory fields captured during scraping.

Examples:

- advisory date
- advisory title
- affected location
- closure details
- advisory description

---

## Derived Columns

Confirm additional calculated fields.

Examples:

- trip_duration_minutes
- pickup_hour
- pickup_day
- pickup_week
- pickup_month
- pickup_year

---

## Surrogate Key Implementation

Confirm which tables use surrogate keys and document key-generation logic.

## Data Dictionary
### taxi_zones_clean

| Column | Data Type | Description |
|----------|----------|----------|
| location_id | INT | Unique taxi zone identifier |
| borough | STRING | NYC borough |
| zone | STRING | Taxi zone name |
| service_zone | STRING | TLC service zone |
