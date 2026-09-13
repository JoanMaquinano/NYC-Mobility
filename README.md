# NYC Mobility Data Pipeline

## Overview

This project builds a scalable and repeatable data engineering pipeline that combines multiple NYC public datasets into a trusted mobility analytics dataset.

The pipeline follows the Medallion Architecture pattern:

Source → Bronze → Silver → Gold

By integrating taxi trips, weather conditions, taxi zone metadata, and traffic disruptions, the dataset enables analysis of mobility demand, travel behavior, and operational disruptions across New York City.

---

## Why This Project?

Urban mobility is influenced by many factors, including:

- Weather conditions
- Traffic disruptions
- Geographic location
- Time of day
- Travel demand patterns

However, these data sources are often stored independently and cannot be directly analyzed together.

This project addresses that challenge by building a unified data platform that:

- Consolidates multiple public mobility datasets
- Maintains data quality and consistency
- Supports incremental updates
- Prevents duplicate records
- Produces analytics-ready datasets for business users

The result is a trusted mobility dataset that can help answer questions such as:

### 1. When and where is taxi demand highest?
Identify:

- Peak travel hours
- High-demand weekdays
- Most active taxi zones

### 2. How does weather affect taxi demand?

Analyze relationships between:

- Trip volume
- Trip distance
- Trip duration
- Fare amount

under different weather conditions.

### 3. Which NYC locations show the strongest mobility activity?

Identify areas with:

- High pickup activity
- High drop-off activity
- Longer trip patterns
- Increased transportation demand

### 4. Do road closures impact mobility behavior?

Evaluate whether traffic advisories and road disruptions affect:

- Taxi demand
- Route patterns
- Travel times
- Trip distances

---

# Data Sources

## NYC Green Taxi Trips

Monthly taxi trip records:

- March 2026
- April 2026
- May 2026

Format:

- Parquet

Contains:

- Pickup and dropoff timestamps
- Passenger count
- Trip distance
- Fare information
- Pickup and dropoff locations

---

## Weather Data

Historical weather information retrieved via REST API.

Contains:

- Temperature
- Precipitation
- Weather conditions
- Daily observations

---

## NYC Taxi Zones

Reference dataset for NYC taxi pickup and dropoff locations.

Format:

- CSV

Contains:

- Zone ID
- Borough
- Service Zone
- Zone Name

---

## NYC DOT Traffic Advisories (Bonus)

Traffic and road closure information obtained through web scraping.

Contains:

- Road closure information
- Disruption locations
- Advisory dates

---

# Architecture

```text
                ┌─────────────┐
                │ Source Data │
                └──────┬──────┘
                       │
                       ▼
                ┌─────────────┐
                │   Bronze    │
                │ Raw Ingest  │
                └──────┬──────┘
                       │
                       ▼
                ┌─────────────┐
                │   Silver    │
                │ Clean Data  │
                │ Standardized│
                │ Deduped     │
                └──────┬──────┘
                       │
                       ▼
                ┌─────────────┐
                │    Gold     │
                │ Integrated  │
                │ Analytics   │
                └─────────────┘
