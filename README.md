# NYC Mobility Data Pipeline

## Overview

This project builds a scalable and repeatable data engineering pipeline that combines multiple NYC public datasets into a trusted mobility analytics dataset.

## Repository layout

The source tree is domain-first and keeps Bronze, Silver, and Gold execution
order in each domain:

```text
src/
├── green_taxi/{bronze,silver,gold}/
├── weather/{bronze,silver,gold}/
├── taxi_zones/{bronze,silver,gold}/
├── traffic_advisories/{bronze,silver,gold}/
└── shared/{setup,monitoring}/
tests/
docs/
resources/
databricks.yml
```

`src/shared/setup` contains the ordered table setup SQL. `tests` contains the
indexed QC scripts, `docs` contains all Markdown documentation, and
`07_dashboard` retains the dashboard placeholders. The Databricks Asset Bundle
is a deployment scaffold only; configure the `cluster_id` variable and
environment-specific task contracts before deploying.

The pipeline follows the Medallion Architecture pattern:

Source â†’ Bronze â†’ Silver â†’ Gold

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
                â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
                â”‚ Source Data â”‚
                â””â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”˜
                       â”‚
                       â–¼
                â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
                â”‚   Bronze    â”‚
                â”‚ Raw Ingest  â”‚
                â””â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”˜
                       â”‚
                       â–¼
                â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
                â”‚   Silver    â”‚
                â”‚ Clean Data  â”‚
                â”‚ Standardizedâ”‚
                â”‚ Deduped     â”‚
                â””â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”˜
                       â”‚
                       â–¼
                â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
                â”‚    Gold     â”‚
                â”‚ Integrated  â”‚
                â”‚ Analytics   â”‚
                â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
