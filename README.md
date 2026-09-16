# NYC Mobility Data Pipeline

## Overview

This project builds a scalable and repeatable data engineering pipeline that combines multiple NYC public datasets into a trusted mobility analytics dataset.

The pipeline integrates taxi trips, weather conditions, taxi zone metadata, and traffic disruptions into a unified mobility platform for analytical reporting and business insights.

---

# Repository Layout

The source tree is domain-first and keeps Bronze, Silver, and Gold execution order within each domain.

```text
src/
├── green_taxi/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── weather/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── taxi_zones/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── traffic_advisories/
│   ├── bronze/
│   ├── silver/
│   └── gold/
└── shared/
    ├── 00_schema_setup.sql
    ├── 01_bronze_tables.sql
    ├── 02_silver_tables.sql
    ├── 03_gold_tables.sql
    └── 04_monitoring_tables.sql

tests/
docs/
resources/
databricks.yml
```

The project follows a source-oriented repository structure where each source domain owns its Bronze, Silver, and Gold assets. This approach simplifies ownership, navigation, and collaboration by keeping all assets related to a dataset in a single location.

The `src/shared` directory contains centralized schema and table creation scripts.

```text
shared/
    = CREATE SCHEMA
    = CREATE TABLE

domain folders/
    = ingestion
    = transformation
    = profiling
    = validation
```

---

# Architecture

The pipeline follows the Medallion Architecture pattern.

```text
Source Data
    ↓
Bronze
    - Raw ingestion
    - Audit fields
    - Minimal transformations
    ↓
Silver
    - Data cleaning
    - Standardization
    - Deduplication
    - Quality enforcement
    ↓
Gold
    - Fact tables
    - Dimension tables
    - Analytics-ready datasets
```

---

# Why This Project?

Urban mobility is influenced by many factors, including:

- Weather conditions
- Traffic disruptions
- Geographic location
- Time of day
- Travel demand patterns

However, these datasets are typically stored independently and cannot easily be analyzed together.

This project addresses that challenge by building a unified data platform that:

- Consolidates multiple public mobility datasets
- Maintains data quality and consistency
- Supports repeatable execution
- Prevents duplicate records
- Produces analytics-ready datasets

The result is a trusted mobility dataset that can help answer questions such as:

### When and where is taxi demand highest?

- Peak travel hours
- High-demand weekdays
- Most active taxi zones

### How does weather affect taxi demand?

Analyze relationships between:

- Trip volume
- Trip distance
- Trip duration
- Fare amount

under different weather conditions.

### Which NYC locations show the strongest mobility activity?

Identify areas with:

- High pickup activity
- High drop-off activity
- Longer trip patterns
- Increased transportation demand

### Do road closures impact mobility behavior?

Evaluate whether traffic advisories and road disruptions affect:

- Taxi demand
- Route patterns
- Travel times
- Trip distances

---

# Data Sources

## NYC Green Taxi Trips

Monthly taxi trip records provided by NYC TLC.

### Source Details

- Source: NYC TLC Trip Record Data
- Format: Parquet
- Acquisition Method: File Ingestion
- Refresh Frequency: Monthly

### Contains

- Pickup and dropoff timestamps
- Passenger count
- Trip distance
- Fare information
- Pickup and dropoff locations

Source:

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

---

## Open-Meteo Historical Weather

Historical weather observations used to enrich mobility analytics and evaluate weather impacts on transportation demand.

### Source Details

- Source: Open-Meteo Historical Weather API
- Format: JSON (REST API)
- Acquisition Method: API Extraction

### Contains

- Temperature
- Precipitation
- Weather conditions
- Daily observations

### Assumptions

- Weather codes are sourced from Open-Meteo documentation.
- Weather interpretation follows WMO Weather Interpretation Codes (WW).

Source:

https://open-meteo.com/en/docs/historical-weather-api

Reference:

https://open-meteo.com/en/docs/historical-weather-api#weather_variable_documentation

---

## NYC Taxi Zones

Reference dataset used to map trip records to geographic taxi zones and boroughs.

### Source Details

- Source: NYC Taxi Zone Lookup
- Format: CSV
- Acquisition Method: File Ingestion
- Data Type: Reference Data

### Contains

- Zone ID
- Borough
- Service Zone
- Zone Name

Source:

https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv

---

## NYC DOT Traffic Advisories

Traffic disruption data obtained through web scraping.

### Source Details

- Source: NYC DOT Weekly Traffic Advisory
- Format: HTML
- Acquisition Method: Web Scraping
- Refresh Frequency: Weekly

### Contains

- Road closure information
- Disruption locations
- Advisory dates

Source:

https://www.nyc.gov/html/dot/html/motorist/weektraf.shtml

---

# Data Ingestion Strategy

The pipeline supports repeatable execution across all source systems.

| Source | Arrival Method | Change Detection | Repeatability |
|----------|----------|----------|----------|
| Green Taxi | NYC TLC monthly files | File-based monthly loads | Parameterized execution |
| Weather | Open-Meteo API | Date-range extraction | Same API requests can be rerun |
| Taxi Zones | CSV lookup file | Full refresh | Reference data can be reloaded |
| Traffic Advisories | Web scraping | Full source scrape | Scraping process can be rerun |

---

# Key Assumptions

## Green Taxi

- Initial project scope focuses on a limited monthly sample.
- Bronze ingestion is parameterized for repeatable execution.

## Weather

- Open-Meteo weather codes are treated as authoritative.
- Historical weather observations are considered the source of truth.

## Traffic Advisories

- Advisory records represent roadway conditions published for the specified period.

## General

- Public source systems remain accessible during execution.
- Source schemas remain stable during the project lifecycle.

---

# Documentation

Detailed documentation is maintained in the `docs/` directory.

| Document | Purpose |
|----------|----------|
| 01_business_rules.md | Business assumptions and transformation rules |
| 02_grain_definitions.md | Fact and dimension grain definitions |
| 03_table_specs.md | Table specifications and data dictionary |
| 04_data_model.md | ERD, snowflake schema, and modeling decisions |
| 05_data_quality.md | Data quality framework, validation rules, and severity levels |
| 06_engineering_standards.md | Repository standards, GitHub Actions, and development workflow |

---

# Development Workflow

All changes follow a pull request workflow.

```text
Feature Branch
    ↓
Pull Request Opened
    ↓
auto-reviewer.yml
    ↓
Reviewer Assigned
    ↓
pr-checks.yml
    ↓
Repository Validation
    ↓
Reviewer Approval
    ↓
Merge to Main
```

Repository automation includes:

- Automatic reviewer assignment
- Repository validation checks
- Branch protection rules
- Standardized code review process

---

# Technology Stack

## Data Processing

- Databricks
- Delta Lake
- SQL
- PySpark

## Data Sources

- NYC TLC Trip Records
- Open-Meteo Historical Weather API
- NYC Taxi Zone Lookup
- NYC DOT Traffic Advisories

## Development & Quality

- GitHub
- GitHub Actions
- SQLFluff
- Black
- Flake8
- isort
- nbqa
- nbstripout

---

# Project Status

The current project scope focuses on validating the end-to-end architecture using a limited initial dataset before expanding to additional periods and operational enhancements.

Current implementation includes:

- Multi-source data ingestion
- Bronze, Silver, and Gold Medallion layers
- Automated validation workflows
- Data quality monitoring
- Analytics-ready dimensional models
- GitHub-based collaboration and review workflows