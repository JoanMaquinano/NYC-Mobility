# Engineering Standards

## Overview

This document defines repository standards, development workflow, code quality practices, pull request processes, and deployment conventions used by the NYC Mobility project.

---

# Architecture Standards

## Repository Structure

The project uses a **source-oriented repository structure**.

```text
src/
├── green_taxi/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── dq/
├── weather/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── dq/
└── taxi_zones/
    ├── bronze/
    ├── silver/
    ├── gold/
    └── dq/
```

Each source domain owns its:

- Ingestion logic
- Transformation logic
- Data quality checks
- Business logic
- Documentation

### Why Source-Oriented?

Ownership is assigned by source domain:

```text
Green Taxi
Weather
Taxi Zones
```

Benefits:

- Simpler navigation
- Clear ownership boundaries
- Easier development and troubleshooting
- Better scalability for multi-source projects

---

## Databricks Architecture

The Databricks catalog follows a **layer-oriented Medallion Architecture**.

```text
nyc_bronze
│
├── green_taxi
├── taxi_zones
└── weather

nyc_silver
│
├── green_taxi_clean
├── taxi_zones_clean
└── weather_clean

nyc_gold
│
├── gold_trip_analytics
├── gold_weather_impact
├── gold_zone_performance
└── gold_daily_kpis
```

Benefits:

- Clear separation of data maturity levels
- Easier governance
- Improved data lineage
- Consistent Medallion implementation

---

# Database Object Standards

## Shared Folder Structure

```text
src/shared/
├── 00_catalog_setup.sql
├── 01_bronze_tables.sql
├── 02_silver_tables.sql
├── 03_gold_tables.sql
└── 04_monitoring_tables.sql
```

---

## Shared Folder Responsibilities

The shared folder contains only database object definitions.

Examples:

```sql
CREATE CATALOG
CREATE SCHEMA
CREATE TABLE
CREATE VIEW
```

Examples:

```text
✅ CREATE TABLE nyc_bronze.green_taxi
✅ CREATE TABLE nyc_bronze.weather
✅ CREATE TABLE nyc_silver.weather_clean
✅ CREATE TABLE nyc_gold.gold_trip_analytics
```

---

## Domain Folder Responsibilities

Domain folders contain operational processing logic.

Examples:

```text
Data ingestion
Data transformation
MERGE logic
Validation queries
Profiling queries
Monitoring queries
Business logic
```

Allowed:

```text
✅ INSERT INTO
✅ MERGE INTO
✅ Transformation logic
✅ Validation queries
✅ Data quality checks
```

Not Allowed:

```text
❌ CREATE TABLE statements
❌ CREATE SCHEMA statements
❌ CREATE CATALOG statements
```

Example:

```text
✅ src/shared/01_bronze_tables.sql
    CREATE TABLE weather

✅ src/weather/bronze/
    MERGE INTO weather

❌ src/weather/bronze/
    CREATE TABLE weather
```

---

# Separation of Concerns

## Table Creation

Database object creation must be isolated from recurring pipeline execution.

Examples:

```text
CREATE TABLE
CREATE VIEW
CREATE SCHEMA
```

belong in:

```text
src/shared/
```

---

## Ingestion Scripts

Recurring ingestion scripts should contain only:

```text
INSERT
MERGE
Transformations
Validation Logic
```

Benefits:

- Cleaner deployments
- Easier schema management
- Reduced merge conflicts
- Improved maintainability
- Better idempotency

---

# Data Loading Standards

## Idempotent Processing

Recurring jobs must be safe to rerun.

Requirements:

```text
Use MERGE where duplicate loads are possible.
Prevent duplicate business records.
Support recovery from failed executions.
```

Examples:

```text
✅ Weather ingestion uses MERGE ON date

✅ Taxi zone ingestion uses MERGE ON location_id

❌ Repeated INSERT INTO reference tables
```

---

## Weather Loading Standard

Weather data is loaded through two separate processes.

Workflow:

```text
06_ingest_weather_api.ipynb
          ↓
      API → CSV
          ↓
07_load_weather_bronze.ipynb
          ↓
      CSV → bronze_weather
```

Benefits:

- Clear responsibility separation
- Easier troubleshooting
- Independent reruns

---

## Catalog Standards

Use:

```text
nyc_mobility
```

Examples:

```sql
nyc_mobility.nyc_bronze.weather
nyc_mobility.nyc_silver.weather_clean
nyc_mobility.nyc_gold.gold_trip_analytics
```

Avoid:

```text
workspace.default...
```

Reason:

```text
Workspace paths are user-specific and not portable across environments.
```

---

# Workflow Standards

## Pipeline Workflow

```text
Preload_Checks
├── Green_Taxi_Bronze
├── Taxi_Zones_Bronze
├── Weather_API_Ingest
│   └── Weather_Bronze_Load
└── Bronze_QC
```

Requirements:

- Dependencies must be explicit.
- Validation should run after ingestion.
- Failed upstream tasks should block downstream execution.

---

# Pull Request Workflow

All changes must be submitted through Pull Requests.

```text
Feature Branch
    ↓
Pull Request
    ↓
Automated Checks
    ↓
Reviewer Assignment
    ↓
Approval
    ↓
Merge to Main
```

---

## Pull Request Reviews

Requirements:

```text
✅ Pull Request required
✅ Reviewer assigned
✅ Validation checks pass
✅ Approval received before merge
```

Reviewers should verify:

- Business logic correctness
- Catalog standards
- File path standards
- Documentation updates
- DQ compliance

---

# GitHub Actions

## auto-reviewer.yml

Purpose:

Automatically assigns reviewers when Pull Requests are opened.

Workflow:

```text
PR Opened
    ↓
auto-reviewer.yml
    ↓
Reviewer Assigned
```

Benefits:

- Prevents unreviewed PRs
- Ensures ownership
- Supports consistent review practices

---

## pr-checks.yml

Purpose:

Validates repository standards before merge.

Checks may include:

- Folder structure validation
- Documentation validation
- Workflow validation
- Linting checks
- Required file checks

Benefits:

- Detects issues early
- Prevents repository drift
- Maintains consistency

---

# Branch Protection

The main branch is protected.

Requirements:

```text
✅ Pull Request Required
✅ Status Checks Required
✅ Reviewer Approval Required
✅ No Force Pushes
✅ Protected Main Branch
```

Benefits:

- Improved code quality
- Better auditability
- Safer deployments

---

# Code Quality Standards

## SQL Standards

Requirements:

```text
Uppercase SQL keywords
Consistent formatting
Readable aliases
Databricks SQL compatibility
```

Recommended Tool:

```text
SQLFluff
```

---

## Python Standards

Requirements:

```text
Consistent formatting
Meaningful variable names
Reusable functions
Readable code
```

Recommended Tools:

```text
Black
Flake8
isort
```

---

## Notebook Standards

Requirements:

```text
Outputs removed before commit
Minimal metadata
Readable code cells
Version-control-friendly structure
```

Recommended Tools:

```text
nbqa
nbstripout
```

---

# Development Lifecycle

```text
Developer
    ↓
Create Feature Branch
    ↓
Develop & Test
    ↓
Open Pull Request
    ↓
GitHub Validation Checks
    ↓
Reviewer Approval
    ↓
Merge to Main
    ↓
Deploy / Execute Workflow
```

---

# Project Standards

- Repository uses a source-oriented structure.
- Databricks uses a layer-oriented Medallion structure.
- CREATE TABLE statements are stored separately from ingestion processes.
- Weather ingestion is split into API extraction and Bronze loading.
- Weather and taxi zone pipelines use MERGE for idempotency.
- Shared catalog standard is nyc_mobility.
- All files follow the snake case naming convention.
- Pull Requests are required for all repository changes.
- GitHub Actions automate reviewer assignment and repository validation.
- Data quality validation must execute before publishing Gold-layer outputs.