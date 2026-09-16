# Engineering Standards

## Overview

This document defines repository standards, development workflow, code quality tooling, pull request processes, and deployment practices used by the NYC Mobility project.

---

# Repository Architecture

The project follows a source-oriented repository structure.

```text
src/
├── green_taxi/
├── weather/
├── taxi_zones/
└── traffic_advisories/
```

Each source domain owns its Bronze, Silver, and Gold assets.

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
└── traffic_advisories/
    ├── bronze/
    ├── silver/
    └── gold/
```

### Why Source-Oriented?

Work ownership is assigned by source domain.

```text
Green Taxi
Weather
Taxi Zones
Traffic Advisories
```

This structure allows contributors to find ingestion, transformation, profiling, validation, and gold-layer assets for a source system in a single location.

For the current project size and team structure, source-oriented organization provides simpler navigation and stronger ownership boundaries than a layer-oriented approach.

---

# Shared Folder Standards

```text
src/shared/
├── 00_schema_setup.sql
├── 01_bronze_tables.sql
├── 02_silver_tables.sql
├── 03_gold_tables.sql
└── 04_monitoring_tables.sql
```

## Shared Folder Responsibilities

The shared folder contains only database object definitions.

Examples:

```sql
CREATE CATALOG
CREATE SCHEMA
CREATE TABLE
CREATE VIEW
```

### Examples

```text
✅ CREATE TABLE nyc_bronze.taxi_zones
✅ CREATE TABLE nyc_silver.taxi_zones_clean
✅ CREATE TABLE nyc_gold.dim_taxi_zone
```

---

## Domain Folder Responsibilities

Domain folders contain operational pipeline logic.

Examples:

```text
Data ingestion
Data transformation
Profiling queries
Quality checks
Monitoring queries
Business logic
```

### Examples

```text
✅ INSERT INTO
✅ INSERT OVERWRITE
✅ MERGE INTO
✅ Validation queries
✅ Profiling notebooks
```

### Not Allowed

```text
❌ CREATE TABLE statements inside domain folders
❌ CREATE SCHEMA statements inside domain folders
```

Example:

```text
✅ src/shared/01_bronze_tables.sql
    CREATE TABLE taxi_zones

✅ src/taxi_zones/bronze/
    INSERT INTO taxi_zones

❌ src/taxi_zones/bronze/
    CREATE TABLE taxi_zones
```

This separation ensures a single source of truth for schema definitions and simplifies maintenance when schema changes occur.

---

# Pull Request Workflow

All changes must be submitted through Pull Requests.

```text
Feature Branch
    ↓
Pull Request Opened
    ↓
Auto Reviewer Assignment
    ↓
Repository Validation Checks
    ↓
Reviewer Approval
    ↓
Merge to Main
```

---

# GitHub Actions

The repository uses GitHub Actions to automate validation and review processes.

## auto-reviewer.yml

Purpose:

Automatically assigns a reviewer when a Pull Request is opened.

Workflow:

```text
PR Opened
    ↓
auto-reviewer.yml
    ↓
Reviewer Assigned
```

Benefits:

- Ensures every PR receives review ownership
- Removes manual reviewer assignment
- Creates a consistent review process
- Prevents PRs from being overlooked

---

## pr-checks.yml

Purpose:

Validates repository structure and standards before code can be merged.

Workflow:

```text
PR Opened
    ↓
pr-checks.yml
    ↓
Repository Validation
```

Checks may include:

- Required documentation files
- Expected folder structure
- File existence validation
- Workflow validation
- Linting and formatting checks

Benefits:

- Prevents accidental repository drift
- Detects issues before review
- Maintains project standards

---

## Branch Protection

The main branch is protected through repository rules.

Requirements:

```text
✅ Pull Request Required
✅ Status Checks Required
✅ Reviewer Approval Required
✅ No Force Pushes
✅ Branch Deletion Blocked
```

Benefits:

- Improves code quality
- Prevents accidental changes
- Maintains auditability
- Enforces review standards

---

# Code Quality Standards

## SQLFluff

Purpose:

SQL formatting and linting.

Benefits:

- Consistent SQL style
- Easier code reviews
- Improved readability
- Detection of common SQL issues

Standards:

```text
Uppercase keywords
Consistent indentation
Explicit aliases
Databricks SQL dialect
```

---

## Black

Purpose:

Python formatting.

Benefits:

- Consistent style
- Reduced formatting discussions
- Improved code readability

---

## isort

Purpose:

Python import management.

Benefits:

- Consistent import ordering
- Reduced duplicate imports
- Improved dependency visibility

---

## Flake8

Purpose:

Python static analysis.

Benefits:

- Detect undefined variables
- Detect unused imports
- Detect unreachable code
- Improve code quality

---

## nbqa

Purpose:

Apply Python quality tools to notebook code.

Benefits:

- Consistent notebook development standards
- Linting support for notebooks
- Improved notebook maintainability

---

## nbstripout

Purpose:

Remove notebook outputs before commits.

Benefits:

- Smaller repository size
- Cleaner pull requests
- Easier notebook reviews
- Better Git history

---

# Quality Expectations

## SQL Files

Requirements:

```text
Pass SQLFluff validation
Follow Databricks SQL standards
Use consistent formatting
```

---

## Python Files

Requirements:

```text
Pass Black formatting
Pass Flake8 checks
Pass isort validation
```

---

## Notebooks

Requirements:

```text
Outputs removed before commit
No unnecessary metadata
Readable code cells
Version-control friendly format
```

---

# Development Lifecycle

```text
Developer
    ↓
Create Feature Branch
    ↓
Develop & Test Changes
    ↓
Open Pull Request
    ↓
auto-reviewer.yml assigns reviewer
    ↓
pr-checks.yml validates repository
    ↓
Reviewer Approval
    ↓
Merge to Main
    ↓
Production Deployment
```

This workflow ensures all repository changes follow consistent engineering, documentation, testing, and review standards.