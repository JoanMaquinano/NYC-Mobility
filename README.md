# NYC Mobility Data Pipeline

## 📌 Overview
This project builds a scalable and repeatable data engineering pipeline that combines multiple NYC public datasets into a trusted mobility analytics dataset.

The pipeline integrates **taxi trips**, **weather conditions**, and **taxi zone metadata** into a unified mobility platform for analytical reporting and business insights.

---

## 📂 Repository Layout
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

- Each **domain folder** owns its Bronze, Silver, and Gold assets.  
- The `src/shared` directory contains centralized schema and table creation scripts.  

---

## 🏛 Architecture
The pipeline follows the **Medallion Architecture** pattern:

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

## 🎯 Why This Project?
Urban mobility is influenced by many factors, including:

- Weather conditions  
- Geographic location  
- Time of day  
- Travel demand patterns  

These datasets are typically siloed and cannot easily be analyzed together.  
This project solves that by building a **unified data platform** that:

- Consolidates multiple public mobility datasets  
- Maintains data quality and consistency  
- Supports repeatable execution  
- Prevents duplicate records  
- Produces analytics-ready datasets  

### Example Questions Answered
- **When and where is taxi demand highest?**  
- **How does weather affect taxi demand?**  
- **Which NYC locations show the strongest mobility activity?**

---

## 📊 Data Sources

### NYC Green Taxi Trips
- **Source:** NYC TLC Trip Record Data  
- **Format:** Parquet  
- **Acquisition:** File ingestion  
- **Refresh:** Monthly  
- **Contains:** Pickup/dropoff timestamps, passenger count, trip distance, fare info, pickup/dropoff zones  
- Source Link [(nyc.gov in Bing)](https://www.bing.com/search?q="https%3A%2F%2Fwww.nyc.gov%2Fsite%2Ftlc%2Fabout%2Ftlc-trip-record-data.page")

---

### Open-Meteo Historical Weather
- **Source:** Open-Meteo Historical Weather API  
- **Format:** JSON (REST API)  
- **Acquisition:** API extraction  
- **Contains:** Temperature, precipitation, weather conditions, daily observations  
- **Notes:** Weather codes follow WMO standards  
- [Source Link](https://open-meteo.com/en/docs/historical-weather-api)

---

### NYC Taxi Zones
- **Source:** NYC Taxi Zone Lookup  
- **Format:** CSV  
- **Acquisition:** File ingestion  
- **Contains:** Zone ID, Borough, Service Zone, Zone Name  
- Source Link [(s3.amazonaws.com in Bing)](https://www.bing.com/search?q="https%3A%2F%2Fs3.amazonaws.com%2Fnyc-tlc%2Fmisc%2Ftaxi%2B_zone_lookup.csv")

---

## 🔄 Data Ingestion Strategy

| Source      | Arrival Method       | Change Detection       | Repeatability              |
|-------------|----------------------|------------------------|----------------------------|
| Green Taxi  | Monthly TLC files    | File-based monthly load | Parameterized execution    |
| Weather     | Open-Meteo API       | Date-range extraction   | Same API requests rerun    |
| Taxi Zones  | CSV lookup file      | Full refresh            | Reference data reloadable  |

---

## 📑 Documentation
Detailed documentation lives in the `docs/` directory:

| Document | Purpose |
|----------|---------|
| 01_business_rules.md | Business assumptions & transformation rules |
| 02_grain_definitions.md | Fact & dimension grain definitions |
| 03_table_specs.md | Table specifications & data dictionary |
| 04_data_model.md | ERD, schema design, modeling decisions |
| 05_data_quality.md | Data quality framework & validation rules |
| 06_engineering_standards.md | Repo standards, GitHub Actions, dev workflow |

---

## ⚙️ Development Workflow
All changes follow a **pull request workflow**:

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

Automation includes:
- Automatic reviewer assignment  
- Repository validation checks  
- Branch protection rules  
- Standardized code review process  

---

## 🛠 Technology Stack

### Data Processing
- Databricks  
- Delta Lake  
- SQL  
- PySpark  

### Data Sources
- NYC TLC Trip Records  
- Open-Meteo Historical Weather API  
- NYC Taxi Zone Lookup  

### Development & Quality
- GitHub  
- GitHub Actions  
- SQLFluff  
- Black  
- Flake8  
- isort  
- nbqa  
- nbstripout  

---

## 📌 Project Status
Current scope focuses on **validating the end-to-end architecture** using a limited initial dataset before expanding further.

✅ Implemented:
- Multi-source data ingestion  
- Bronze, Silver, Gold Medallion layers  
- Automated validation workflows  
- Data quality monitoring  
- Analytics-ready dimensional models  
- GitHub-based collaboration and review workflows  
```