# Data Model

## 1. Overview

The NYC Mobility data model integrates multiple public datasets into a trusted, analytics-ready dataset using a Medallion Architecture approach.

Data Sources:

- NYC Green Taxi Trip Records
- Open-Meteo Historical Weather API
- NYC Taxi Zone Lookup
- NYC DOT Traffic Advisories (Bonus)

The model supports analysis of mobility demand, trip behavior, weather conditions, and traffic disruptions across New York City.

---

## 2. Modeling Approach

The project follows a dimensional modeling approach.

Data progresses through:

```text
Sources
    ↓
Bronze (Raw)
    ↓
Silver (Cleaned & Standardized)
    ↓
Gold (Integrated Analytics Layer)
```

The Gold layer consists of:

- Fact tables for measurable business events
- Dimension tables for descriptive business attributes

The model uses a snowflake-inspired design where shared reference data is maintained separately and linked through keys.

---

## 3. ERD

### ERD Diagram

> Insert ERD image here.

```text
[ERD IMAGE PLACEHOLDER]
```

### Relationship Diagram

```text
                    dim_date
                        |
                        |
                        ▼

dim_taxi_zone ---> fact_green_taxi_trip <--- dim_weather
       ▲                     |
       |                     |
       |                     ▼

       +------ fact_traffic_advisory (optional)
```

> Update diagram after final implementation.

---

## 4. Fact Tables

### fact_green_taxi_trip

Purpose:

Stores taxi trip transactions and mobility activity.

Business Event:

```text
A completed Green Taxi trip.
```

Measures may include:

- Trip count
- Trip distance
- Fare amount
- Tip amount
- Total amount
- Trip duration

---

### fact_traffic_advisory *(Optional)*

Purpose:

Stores traffic disruption events for mobility context.

Business Event:

```text
A published traffic advisory.
```

Potential measures:

- Advisory count
- Number of affected locations
- Duration of advisory

---

## 5. Dimension Tables

### dim_taxi_zone

Purpose:

Provides geographic reference information.

Examples:

- Zone
- Borough

Used for:

- Pickup analysis
- Dropoff analysis
- Geographic reporting

---

### dim_weather

Purpose:

Provides weather observations used for mobility enrichment.

Examples:

- Temperature
- Precipitation
- Wind speed
- Weather code

Used for:

- Weather impact analysis
- Trend analysis

---

### dim_date

Purpose:

Provides calendar attributes for reporting.

Examples:

- Day
- Week
- Month
- Quarter
- Year

Used for:

- Time-series reporting
- Aggregation

---

### dim_time *(Optional)*

Purpose:

Provides hour-level reporting attributes.

Examples:

- Hour of day
- Time period

Used for:

- Peak-hour analysis
- Mobility activity analysis

---

## 6. Table Grains

### fact_green_taxi_trip

Grain:

```text
One row per taxi trip
```

---

### fact_traffic_advisory

Proposed Grain:

```text
One row per traffic advisory
```

To be confirmed.

---

### dim_taxi_zone

Grain:

```text
One row per LocationID
```

---

### dim_weather

Proposed Grain:

```text
One row per weather observation
```

Possible implementations:

```text
One row per hour
```

or

```text
One row per day
```

To be confirmed.

---

### dim_date

Grain:

```text
One row per calendar date
```

---

### dim_time

Grain:

```text
One row per hour
```

If implemented.

---

## 7. Relationships

### fact_green_taxi_trip → dim_taxi_zone

Relationship:

```text
Many-to-One
```

Business Rule:

```text
Many trips can originate from the same taxi zone.
Many trips can end in the same taxi zone.
```

---

### fact_green_taxi_trip → dim_weather

Relationship:

```text
Many-to-One
```

Business Rule:

```text
Many trips may share the same weather observation.
```

---

### fact_green_taxi_trip → dim_date

Relationship:

```text
Many-to-One
```

Business Rule:

```text
Many trips may occur on the same calendar date.
```

---

### fact_traffic_advisory → dim_date

Relationship:

```text
Many-to-One
```

Business Rule:

```text
Multiple advisories may occur on the same date.
```

---

## 8. Design Decisions

### Dimensional Modeling

The project uses a dimensional model to support analytical reporting and aggregation.

Benefits:

- Simplified reporting
- Consistent business definitions
- Reusable dimensions

---

### Geographic Reference Data

Taxi Zones are separated into a dedicated dimension table.

Benefits:

- Prevents repeated storage of zone information
- Ensures consistent geographic reporting
- Improves maintainability

---

### Weather Enrichment

Weather data is modeled separately and linked to mobility activity through shared date or datetime attributes.

Benefits:

- Reduces duplication
- Simplifies weather-related analysis
- Supports weather impact reporting

---

### Incremental & Idempotent Design

The pipeline is designed to support:

- Incremental file ingestion
- Repeated processing without duplicate results
- Data quality validation

---

## 9. Trade-offs

### Weather Granularity

Trade-off:

```text
Daily weather is simpler
Hourly weather is more detailed
```

Consideration:

```text
Higher granularity increases analytical flexibility but also increases data volume and join complexity.
```

---

### Traffic Advisory Integration

Trade-off:

```text
Keeping advisories separate reduces complexity.
Joining advisories directly to trips increases analytical detail.
```

Consideration:

```text
Additional business rules may be required to avoid many-to-many relationships.
```

---

### Snowflake vs Star Schema

Trade-off:

```text
Snowflake:
Less duplication
More joins

Star:
Simpler querying
More duplication
```

Consideration:

```text
The project prioritizes maintainability and consistency of reference data.
```

---

# Notes / To Be Confirmed

## Final Gold Tables

Confirm implemented Gold tables:

- fact_green_taxi_trip
- fact_traffic_advisory
- dim_taxi_zone
- dim_weather
- dim_date
- dim_time

---

## Final ERD

Replace placeholder diagram with actual ERD image generated from the implemented model.

---

## Weather Grain

Confirm whether weather data is stored as:

```text
One row per hour
```

or

```text
One row per day
```

---

## Weather Join Strategy

Confirm whether weather joins occur on:

```text
Trip Date
Trip Hour
Pickup Datetime
Dropoff Datetime
```

---

## Traffic Advisory Grain

Confirm whether advisory records represent:

```text
One advisory
One advisory per date
One advisory per affected location
```

---

## Traffic Advisory Integration

Confirm whether advisories:

- Remain standalone facts
- Are joined directly to trips
- Are aggregated by date
- Are aggregated by zone

---

## Surrogate Keys

Confirm dimensions that use surrogate keys and document relationship mappings.

---

## Final Cardinality Validation

Validate all table relationships after implementation to ensure no many-to-many joins are introduced unintentionally.