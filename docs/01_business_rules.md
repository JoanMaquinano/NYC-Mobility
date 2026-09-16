# Business Rules

## 1. Purpose

The NYC Mobility project integrates NYC Green Taxi trip records, weather data, taxi zone reference data, and traffic advisory information into a trusted, analytics-ready dataset.

The primary business objectives are to:

- Identify when and where taxi demand is highest.
- Understand how weather impacts mobility demand and trip behavior.
- Identify areas with strong mobility activity and patterns.
- Analyze whether traffic disruptions affect taxi activity and travel behavior.

---

## 2. Source-Specific Business Rules

### NYC Green Taxi

- One taxi trip represents a single mobility event.
- One taxi trip corresponds to one fact record.
- Pickup datetime represents the beginning of a trip.
- Dropoff datetime represents the end of a trip.
- Pickup and dropoff locations are identified using TLC Taxi Zone Location IDs.
- Pickup location represents the trip origin.
- Dropoff location represents the trip destination.
- Trip demand is measured using trip count.
- Revenue analysis is based on fare-related fields such as fare amount, tip amount, tolls amount, and total amount.
- Trip distance is measured in miles.

### Open-Meteo Historical Weather

- Weather data provides environmental context for mobility analysis.
- Weather observations are not mobility events and are used only as enrichment data.
- Weather attributes may include temperature, precipitation, wind speed, and weather condition indicators.
- Weather data is linked to taxi activity through a shared date or datetime grain.

### NYC Taxi Zones

- Taxi Zone Lookup is the authoritative geographic reference dataset.
- LocationID is treated as the business key for taxi zones.
- Zone names and borough information are derived from Taxi Zone Lookup data.
- Taxi zones provide the geographic dimension for pickup and dropoff analysis.

### NYC DOT Traffic Advisories

- Traffic advisories represent planned road closures, restrictions, or disruptions.
- Traffic advisories are treated as contextual events rather than mobility transactions.
- Traffic advisories may be analyzed alongside trip activity to understand possible disruption impacts.
- Traffic advisories do not modify source taxi trip records.

---

## 3. Data Standardization Rules

- Source column names are standardized using snake_case naming conventions.
- Date and timestamp fields are converted to a consistent datetime format.
- Text values are trimmed to remove leading and trailing spaces.
- Numeric measures are stored using appropriate numeric data types.
- Source system identifiers are retained for traceability.
- Null values are preserved when a reliable business replacement value does not exist.

---

## 4. Surrogate Key Rules

- Dimension tables use surrogate keys where appropriate.
- Source business keys are retained alongside surrogate keys.
- Fact tables reference dimensions using surrogate keys.
- Surrogate keys are system-generated and independent of source-system identifiers.

---

## 5. Derived Field Rules

- Trip duration is calculated using pickup and dropoff timestamps.
- Date-related attributes may be derived from trip timestamps.
- Hour, day, week, month, and year attributes may be derived for analytical purposes.
- Pickup and dropoff zone descriptions are derived from Taxi Zone Lookup data.
- Borough information is derived from Taxi Zone data.
- Weather context is added through joins with weather observations.

---

## 6. Assumptions

- NYC TLC trip records are treated as the authoritative source for taxi trip activity.
- Taxi Zone Lookup data is treated as the authoritative geographic reference.
- Open-Meteo weather observations reasonably represent weather conditions affecting mobility demand.
- NYC DOT advisory information represents planned disruptions available at publication time.
- Missing values may exist in source datasets and are not always considered data quality defects.
- Incremental loads should produce the same analytical results as historical full loads.
- Reprocessing the same source data should not introduce duplicate business records.
Largest state in NYC to generalize
Weather codes from Open-Meteo to check weather
Traffic assumptions


---

## 7. Known Limitations

- The project includes Green Taxi data only and does not include Yellow Taxi trips.
- Weather observations may not fully capture localized micro-weather conditions across NYC.
- Traffic advisory coverage depends on information published by NYC DOT.
- Source systems may contain reporting delays, corrections, or missing values.
- Results should be interpreted as analytical insights rather than operational forecasts.

---

# Notes / To Be Confirmed

## Weather Grain

Confirm:

- One weather record per hour
- OR one weather record per day

This determines how weather data is joined to taxi activity.

---

## Traffic Advisory Grain

Confirm:

- One row per advisory
- OR one row per affected location
- OR one row per advisory date

---

## Final Gold Tables

Confirm actual Gold-layer tables.

Example:

- fact_green_taxi_trip
- dim_weather
- dim_taxi_zone
- dim_date
- fact_traffic_advisory

---

## Surrogate Key Implementation

Confirm:

- Which dimensions use surrogate keys
- Key generation strategy

---

## Business Metrics / KPIs

Confirm final metrics used for analysis.

Potential metrics:

- Trip Count
- Average Fare
- Average Trip Distance
- Average Trip Duration
- Pickup Volume
- Dropoff Volume
- Revenue Metrics
- Weather Impact Metrics
- Traffic Advisory Impact Metrics

---

## Weather Join Logic

Confirm whether weather data is joined using:

- Trip Date
- Trip Hour
- Pickup Datetime
- Another business rule

---

## Traffic Advisory Integration Logic

Confirm whether traffic advisories are:

- Used for contextual analysis only
- Joined directly to trips
- Aggregated by date
- Aggregated by zone