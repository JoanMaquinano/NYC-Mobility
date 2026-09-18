# Business Rules

## 1. Purpose

The NYC Mobility project integrates NYC Green Taxi trip records, weather data, and taxi zone reference data into a trusted, analytics-ready dataset.

The primary business objectives are to:

- Identify when and where taxi demand is highest.
- Understand how weather impacts mobility demand and trip behavior.
- Compare mobility activity across taxi zones and boroughs.
- Provide reliable daily and zone-level KPIs for analytics and reporting.

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
- Weather data represents a single NYC weather location.
- Weather is associated with taxi trips using the trip date.
- Weather joins occur at the date grain rather than location grain.
- Weather attributes include temperature, precipitation probability, rain, cloud cover, visibility, wind speed, and weather codes.
- Latitude and longitude are excluded from analytics tables because they are constant for the selected NYC weather location and provide no analytical value.

### NYC Taxi Zones

- Taxi Zone Lookup is the authoritative geographic reference dataset.
- LocationID is treated as the business key for taxi zones.
- Zone names and borough information are derived from Taxi Zone Lookup data.
- Taxi zones provide the geographic dimension for pickup and dropoff analysis.

---

## 3. Data Standardization Rules

- Source column names are standardized using snake_case naming conventions.
- Date and timestamp fields are converted to consistent formats.
- Text values are trimmed to remove leading and trailing spaces.
- Numeric measures are stored using appropriate numeric data types.
- Source system identifiers are retained for traceability.
- Null values are preserved when a reliable business replacement value does not exist.

---

## 4. Key Rules

- Business keys from source systems are retained where available.
- LocationID is the business key for taxi zones.
- Weather records use date as the business key.
- Weather keys are generated from weather dates for analytical joins.
- Fact records reference dimension records through generated keys where applicable.

---

## 5. Derived Field Rules

- Trip duration is calculated using pickup and dropoff timestamps.
- Date attributes may be derived from trip timestamps.
- Hour, day, week, month, and year attributes may be derived for analytical purposes.
- Pickup and dropoff zone descriptions are derived from Taxi Zone Lookup data.
- Borough information is derived from Taxi Zone data.
- Weather context is added through joins with weather observations.
- Weather keys are generated from weather dates during transformation processing.

---

## 6. Assumptions

- NYC TLC trip records are treated as the authoritative source for taxi trip activity.
- Taxi Zone Lookup data is treated as the authoritative geographic reference.
- Open-Meteo weather observations reasonably represent weather conditions affecting mobility demand across NYC.
- Weather data represents a single city-wide weather source and not localized weather conditions per taxi zone.
- Missing values may exist in source datasets and are not always considered data quality defects.
- Incremental loads should produce the same analytical results as historical full loads.
- Reprocessing the same source data should not introduce duplicate business records.
- All recurring ingestion processes should be idempotent and safe to rerun.

---

## 7. Known Limitations

- The project includes Green Taxi data only and does not include Yellow Taxi trips.
- Weather observations may not fully capture localized micro-weather conditions across NYC.
- Weather data represents a single NYC location rather than weather observations per borough or taxi zone.
- Source systems may contain reporting delays, corrections, or missing values.
- Results should be interpreted as analytical insights rather than operational forecasts.

---

## 8. Business Metrics / KPIs

The primary metrics supported by the platform include:

- Trip Count
- Passenger Count
- Total Revenue
- Total Fare
- Average Fare
- Total Distance
- Average Distance
- Average Temperature
- Rain
- Precipitation Probability
- Borough-Level Zone Ranking