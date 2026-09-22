-- Create the nyc_mobility catalog
CREATE CATALOG IF NOT EXISTS nyc_mobility;

-- Create Bronze, Silver, Gold, and Quality  Schemas for NYC Mobility
CREATE SCHEMA IF NOT EXISTS nyc_mobility.nyc_bronze;

CREATE SCHEMA IF NOT EXISTS nyc_mobility.nyc_silver;

CREATE SCHEMA IF NOT EXISTS nyc_mobility.nyc_gold;

CREATE SCHEMA IF NOT EXISTS nyc_mobility.nyc_quality;

SHOW SCHEMAS IN nyc_mobility;