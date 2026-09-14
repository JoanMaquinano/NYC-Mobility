-- Create the nyc-mobility catalog
CREATE CATALOG IF NOT EXISTS `nyc-mobility`;

-- Create Bronze, Silver, Gold, and Quality  Schemas for NYC Mobility
CREATE SCHEMA IF NOT EXISTS `nyc-mobility`.nyc_bronze;

CREATE SCHEMA IF NOT EXISTS `nyc-mobility`.nyc_silver;

CREATE SCHEMA IF NOT EXISTS `nyc-mobility`.nyc_gold;

CREATE SCHEMA IF NOT EXISTS `nyc-mobility`.nyc_quality;

SHOW SCHEMAS IN `nyc-mobility`;