CREATE CATALOG IF NOT EXISTS taxi_dwh;

CREATE SCHEMA IF NOT EXISTS taxi_dwh.bronze;
CREATE SCHEMA IF NOT EXISTS taxi_dwh.silver;
CREATE SCHEMA IF NOT EXISTS taxi_dwh.gold;

COMMENT ON SCHEMA taxi_dwh.bronze IS 'Raw taxi trip ingestion layer';
COMMENT ON SCHEMA taxi_dwh.silver IS 'Cleaned and conformed taxi trips';
COMMENT ON SCHEMA taxi_dwh.gold IS 'Analytics-ready taxi marts';
