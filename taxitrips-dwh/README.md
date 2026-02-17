# Taxi Trips Data Warehouse (Databricks)

Small Databricks medallion warehouse for taxi trips using Delta Live Tables (DLT).

## Layout

- `bootstrap/01_setup_catalog.sql`:
  Creates `taxi_dwh` catalog and `bronze/silver/gold` schemas.
- `dlt/taxitrips_pipeline.py`:
  DLT pipeline that builds bronze, silver, and gold tables.
- `workflows/taxitrips_job.json`:
  Example Databricks Jobs API payload to run the pipeline daily.
- `notebooks/analytics_examples.sql`:
  Starter SQL for common analytics queries.

## Prerequisites

- Unity Catalog enabled workspace.
- A cloud path with taxi trip files (CSV expected by default), for example:
  - `s3://<bucket>/taxi/raw/`
  - `abfss://<container>@<account>.dfs.core.windows.net/taxi/raw/`
  - `gs://<bucket>/taxi/raw/`

## Run

1. Run `bootstrap/01_setup_catalog.sql` in Databricks SQL.
2. Create a DLT pipeline in Databricks and point it at `dlt/taxitrips_pipeline.py`.
3. Set the pipeline configuration key:
   - `taxi.source.path` = your raw data path
4. Start the pipeline in `Triggered` mode (or continuous if preferred).
5. Optionally create a scheduled job using `workflows/taxitrips_job.json`.

## Notes

- The DLT pipeline enforces basic quality checks:
  - non-null pickup/dropoff timestamps
  - non-negative fare and total amounts
  - dropoff >= pickup
- Gold table is partitioned by `trip_date` and includes derived fields for BI.
