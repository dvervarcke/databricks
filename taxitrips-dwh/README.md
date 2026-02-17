# Taxi Trips Data Warehouse (Databricks)

Small Databricks medallion warehouse for taxi trips using Delta Live Tables (DLT).

## Layout

- `bootstrap/01_setup_catalog.sql`:
  Creates `taxi_dwh` catalog and `bronze/silver/gold` schemas.
- `dlt/taxitrips_pipeline.py`:
  DLT pipeline that builds bronze, silver, and gold tables.
- `databricks.yml` and `resources/pipeline_and_job.yml`:
  Databricks Asset Bundle configuration for deploying pipeline + daily job.
- `workflows/taxitrips_job.json`:
  Example Databricks Jobs API payload for manual/API-based job setup.
- `notebooks/analytics_examples.sql`:
  Starter SQL for common analytics queries.

## Prerequisites

- Unity Catalog enabled workspace.
- Databricks CLI configured with auth for your workspace.
- A cloud path with taxi trip files, for example:
  - `s3://<bucket>/taxi/raw/`
  - `abfss://<container>@<account>.dfs.core.windows.net/taxi/raw/`
  - `gs://<bucket>/taxi/raw/`

## Pipeline behavior

- Supports NYC yellow and green taxi schemas by coalescing:
  - `tpep_pickup_datetime` / `lpep_pickup_datetime`
  - `tpep_dropoff_datetime` / `lpep_dropoff_datetime`
- Uses Auto Loader schema evolution in rescue mode:
  - unexpected fields are captured in `_rescued_data`
- Enforces quality checks:
  - non-null pickup/dropoff timestamps
  - non-negative fare and total amounts
  - dropoff >= pickup

## Deploy with Databricks Asset Bundle

1. Change directory to this project:
   - `cd taxitrips-dwh`
2. Update source location in `resources/pipeline_and_job.yml`:
   - `taxi.source.path`
3. Validate bundle:
   - `databricks bundle validate -t dev`
4. Deploy:
   - `databricks bundle deploy -t dev`
5. Run job:
   - `databricks bundle run -t dev taxitrips_daily_job`

## Alternative manual setup

1. Run `bootstrap/01_setup_catalog.sql` in Databricks SQL.
2. Create a DLT pipeline and point it at `dlt/taxitrips_pipeline.py`.
3. Set pipeline configuration:
   - `taxi.source.path`
   - `taxi.source.format` (default `csv`)
   - `taxi.schema.location` (for Auto Loader schemas)
