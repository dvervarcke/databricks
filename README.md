# Taxi DW on Databricks

This project builds and operates a taxi ride warehouse model in Databricks from:

- Source: `samples.nyctaxi.trips`

It includes:

- Incremental star-schema SQL load (`fact_taxi_rides`, `dim_date`, `dim_zipcode`, `dim_city`)
- External ZIP-to-city enrichment flow
- Scheduled Databricks Workflow to refresh missing city mappings
- CI/CD deployment to Databricks `prod` target via Databricks Asset Bundles

## Repository Structure

- `/Users/dvervarcke/Documents/New project/sql/001_build_taxi_dw.sql`
  - Incremental warehouse load from `samples.nyctaxi.trips` using watermark + `MERGE`
- `/Users/dvervarcke/Documents/New project/sql/002_enrich_city_from_external_zip.sql`
  - One-time city enrichment from external ZIP reference
- `/Users/dvervarcke/Documents/New project/pipelines/update_missing_cities.py`
  - Pipeline file used by scheduled Databricks job
- `/Users/dvervarcke/Documents/New project/jobs/taxi_dw_missing_city_job.json`
  - Databricks job definition (serverless, scheduled)
- `/Users/dvervarcke/Documents/New project/databricks.yml`
  - Databricks Asset Bundle with `dev` and `prod` targets
- `/Users/dvervarcke/Documents/New project/.github/workflows/databricks-cicd.yml`
  - GitHub Actions workflow for validate + deploy
- `/Users/dvervarcke/Documents/New project/data/zip_city_mapping.csv`
  - Generated external ZIP-city-state snapshot
- `/Users/dvervarcke/Documents/New project/docs/taxi_dw_runbook.md`
  - Operational runbook

## Data Model

- `main.taxi_dw.fact_taxi_rides`
  - Ride grain with pickup/dropoff timestamps, ZIP foreign keys, and fare amount
- `main.taxi_dw.dim_date`
  - Date dimension keyed by `yyyyMMdd`
- `main.taxi_dw.dim_zipcode`
  - ZIP dimension with `city_key` foreign key
- `main.taxi_dw.dim_city`
  - City dimension with `city_name` and `state_code`
- `main.taxi_dw.zip_city_reference`
  - Incremental reference table maintained by the pipeline

## Deployment Steps

1. Run base build SQL:
   - `/Users/dvervarcke/Documents/New project/sql/001_build_taxi_dw.sql`
2. Run one-time enrichment SQL:
   - `/Users/dvervarcke/Documents/New project/sql/002_enrich_city_from_external_zip.sql`
3. Deploy pipeline files and job:
   - Pipeline files:
     - `/Users/rickoe@hotmail.com/taxi_dw/pipelines/run_incremental_dw_load.py`
     - `/Users/rickoe@hotmail.com/taxi_dw/pipelines/update_missing_cities.py`
   - Job name: `taxi-dw-daily-incremental-load-and-enrichment`
   - Job ID: `52643488824313`

## CI/CD to Prod Catalog

The bundle deploys to the same Databricks workspace with different targets:

- `dev` target:
  - Catalog: `dev`
  - Schema: `taxi_dw`
  - Job schedule: paused
- `prod` target:
  - Catalog: `prod`
  - Schema: `taxi_dw`
  - Job name: `taxi-dw-prod-daily-incremental-load-and-enrichment`
  - Job schedule: daily 07:00 PT (unpaused)

Required GitHub repository secrets:

- `DATABRICKS_HOST` (example: `https://dbc-xxxx.cloud.databricks.com`)
- `DATABRICKS_TOKEN` (PAT with permission to deploy jobs/files)

Local bundle commands:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev

databricks bundle validate -t prod
databricks bundle deploy -t prod
```

## Scheduled Pipeline

- Runs daily at `07:00 America/Los_Angeles` (PT)
- Updates only missing/unknown ZIP mappings via `zippopotam.us`
- Upserts `main.taxi_dw.zip_city_reference`
- Task 1 runs incremental DW load (fact/date/zipcode `MERGE`)
- Task 2 runs city enrichment (`MERGE` updates)

Manual trigger:

```bash
databricks jobs run-now 52643488824313 --profile rickoe@hotmail.com
```

## Notes

- Workspace is serverless-only, so job uses serverless environment config.
- City enrichment source is external and should be treated as reference data.
- For operational details and validation queries, see:
  - `/Users/dvervarcke/Documents/New project/docs/taxi_dw_runbook.md`
