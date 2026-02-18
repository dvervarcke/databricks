# Databricks notebook source
# Pipeline: incremental DW load for taxi rides (fact + dims via MERGE)

from __future__ import annotations

import argparse

STATEMENTS = [
    """
    CREATE CATALOG IF NOT EXISTS main
    """,
    """
    CREATE SCHEMA IF NOT EXISTS main.taxi_dw
    """,
    """
    CREATE TABLE IF NOT EXISTS main.taxi_dw.etl_watermark (
      process_name STRING,
      last_event_ts TIMESTAMP,
      updated_at TIMESTAMP
    )
    USING DELTA
    """,
    """
    MERGE INTO main.taxi_dw.etl_watermark t
    USING (
      SELECT
        'fact_taxi_rides' AS process_name,
        TIMESTAMP('1900-01-01 00:00:00') AS last_event_ts,
        current_timestamp() AS updated_at
    ) s
    ON t.process_name = s.process_name
    WHEN NOT MATCHED THEN INSERT (process_name, last_event_ts, updated_at)
    VALUES (s.process_name, s.last_event_ts, s.updated_at)
    """,
    """
    CREATE TABLE IF NOT EXISTS main.taxi_dw.zip_city_reference (
      zipcode STRING,
      city_name STRING,
      state_code STRING,
      source STRING,
      updated_at TIMESTAMP
    )
    USING DELTA
    """,
    """
    CREATE TABLE IF NOT EXISTS main.taxi_dw.dim_city (
      city_key BIGINT,
      city_name STRING,
      state_code STRING,
      dw_loaded_at TIMESTAMP
    )
    USING DELTA
    """,
    """
    CREATE TABLE IF NOT EXISTS main.taxi_dw.dim_zipcode (
      zipcode_key BIGINT,
      zipcode STRING,
      city_key BIGINT,
      dw_loaded_at TIMESTAMP
    )
    USING DELTA
    """,
    """
    CREATE TABLE IF NOT EXISTS main.taxi_dw.dim_date (
      date_key INT,
      full_date DATE,
      year_num INT,
      quarter_num INT,
      month_num INT,
      day_of_month INT,
      day_of_week INT,
      day_name STRING,
      week_of_year INT,
      dw_loaded_at TIMESTAMP
    )
    USING DELTA
    """,
    """
    CREATE TABLE IF NOT EXISTS main.taxi_dw.fact_taxi_rides (
      ride_key STRING,
      pickup_ts TIMESTAMP,
      dropoff_ts TIMESTAMP,
      pickup_date_key INT,
      dropoff_date_key INT,
      pickup_zipcode_key BIGINT,
      dropoff_zipcode_key BIGINT,
      fare_amount DECIMAL(12,2),
      dw_loaded_at TIMESTAMP
    )
    USING DELTA
    """,
    """
    MERGE INTO main.taxi_dw.dim_city t
    USING (
      SELECT 0 AS city_key, 'UNKNOWN' AS city_name, CAST(NULL AS STRING) AS state_code, current_timestamp() AS dw_loaded_at
    ) s
    ON t.city_key = s.city_key
    WHEN NOT MATCHED THEN INSERT (city_key, city_name, state_code, dw_loaded_at)
    VALUES (s.city_key, s.city_name, s.state_code, s.dw_loaded_at)
    """,
    """
    MERGE INTO main.taxi_dw.dim_date t
    USING (
      WITH wm AS (
        SELECT coalesce(max(last_event_ts), TIMESTAMP('1900-01-01 00:00:00')) AS last_event_ts
        FROM main.taxi_dw.etl_watermark
        WHERE process_name = 'fact_taxi_rides'
      ),
      source_delta AS (
        SELECT
          CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_ts,
          CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_ts,
          greatest(
            coalesce(CAST(tpep_pickup_datetime AS TIMESTAMP), TIMESTAMP('1900-01-01 00:00:00')),
            coalesce(CAST(tpep_dropoff_datetime AS TIMESTAMP), TIMESTAMP('1900-01-01 00:00:00'))
          ) AS event_ts
        FROM samples.nyctaxi.trips
      ),
      delta_dates AS (
        SELECT DATE(pickup_ts) AS dt FROM source_delta, wm WHERE pickup_ts IS NOT NULL AND event_ts > wm.last_event_ts - INTERVAL 2 DAYS
        UNION
        SELECT DATE(dropoff_ts) AS dt FROM source_delta, wm WHERE dropoff_ts IS NOT NULL AND event_ts > wm.last_event_ts - INTERVAL 2 DAYS
      )
      SELECT
        CAST(date_format(dt, 'yyyyMMdd') AS INT) AS date_key,
        dt AS full_date,
        year(dt) AS year_num,
        quarter(dt) AS quarter_num,
        month(dt) AS month_num,
        day(dt) AS day_of_month,
        dayofweek(dt) AS day_of_week,
        date_format(dt, 'E') AS day_name,
        weekofyear(dt) AS week_of_year,
        current_timestamp() AS dw_loaded_at
      FROM delta_dates
    ) s
    ON t.date_key = s.date_key
    WHEN NOT MATCHED THEN INSERT (
      date_key, full_date, year_num, quarter_num, month_num, day_of_month, day_of_week, day_name, week_of_year, dw_loaded_at
    ) VALUES (
      s.date_key, s.full_date, s.year_num, s.quarter_num, s.month_num, s.day_of_month, s.day_of_week, s.day_name, s.week_of_year, s.dw_loaded_at
    )
    """,
    """
    MERGE INTO main.taxi_dw.dim_zipcode t
    USING (
      WITH wm AS (
        SELECT coalesce(max(last_event_ts), TIMESTAMP('1900-01-01 00:00:00')) AS last_event_ts
        FROM main.taxi_dw.etl_watermark
        WHERE process_name = 'fact_taxi_rides'
      ),
      source_delta AS (
        SELECT
          lpad(CAST(pickup_zip AS STRING), 5, '0') AS pickup_zipcode,
          lpad(CAST(dropoff_zip AS STRING), 5, '0') AS dropoff_zipcode,
          greatest(
            coalesce(CAST(tpep_pickup_datetime AS TIMESTAMP), TIMESTAMP('1900-01-01 00:00:00')),
            coalesce(CAST(tpep_dropoff_datetime AS TIMESTAMP), TIMESTAMP('1900-01-01 00:00:00'))
          ) AS event_ts
        FROM samples.nyctaxi.trips
      ),
      delta_zip AS (
        SELECT pickup_zipcode AS zipcode FROM source_delta, wm WHERE pickup_zipcode IS NOT NULL AND event_ts > wm.last_event_ts - INTERVAL 2 DAYS
        UNION
        SELECT dropoff_zipcode AS zipcode FROM source_delta, wm WHERE dropoff_zipcode IS NOT NULL AND event_ts > wm.last_event_ts - INTERVAL 2 DAYS
      ),
      zip_mapped AS (
        SELECT
          z.zipcode,
          coalesce(upper(trim(r.city_name)), 'UNKNOWN') AS city_name,
          upper(trim(r.state_code)) AS state_code
        FROM delta_zip z
        LEFT JOIN main.taxi_dw.zip_city_reference r ON z.zipcode = r.zipcode
      )
      SELECT
        ABS(xxhash64(zipcode)) AS zipcode_key,
        zipcode,
        CASE
          WHEN city_name = 'UNKNOWN' THEN 0
          ELSE ABS(xxhash64(concat_ws('|', city_name, coalesce(state_code, ''))))
        END AS city_key,
        current_timestamp() AS dw_loaded_at
      FROM zip_mapped
    ) s
    ON t.zipcode_key = s.zipcode_key
    WHEN MATCHED AND (t.city_key <> s.city_key OR t.zipcode <> s.zipcode) THEN UPDATE SET
      zipcode = s.zipcode,
      city_key = s.city_key,
      dw_loaded_at = s.dw_loaded_at
    WHEN NOT MATCHED THEN INSERT (zipcode_key, zipcode, city_key, dw_loaded_at)
    VALUES (s.zipcode_key, s.zipcode, s.city_key, s.dw_loaded_at)
    """,
    """
    MERGE INTO main.taxi_dw.fact_taxi_rides t
    USING (
      WITH wm AS (
        SELECT coalesce(max(last_event_ts), TIMESTAMP('1900-01-01 00:00:00')) AS last_event_ts
        FROM main.taxi_dw.etl_watermark
        WHERE process_name = 'fact_taxi_rides'
      ),
      source_delta AS (
        SELECT
          CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_ts,
          CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_ts,
          lpad(CAST(pickup_zip AS STRING), 5, '0') AS pickup_zipcode,
          lpad(CAST(dropoff_zip AS STRING), 5, '0') AS dropoff_zipcode,
          CAST(fare_amount AS DECIMAL(12,2)) AS fare_amount,
          greatest(
            coalesce(CAST(tpep_pickup_datetime AS TIMESTAMP), TIMESTAMP('1900-01-01 00:00:00')),
            coalesce(CAST(tpep_dropoff_datetime AS TIMESTAMP), TIMESTAMP('1900-01-01 00:00:00'))
          ) AS event_ts
        FROM samples.nyctaxi.trips
        WHERE fare_amount IS NOT NULL
      ),
      delta_fact AS (
        SELECT *
        FROM source_delta, wm
        WHERE event_ts > wm.last_event_ts - INTERVAL 2 DAYS
      )
      SELECT
        sha2(
          concat_ws(
            '||',
            coalesce(CAST(pickup_ts AS STRING), ''),
            coalesce(CAST(dropoff_ts AS STRING), ''),
            coalesce(pickup_zipcode, ''),
            coalesce(dropoff_zipcode, ''),
            coalesce(CAST(fare_amount AS STRING), '')
          ),
          256
        ) AS ride_key,
        pickup_ts,
        dropoff_ts,
        CAST(date_format(DATE(pickup_ts), 'yyyyMMdd') AS INT) AS pickup_date_key,
        CAST(date_format(DATE(dropoff_ts), 'yyyyMMdd') AS INT) AS dropoff_date_key,
        ABS(xxhash64(pickup_zipcode)) AS pickup_zipcode_key,
        ABS(xxhash64(dropoff_zipcode)) AS dropoff_zipcode_key,
        fare_amount,
        current_timestamp() AS dw_loaded_at,
        event_ts
      FROM delta_fact
    ) s
    ON t.ride_key = s.ride_key
    WHEN MATCHED THEN UPDATE SET
      pickup_ts = s.pickup_ts,
      dropoff_ts = s.dropoff_ts,
      pickup_date_key = s.pickup_date_key,
      dropoff_date_key = s.dropoff_date_key,
      pickup_zipcode_key = s.pickup_zipcode_key,
      dropoff_zipcode_key = s.dropoff_zipcode_key,
      fare_amount = s.fare_amount,
      dw_loaded_at = s.dw_loaded_at
    WHEN NOT MATCHED THEN INSERT (
      ride_key, pickup_ts, dropoff_ts, pickup_date_key, dropoff_date_key, pickup_zipcode_key, dropoff_zipcode_key, fare_amount, dw_loaded_at
    ) VALUES (
      s.ride_key, s.pickup_ts, s.dropoff_ts, s.pickup_date_key, s.dropoff_date_key, s.pickup_zipcode_key, s.dropoff_zipcode_key, s.fare_amount, s.dw_loaded_at
    )
    """,
    """
    MERGE INTO main.taxi_dw.etl_watermark t
    USING (
      SELECT
        'fact_taxi_rides' AS process_name,
        coalesce(
          max(
            greatest(
              coalesce(CAST(tpep_pickup_datetime AS TIMESTAMP), TIMESTAMP('1900-01-01 00:00:00')),
              coalesce(CAST(tpep_dropoff_datetime AS TIMESTAMP), TIMESTAMP('1900-01-01 00:00:00'))
            )
          ),
          TIMESTAMP('1900-01-01 00:00:00')
        ) AS last_event_ts,
        current_timestamp() AS updated_at
      FROM samples.nyctaxi.trips
    ) s
    ON t.process_name = s.process_name
    WHEN MATCHED THEN UPDATE SET
      last_event_ts = greatest(t.last_event_ts, s.last_event_ts),
      updated_at = s.updated_at
    WHEN NOT MATCHED THEN INSERT (process_name, last_event_ts, updated_at)
    VALUES (s.process_name, s.last_event_ts, s.updated_at)
    """,
    """
    OPTIMIZE main.taxi_dw.dim_city ZORDER BY (city_name, state_code)
    """,
    """
    OPTIMIZE main.taxi_dw.dim_zipcode ZORDER BY (zipcode, city_key)
    """,
    """
    OPTIMIZE main.taxi_dw.dim_date ZORDER BY (date_key)
    """,
    """
    OPTIMIZE main.taxi_dw.fact_taxi_rides ZORDER BY (pickup_date_key, pickup_zipcode_key, dropoff_zipcode_key)
    """,
    """
    ANALYZE TABLE main.taxi_dw.dim_city COMPUTE STATISTICS
    """,
    """
    ANALYZE TABLE main.taxi_dw.dim_zipcode COMPUTE STATISTICS
    """,
    """
    ANALYZE TABLE main.taxi_dw.dim_date COMPUTE STATISTICS
    """,
    """
    ANALYZE TABLE main.taxi_dw.fact_taxi_rides COMPUTE STATISTICS
    """,
]

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", default="main")
parser.add_argument("--schema", default="taxi_dw")
parser.add_argument("--source-table", default="samples.nyctaxi.trips")
args, _ = parser.parse_known_args()

namespace = f"{args.catalog}.{args.schema}"


def render(sql: str) -> str:
    return (
        sql.replace("CREATE CATALOG IF NOT EXISTS main", f"CREATE CATALOG IF NOT EXISTS {args.catalog}")
        .replace("main.taxi_dw", namespace)
        .replace("samples.nyctaxi.trips", args.source_table)
    )


for i, statement in enumerate(STATEMENTS, start=1):
    spark.sql(render(statement))
    print(f"Statement {i}/{len(STATEMENTS)} succeeded")

summary = spark.sql(
    render(
        """
    SELECT
      (SELECT COUNT(*) FROM main.taxi_dw.fact_taxi_rides) AS fact_rows,
      (SELECT COUNT(*) FROM main.taxi_dw.dim_date) AS date_rows,
      (SELECT COUNT(*) FROM main.taxi_dw.dim_zipcode) AS zip_rows,
      (SELECT COUNT(*) FROM main.taxi_dw.dim_city) AS city_rows,
      (SELECT max(last_event_ts) FROM main.taxi_dw.etl_watermark WHERE process_name = 'fact_taxi_rides') AS watermark_ts
    """
    )
).collect()[0]

print(
    "Incremental load summary: "
    f"fact_rows={summary['fact_rows']}, "
    f"date_rows={summary['date_rows']}, "
    f"zip_rows={summary['zip_rows']}, "
    f"city_rows={summary['city_rows']}, "
    f"watermark_ts={summary['watermark_ts']}"
)
