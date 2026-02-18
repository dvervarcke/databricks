# Databricks notebook source
# Pipeline: update missing city mappings for taxi ZIP codes incrementally.

from __future__ import annotations

import argparse
import json
import urllib.error
import urllib.request
from datetime import datetime, timezone

from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", default="main")
parser.add_argument("--schema", default="taxi_dw")
parser.add_argument("--source-table", default="samples.nyctaxi.trips")
args, _ = parser.parse_known_args()

CATALOG = args.catalog
SCHEMA = args.schema
SOURCE_TABLE = args.source_table
REF_TABLE = f"{CATALOG}.{SCHEMA}.zip_city_reference"
DIM_CITY_TABLE = f"{CATALOG}.{SCHEMA}.dim_city"
DIM_ZIP_TABLE = f"{CATALOG}.{SCHEMA}.dim_zipcode"

ZIP_API_TEMPLATE = "https://api.zippopotam.us/us/{zipcode}"


def fetch_zip_city(zipcode: str) -> tuple[str, str]:
    url = ZIP_API_TEMPLATE.format(zipcode=zipcode)
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
        return "UNKNOWN", ""

    places = payload.get("places") or []
    if not places:
        return "UNKNOWN", ""

    city = (places[0].get("place name") or "UNKNOWN").strip()
    state = (places[0].get("state abbreviation") or "").strip()
    if not city:
        city = "UNKNOWN"
    return city.upper(), state.upper()


spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {REF_TABLE} (
      zipcode STRING,
      city_name STRING,
      state_code STRING,
      source STRING,
      updated_at TIMESTAMP
    )
    USING DELTA
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {DIM_CITY_TABLE} (
      city_key BIGINT,
      city_name STRING,
      state_code STRING,
      dw_loaded_at TIMESTAMP
    )
    USING DELTA
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {DIM_ZIP_TABLE} (
      zipcode_key BIGINT,
      zipcode STRING,
      city_key BIGINT,
      dw_loaded_at TIMESTAMP
    )
    USING DELTA
    """
)

spark.sql(
    f"""
    MERGE INTO {DIM_CITY_TABLE} t
    USING (
      SELECT 0 AS city_key, 'UNKNOWN' AS city_name, CAST(NULL AS STRING) AS state_code, current_timestamp() AS dw_loaded_at
    ) s
    ON t.city_key = s.city_key
    WHEN NOT MATCHED THEN INSERT (city_key, city_name, state_code, dw_loaded_at)
    VALUES (s.city_key, s.city_name, s.state_code, s.dw_loaded_at)
    """
)

zip_source_df = spark.sql(
    f"""
    WITH zip_source AS (
      SELECT lpad(CAST(pickup_zip AS STRING), 5, '0') AS zipcode
      FROM {SOURCE_TABLE}
      WHERE pickup_zip IS NOT NULL
      UNION
      SELECT lpad(CAST(dropoff_zip AS STRING), 5, '0') AS zipcode
      FROM {SOURCE_TABLE}
      WHERE dropoff_zip IS NOT NULL
    )
    SELECT DISTINCT zipcode
    FROM zip_source
    """
)

missing_zip_rows = (
    zip_source_df.alias("z")
    .join(spark.table(REF_TABLE).alias("r"), on="zipcode", how="left")
    .where(
        F.col("r.zipcode").isNull()
        | (F.upper(F.coalesce(F.col("r.city_name"), F.lit("UNKNOWN"))) == F.lit("UNKNOWN"))
    )
    .select("zipcode")
    .distinct()
    .orderBy("zipcode")
    .collect()
)

missing_zips = [row["zipcode"] for row in missing_zip_rows]
print(f"Missing or unknown ZIP mappings to refresh: {len(missing_zips)}")

updates = []
for zipcode in missing_zips:
    city_name, state_code = fetch_zip_city(zipcode)
    updates.append(
        {
            "zipcode": zipcode,
            "city_name": city_name,
            "state_code": state_code,
            "source": "zippopotam.us",
            "updated_at": datetime.now(timezone.utc),
        }
    )

if updates:
    updates_df = spark.createDataFrame(updates)
    updates_df.createOrReplaceTempView("zip_updates")

    spark.sql(
        f"""
        MERGE INTO {REF_TABLE} AS t
        USING zip_updates AS s
        ON t.zipcode = s.zipcode
        WHEN MATCHED THEN UPDATE SET
          city_name = s.city_name,
          state_code = s.state_code,
          source = s.source,
          updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT (
          zipcode, city_name, state_code, source, updated_at
        ) VALUES (
          s.zipcode, s.city_name, s.state_code, s.source, s.updated_at
        )
        """
    )

spark.sql(
    f"""
    MERGE INTO {DIM_CITY_TABLE} t
    USING (
      SELECT DISTINCT
        ABS(xxhash64(concat_ws('|', UPPER(TRIM(city_name)), coalesce(UPPER(TRIM(state_code)), '')))) AS city_key,
        UPPER(TRIM(city_name)) AS city_name,
        UPPER(TRIM(state_code)) AS state_code,
        current_timestamp() AS dw_loaded_at
      FROM {REF_TABLE}
      WHERE city_name IS NOT NULL AND TRIM(city_name) <> '' AND UPPER(TRIM(city_name)) <> 'UNKNOWN'
    ) s
    ON t.city_key = s.city_key
    WHEN MATCHED AND (
      t.city_name <> s.city_name OR coalesce(t.state_code, '') <> coalesce(s.state_code, '')
    ) THEN UPDATE SET
      city_name = s.city_name,
      state_code = s.state_code,
      dw_loaded_at = s.dw_loaded_at
    WHEN NOT MATCHED THEN INSERT (city_key, city_name, state_code, dw_loaded_at)
    VALUES (s.city_key, s.city_name, s.state_code, s.dw_loaded_at)
    """
)

spark.sql(
    f"""
    MERGE INTO {DIM_ZIP_TABLE} t
    USING (
      WITH zip_source AS (
        SELECT lpad(CAST(pickup_zip AS STRING), 5, '0') AS zipcode
        FROM {SOURCE_TABLE}
        WHERE pickup_zip IS NOT NULL
        UNION
        SELECT lpad(CAST(dropoff_zip AS STRING), 5, '0') AS zipcode
        FROM {SOURCE_TABLE}
        WHERE dropoff_zip IS NOT NULL
      ),
      zip_city AS (
        SELECT
          z.zipcode,
          COALESCE(UPPER(TRIM(r.city_name)), 'UNKNOWN') AS city_name,
          UPPER(TRIM(r.state_code)) AS state_code
        FROM (SELECT DISTINCT zipcode FROM zip_source) z
        LEFT JOIN {REF_TABLE} r ON z.zipcode = r.zipcode
      )
      SELECT
        ABS(xxhash64(zipcode)) AS zipcode_key,
        zipcode,
        CASE
          WHEN city_name = 'UNKNOWN' THEN 0
          ELSE ABS(xxhash64(concat_ws('|', city_name, coalesce(state_code, ''))))
        END AS city_key,
        current_timestamp() AS dw_loaded_at
      FROM zip_city
    ) s
    ON t.zipcode_key = s.zipcode_key
    WHEN MATCHED AND (t.city_key <> s.city_key OR t.zipcode <> s.zipcode) THEN UPDATE SET
      zipcode = s.zipcode,
      city_key = s.city_key,
      dw_loaded_at = s.dw_loaded_at
    WHEN NOT MATCHED THEN INSERT (zipcode_key, zipcode, city_key, dw_loaded_at)
    VALUES (s.zipcode_key, s.zipcode, s.city_key, s.dw_loaded_at)
    """
)

spark.sql(f"OPTIMIZE {DIM_CITY_TABLE} ZORDER BY (city_name, state_code)")
spark.sql(f"OPTIMIZE {DIM_ZIP_TABLE} ZORDER BY (zipcode, city_key)")
spark.sql(f"ANALYZE TABLE {DIM_CITY_TABLE} COMPUTE STATISTICS")
spark.sql(f"ANALYZE TABLE {DIM_ZIP_TABLE} COMPUTE STATISTICS")

summary = spark.sql(
    f"""
    SELECT
      (SELECT COUNT(*) FROM {REF_TABLE}) AS reference_rows,
      (SELECT COUNT(*) FROM {DIM_CITY_TABLE}) AS city_rows,
      (SELECT COUNT(*) FROM {DIM_ZIP_TABLE}) AS zipcode_rows,
      (SELECT COUNT(*) FROM {DIM_ZIP_TABLE} WHERE city_key = 0) AS unknown_zip_city_keys
    """
).collect()[0]

print(
    "Pipeline summary: "
    f"reference_rows={summary['reference_rows']}, "
    f"city_rows={summary['city_rows']}, "
    f"zipcode_rows={summary['zipcode_rows']}, "
    f"unknown_zip_city_keys={summary['unknown_zip_city_keys']}"
)
