-- Taxi rides warehouse build (Databricks SQL)
-- Verified source schema: samples.nyctaxi.trips

-- 1) Set your target catalog/schema.
CREATE CATALOG IF NOT EXISTS main;
CREATE SCHEMA IF NOT EXISTS main.taxi_dw;

-- 3) City dimension.
-- No city column exists in samples.nyctaxi.trips, so create a default city bucket.
CREATE OR REPLACE TABLE main.taxi_dw.dim_city
USING DELTA
AS
SELECT
  0 AS city_key,
  'UNKNOWN' AS city_name,
  current_timestamp() AS dw_loaded_at;

-- 4) Zipcode dimension (with city FK for future enrichment).
CREATE OR REPLACE TABLE main.taxi_dw.dim_zipcode
USING DELTA
AS
WITH source_mapped AS (
  SELECT
    lpad(CAST(pickup_zip AS STRING), 5, '0') AS pickup_zipcode,
    lpad(CAST(dropoff_zip AS STRING), 5, '0') AS dropoff_zipcode
  FROM samples.nyctaxi.trips
),
all_zip AS (
  SELECT pickup_zipcode AS zipcode FROM source_mapped WHERE pickup_zipcode IS NOT NULL
  UNION
  SELECT dropoff_zipcode AS zipcode FROM source_mapped WHERE dropoff_zipcode IS NOT NULL
)
SELECT
  ABS(xxhash64(zipcode)) AS zipcode_key,
  zipcode,
  0 AS city_key,
  current_timestamp() AS dw_loaded_at
FROM all_zip;

-- 5) Date dimension (for both pickup and dropoff dates).
CREATE OR REPLACE TABLE main.taxi_dw.dim_date
USING DELTA
AS
WITH source_mapped AS (
  SELECT
    CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_ts,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_ts
  FROM samples.nyctaxi.trips
),
all_dates AS (
  SELECT DATE(pickup_ts) AS dt FROM source_mapped WHERE pickup_ts IS NOT NULL
  UNION
  SELECT DATE(dropoff_ts) AS dt FROM source_mapped WHERE dropoff_ts IS NOT NULL
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
FROM all_dates;

-- 6) Fact table.
CREATE OR REPLACE TABLE main.taxi_dw.fact_taxi_rides
USING DELTA
AS
WITH source_mapped AS (
  SELECT
    CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_ts,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_ts,
    lpad(CAST(pickup_zip AS STRING), 5, '0') AS pickup_zipcode,
    lpad(CAST(dropoff_zip AS STRING), 5, '0') AS dropoff_zipcode,
    CAST(fare_amount AS DECIMAL(12,2)) AS fare_amount
  FROM samples.nyctaxi.trips
  WHERE fare_amount IS NOT NULL
)
SELECT
  sha2(
    concat_ws(
      '||',
      COALESCE(CAST(pickup_ts AS STRING), ''),
      COALESCE(CAST(dropoff_ts AS STRING), ''),
      COALESCE(pickup_zipcode, ''),
      COALESCE(dropoff_zipcode, ''),
      COALESCE(CAST(fare_amount AS STRING), '')
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
  current_timestamp() AS dw_loaded_at
FROM source_mapped;

-- 7) Performance tuning.
OPTIMIZE main.taxi_dw.dim_city;
OPTIMIZE main.taxi_dw.dim_zipcode ZORDER BY (zipcode);
OPTIMIZE main.taxi_dw.dim_date ZORDER BY (date_key);
OPTIMIZE main.taxi_dw.fact_taxi_rides ZORDER BY (pickup_date_key, pickup_zipcode_key, dropoff_zipcode_key);

ANALYZE TABLE main.taxi_dw.dim_city COMPUTE STATISTICS;
ANALYZE TABLE main.taxi_dw.dim_zipcode COMPUTE STATISTICS;
ANALYZE TABLE main.taxi_dw.dim_date COMPUTE STATISTICS;
ANALYZE TABLE main.taxi_dw.fact_taxi_rides COMPUTE STATISTICS;
