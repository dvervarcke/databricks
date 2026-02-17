-- Build a small star schema from shared NYC taxi source table.
-- Source: <catalog>.nyctaxi.trips
-- Targets:
--   taxi_dwh.gold.fact_taxitrips_zip
--   taxi_dwh.gold.dim_zipcode

CREATE CATALOG IF NOT EXISTS taxi_dwh;
CREATE SCHEMA IF NOT EXISTS taxi_dwh.gold;

DECLARE source_catalog STRING DEFAULT current_catalog();
DECLARE source_schema STRING DEFAULT 'nyctaxi';
DECLARE source_table STRING DEFAULT 'trips';
DECLARE source_fq_table STRING;
DECLARE target_schema STRING DEFAULT 'taxi_dwh.gold';

DECLARE trip_date_col STRING;
DECLARE origin_zip_col STRING;
DECLARE destination_zip_col STRING;
DECLARE fare_col STRING;
DECLARE origin_city_col STRING;
DECLARE destination_city_col STRING;
DECLARE origin_city_expr STRING;
DECLARE destination_city_expr STRING;

EXECUTE IMMEDIATE format_string(
  'CREATE TABLE IF NOT EXISTS %s.zip_city_lookup (
     zip_code STRING,
     city STRING
   )',
  target_schema
);

SET VAR trip_date_col = (
  SELECT COALESCE(
    MAX(CASE WHEN lower(col_name) = 'trip_date' THEN col_name END),
    MAX(CASE WHEN lower(col_name) = 'pickup_date' THEN col_name END),
    MAX(CASE WHEN lower(col_name) = 'pickup_datetime' THEN col_name END),
    MAX(CASE WHEN lower(col_name) = 'tpep_pickup_datetime' THEN col_name END),
    MAX(CASE WHEN lower(col_name) = 'lpep_pickup_datetime' THEN col_name END)
  )
  FROM information_schema.columns
  WHERE table_catalog = source_catalog
    AND table_schema = source_schema
    AND table_name = source_table
);

SET VAR origin_zip_col = (
  SELECT COALESCE(
    MAX(CASE WHEN lower(col_name) = 'trip_zip_origin' THEN col_name END),
    MAX(CASE WHEN lower(col_name) = 'pickup_zip' THEN col_name END),
    MAX(CASE WHEN lower(col_name) = 'origin_zip' THEN col_name END),
    MAX(CASE WHEN lower(col_name) = 'pu_zip' THEN col_name END),
    MAX(CASE WHEN lower(col_name) = 'pickup_zipcode' THEN col_name END),
    MAX(CASE WHEN lower(col_name) = 'pickup_postal_code' THEN col_name END)
  )
  FROM information_schema.columns
  WHERE table_catalog = source_catalog
    AND table_schema = source_schema
    AND table_name = source_table
);

SET VAR destination_zip_col = (
  SELECT COALESCE(
    MAX(CASE WHEN lower(col_name) = 'trip_zip_destination' THEN col_name END),
    MAX(CASE WHEN lower(col_name) = 'dropoff_zip' THEN col_name END),
    MAX(CASE WHEN lower(col_name) = 'destination_zip' THEN col_name END),
    MAX(CASE WHEN lower(col_name) = 'do_zip' THEN col_name END),
    MAX(CASE WHEN lower(col_name) = 'dropoff_zipcode' THEN col_name END),
    MAX(CASE WHEN lower(col_name) = 'dropoff_postal_code' THEN col_name END)
  )
  FROM information_schema.columns
  WHERE table_catalog = source_catalog
    AND table_schema = source_schema
    AND table_name = source_table
);

SET VAR fare_col = (
  SELECT COALESCE(
    MAX(CASE WHEN lower(col_name) = 'fare' THEN col_name END),
    MAX(CASE WHEN lower(col_name) = 'fare_amount' THEN col_name END),
    MAX(CASE WHEN lower(col_name) = 'total_amount' THEN col_name END)
  )
  FROM information_schema.columns
  WHERE table_catalog = source_catalog
    AND table_schema = source_schema
    AND table_name = source_table
);

SET VAR origin_city_col = (
  SELECT COALESCE(
    MAX(CASE WHEN lower(col_name) = 'origin_city' THEN col_name END),
    MAX(CASE WHEN lower(col_name) = 'pickup_city' THEN col_name END),
    MAX(CASE WHEN lower(col_name) = 'pu_city' THEN col_name END),
    MAX(CASE WHEN lower(col_name) = 'city' THEN col_name END)
  )
  FROM information_schema.columns
  WHERE table_catalog = source_catalog
    AND table_schema = source_schema
    AND table_name = source_table
);

SET VAR destination_city_col = (
  SELECT COALESCE(
    MAX(CASE WHEN lower(col_name) = 'destination_city' THEN col_name END),
    MAX(CASE WHEN lower(col_name) = 'dropoff_city' THEN col_name END),
    MAX(CASE WHEN lower(col_name) = 'do_city' THEN col_name END),
    MAX(CASE WHEN lower(col_name) = 'city' THEN col_name END)
  )
  FROM information_schema.columns
  WHERE table_catalog = source_catalog
    AND table_schema = source_schema
    AND table_name = source_table
);

SET VAR source_fq_table = concat(source_catalog, '.', source_schema, '.', source_table);
SET VAR origin_city_expr = CASE
  WHEN origin_city_col IS NULL THEN 'CAST(NULL AS STRING)'
  ELSE concat('CAST(src.', origin_city_col, ' AS STRING)')
END;
SET VAR destination_city_expr = CASE
  WHEN destination_city_col IS NULL THEN 'CAST(NULL AS STRING)'
  ELSE concat('CAST(src.', destination_city_col, ' AS STRING)')
END;

SELECT assert_true(
  trip_date_col IS NOT NULL
  AND origin_zip_col IS NOT NULL
  AND destination_zip_col IS NOT NULL
  AND fare_col IS NOT NULL,
  concat(
    'Could not detect required columns in ',
    source_fq_table,
    '. Run DESCRIBE TABLE ',
    source_fq_table,
    ' and update candidate column names in this script.'
  )
);

EXECUTE IMMEDIATE format_string(
  'CREATE OR REPLACE TABLE %s.fact_taxitrips_zip AS
   SELECT
     CAST(%s AS DATE) AS trip_date,
     TRIM(CAST(%s AS STRING)) AS trip_zip_origin,
     TRIM(CAST(%s AS STRING)) AS trip_zip_destination,
     CAST(%s AS DECIMAL(12,2)) AS fare
   FROM %s
   WHERE %s IS NOT NULL
     AND %s IS NOT NULL
     AND %s IS NOT NULL',
  target_schema,
  trip_date_col,
  origin_zip_col,
  destination_zip_col,
  fare_col,
  source_fq_table,
  origin_zip_col,
  destination_zip_col,
  fare_col
);

EXECUTE IMMEDIATE format_string(
  'CREATE OR REPLACE TABLE %s.dim_zipcode AS
   SELECT
     zip_code,
     any_value(city) AS city
   FROM (
     SELECT
       TRIM(CAST(src.%s AS STRING)) AS zip_code,
       COALESCE(%s, zl1.city) AS city
     FROM %s src
     LEFT JOIN %s.zip_city_lookup zl1
       ON zl1.zip_code = TRIM(CAST(src.%s AS STRING))
     UNION ALL
     SELECT
       TRIM(CAST(src.%s AS STRING)) AS zip_code,
       COALESCE(%s, zl2.city) AS city
     FROM %s src
     LEFT JOIN %s.zip_city_lookup zl2
       ON zl2.zip_code = TRIM(CAST(src.%s AS STRING))
   ) z
   WHERE zip_code IS NOT NULL AND zip_code <> ''''
   GROUP BY zip_code',
  target_schema,
  origin_zip_col,
  origin_city_expr,
  source_fq_table,
  target_schema,
  origin_zip_col,
  destination_zip_col,
  destination_city_expr,
  source_fq_table,
  target_schema,
  destination_zip_col
);

SELECT COUNT(*) AS fact_rows FROM taxi_dwh.gold.fact_taxitrips_zip;
SELECT COUNT(*) AS dim_rows FROM taxi_dwh.gold.dim_zipcode;
