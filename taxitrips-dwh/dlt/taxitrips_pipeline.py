import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

SOURCE_PATH = spark.conf.get("taxi.source.path")
SOURCE_FORMAT = spark.conf.get("taxi.source.format", "csv")
SCHEMA_LOCATION = spark.conf.get("taxi.schema.location", "dbfs:/pipelines/taxi_dwh/schema")


@dlt.table(
    name="taxitrips_raw",
    comment="Raw taxi trip records ingested from cloud object storage",
    table_properties={"quality": "bronze"},
)
def taxitrips_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", SOURCE_FORMAT)
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("rescuedDataColumn", "_rescued_data")
        .option("header", "true")
        .load(SOURCE_PATH)
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
        .withColumn("load_date", F.to_date(F.current_timestamp()))
    )


@dlt.table(
    name="taxitrips_clean",
    comment="Validated and typed taxi trip records",
    table_properties={"quality": "silver"},
)
@dlt.expect("pickup_exists", "pickup_datetime IS NOT NULL")
@dlt.expect("dropoff_exists", "dropoff_datetime IS NOT NULL")
@dlt.expect("non_negative_total", "total_amount >= 0")
@dlt.expect("non_negative_fare", "fare_amount >= 0")
@dlt.expect("dropoff_after_pickup", "dropoff_datetime >= pickup_datetime")
def taxitrips_clean():
    raw = dlt.read_stream("taxitrips_raw")

    pickup_col = F.coalesce(F.col("tpep_pickup_datetime"), F.col("lpep_pickup_datetime"))
    dropoff_col = F.coalesce(F.col("tpep_dropoff_datetime"), F.col("lpep_dropoff_datetime"))

    typed = (
        raw.withColumn("vendor_id", F.col("VendorID").cast(IntegerType()))
        .withColumn("pickup_datetime", F.to_timestamp(pickup_col))
        .withColumn("dropoff_datetime", F.to_timestamp(dropoff_col))
        .withColumn("passenger_count", F.col("passenger_count").cast(IntegerType()))
        .withColumn("trip_distance", F.col("trip_distance").cast(DoubleType()))
        .withColumn("ratecode_id", F.col("RatecodeID").cast(IntegerType()))
        .withColumn("pickup_location_id", F.col("PULocationID").cast(IntegerType()))
        .withColumn("dropoff_location_id", F.col("DOLocationID").cast(IntegerType()))
        .withColumn("payment_type", F.col("payment_type").cast(IntegerType()))
        .withColumn("fare_amount", F.col("fare_amount").cast(DoubleType()))
        .withColumn("extra", F.col("extra").cast(DoubleType()))
        .withColumn("mta_tax", F.col("mta_tax").cast(DoubleType()))
        .withColumn("tip_amount", F.col("tip_amount").cast(DoubleType()))
        .withColumn("tolls_amount", F.col("tolls_amount").cast(DoubleType()))
        .withColumn("improvement_surcharge", F.col("improvement_surcharge").cast(DoubleType()))
        .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
        .withColumn("congestion_surcharge", F.col("congestion_surcharge").cast(DoubleType()))
        .withColumn("airport_fee", F.coalesce(F.col("Airport_fee"), F.col("airport_fee")).cast(DoubleType()))
    )

    return (
        typed.dropDuplicates(
            [
                "vendor_id",
                "pickup_datetime",
                "dropoff_datetime",
                "pickup_location_id",
                "dropoff_location_id",
                "total_amount",
            ]
        )
        .withColumn("trip_date", F.to_date("pickup_datetime"))
        .withColumn("trip_hour", F.hour("pickup_datetime"))
        .withColumn(
            "trip_duration_min",
            (F.col("dropoff_datetime").cast("long") - F.col("pickup_datetime").cast("long")) / 60.0,
        )
        .withColumn(
            "trip_distance_bucket",
            F.when(F.col("trip_distance") < 1, "0-1")
            .when((F.col("trip_distance") >= 1) & (F.col("trip_distance") < 3), "1-3")
            .when((F.col("trip_distance") >= 3) & (F.col("trip_distance") < 7), "3-7")
            .otherwise("7+"),
        )
    )


@dlt.table(
    name="fact_taxitrips",
    comment="Analytics-ready taxi trips fact table",
    partition_cols=["trip_date"],
    table_properties={"quality": "gold"},
)
def fact_taxitrips():
    silver = dlt.read("taxitrips_clean")

    return silver.select(
        F.sha2(
            F.concat_ws(
                "|",
                F.col("vendor_id").cast("string"),
                F.col("pickup_datetime").cast("string"),
                F.col("dropoff_datetime").cast("string"),
                F.col("pickup_location_id").cast("string"),
                F.col("dropoff_location_id").cast("string"),
                F.col("total_amount").cast("string"),
            ),
            256,
        ).alias("trip_id"),
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        "trip_date",
        "trip_hour",
        "trip_duration_min",
        "trip_distance_bucket",
        "pickup_location_id",
        "dropoff_location_id",
        "passenger_count",
        "trip_distance",
        "ratecode_id",
        "payment_type",
        "fare_amount",
        "tip_amount",
        "tolls_amount",
        "total_amount",
        "load_date",
    )


@dlt.table(name="dim_vendor", comment="Taxi vendor dimension", table_properties={"quality": "gold"})
def dim_vendor():
    return (
        dlt.read("taxitrips_clean")
        .select("vendor_id")
        .where(F.col("vendor_id").isNotNull())
        .dropDuplicates()
    )


@dlt.table(name="dim_ratecode", comment="Rate code dimension", table_properties={"quality": "gold"})
def dim_ratecode():
    return (
        dlt.read("taxitrips_clean")
        .select("ratecode_id")
        .where(F.col("ratecode_id").isNotNull())
        .dropDuplicates()
    )


@dlt.table(name="dim_payment_type", comment="Payment type dimension", table_properties={"quality": "gold"})
def dim_payment_type():
    return (
        dlt.read("taxitrips_clean")
        .select("payment_type")
        .where(F.col("payment_type").isNotNull())
        .dropDuplicates()
    )
