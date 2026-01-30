import os

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    MapType,
    StructType,
    StringType,
    BooleanType,
    IntegerType,
    LongType,
    DoubleType,
    FloatType,
    DecimalType,
)


spark = (
    SparkSession.builder
    .appName("SilverToClickHouse")
    .config(
        "spark.jars.packages",
        "com.clickhouse:clickhouse-jdbc:0.4.6,"
        "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    )
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "s3a://telecomlakehouse/iceberg")
    .config("spark.hadoop.fs.s3a.endpoint", "https://expert-pancake-jv9wx6vww5w25gj4-4566.app.github.dev/")
    .config("spark.hadoop.fs.s3a.access.key", "test")
    .config("spark.hadoop.fs.s3a.secret.key", "test")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
    .getOrCreate()
)


clickhouse_url = os.getenv("CLICKHOUSE_URL")
clickhouse_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
clickhouse_port = os.getenv("CLICKHOUSE_PORT", "8123")
clickhouse_db = os.getenv("CLICKHOUSE_DB", "telecom_warehouse")
clickhouse_user = os.getenv("CLICKHOUSE_USER", "default")
clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse")
clickhouse_ssl = os.getenv("CLICKHOUSE_SSL")
clickhouse_compress = os.getenv("CLICKHOUSE_COMPRESS")

if not clickhouse_url:
    clickhouse_url = f"jdbc:clickhouse://{clickhouse_host}:{clickhouse_port}/{clickhouse_db}"

clickhouse_base_options = {
    "url": clickhouse_url,
    "user": clickhouse_user,
    "password": clickhouse_password,
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
}

if clickhouse_ssl:
    clickhouse_base_options["ssl"] = clickhouse_ssl

if clickhouse_compress is not None:
    clickhouse_base_options["compress"] = clickhouse_compress


def write_table_to_clickhouse(source_table, target_table):
    df = spark.read.table(f"local.{source_table}")
    if "is_rejected" in df.columns:
        df = df.filter("is_rejected = false")
    elif "rejection_reason" in df.columns:
        df = df.filter("rejection_reason IS NULL")

    # Drop flags that are null after filtering and may be non-nullable in ClickHouse
    for col_name in ("rejection_reason", "is_rejected"):
        if col_name in df.columns:
            df = df.drop(col_name)

    # Drop complex types (struct/array/map) that JDBC can't serialize
    complex_cols = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, (StructType, ArrayType, MapType))
    ]
    if complex_cols:
        df = df.drop(*complex_cols)

    # Fill nulls for basic types to satisfy non-nullable ClickHouse columns
    fill_values = {}
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            fill_values[field.name] = ""
        elif isinstance(field.dataType, BooleanType):
            fill_values[field.name] = False
        elif isinstance(field.dataType, (IntegerType, LongType, DoubleType, FloatType, DecimalType)):
            fill_values[field.name] = 0
    if fill_values:
        df = df.fillna(fill_values)

    df.write.format("jdbc") \
        .options(**clickhouse_base_options) \
        .option("dbtable", target_table) \
        .mode("append") \
        .save()


table_map = {
    "calls": "silver_calls",
    "sms": "silver_sms",
    "payment": "silver_payment",
    "recharge": "silver_recharge",
    "support": "silver_support",
}

for source, target in table_map.items():
    write_table_to_clickhouse(source, target)