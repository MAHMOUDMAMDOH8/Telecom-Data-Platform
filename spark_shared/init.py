from datetime import datetime
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import input_file_name, regexp_extract


def get_spark_session(
    app_name="ETL-Iceberg",
    s3_endpoint="https://expert-pancake-jv9wx6vww5w25gj4-4566.app.github.dev",
    access_key="test",
    secret_key="test"
):
    spark_jars_packages = [
        "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ]

    conf = (
        SparkConf()
        .setAppName(app_name)
        .set("spark.jars.packages", ",".join(spark_jars_packages))
        .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .set("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .set("spark.hadoop.fs.s3a.access.key", access_key)
        .set("spark.hadoop.fs.s3a.secret.key", secret_key)
        .set("spark.hadoop.fs.s3a.path.style.access", "true")
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        .set("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .set("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        .set("spark.hadoop.fs.s3a.attempts.maximum", "5")
        .set("spark.hadoop.fs.s3a.retry.limit", "5")
        .set("spark.hadoop.fs.s3a.retry.interval", "2000")
        .set("spark.hadoop.fs.s3a.threads.max", "50")
        .set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.local.type", "hadoop")
        .set("spark.sql.catalog.local.warehouse", "s3a://telecomlakehouse/iceberg")
    )

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark



def read_bronze_table(
    spark: SparkSession,
    table_name: str,
    bucket: str = "telecomlakehouse",
    base_layer: str = "bronze_layer"
):
    s3_path = f"s3a://{bucket}/{base_layer}/{table_name}/"

    return (
        spark.read
        .option("multiLine", "true")
        .option("recursiveFileLookup", "true")
        .json(s3_path)
        .withColumn("source_file", input_file_name())
        .withColumn(
            "event_date",
            regexp_extract("source_file", r"/(\d{4}-\d{2}-\d{2})/", 1)
        )
        .withColumn(
            "event_hour",
            regexp_extract(
                "source_file",
                r"/\d{4}-\d{2}-\d{2}/(\d{2})/",
                1
            )
        )
    )



def read_from_iceberg(table_name, spark):

    if spark is None:
        # Try to get active SparkSession
        try:
            spark = SparkSession.getActiveSession()
            if spark is None:
                spark = get_spark_session()
        except:
            spark = get_spark_session()
    
    full_table = f"local.{table_name}"
    
    try:
        if not spark.catalog.tableExists(full_table):
            print(f"Table {full_table} does not exist.")
            return None
        
        print(f"Reading data from Iceberg table: {full_table}")
        df = spark.table(full_table)
        count = df.count()
        print(f"Successfully read {count} records from {full_table}")
        
        return df
        
    except Exception as e:
        print(f"Error reading data from Iceberg table: {e}")
        raise


def write_to_iceberg(df, table_name, mode="append" , output_location="s3a://telecomlakehouse/iceberg"):
    spark = df.sparkSession
    full_table = f"local.{table_name}"

    if not spark.catalog.tableExists(full_table):
        df.writeTo(full_table).create()
    else:
        if mode == "overwrite":
            df.writeTo(full_table).overwritePartitions()
        else:
            df.writeTo(full_table).append()


def move_to_archive(df, table_name):
    spark = df.sparkSession
    now = datetime.now().strftime("%Y%m%d%H")
    new_table = f"local.{table_name}_{now}"

    if not spark.catalog.tableExists(new_table):
        print(f"Table {new_table} does not exist. Creating it...")
        df.writeTo(new_table).create()
    else:
        print(f"Appending to existing Iceberg table: {new_table}")
        df.writeTo(new_table).append()

    return df

def delete_raws_in_bronze(
    table_name,
    date=None,
    hour=None,
    bronze_bucket="s3a://telecomlakehouse",
    spark=None
):
    if spark is None:
        spark = SparkSession.getActiveSession() or get_spark_session()

    base_path = f"{bronze_bucket}/bronze_layer/{table_name}"

    if date:
        base_path += f"/{date}"
        if hour:
            base_path += f"/{hour}"

    fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark.sparkContext._jsc.hadoopConfiguration()
    )

    path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(base_path)

    if not fs.exists(path):
        print(f"No files found at {base_path}")
        return 0

    deleted = fs.delete(path, True)  # recursive
    print(f"Deleted bronze data at {base_path}")
    return deleted

