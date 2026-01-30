from pyspark.sql import SparkSession

def get_spark_session(app_name="DataLakehouseModeling"):
    spark = (
        SparkSession.builder
        .appName("icebergToClickHouse")
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
    return spark


def clickhouse_conne(clickhouse_url,clickhouse_host,clickhouse_port,clickhouse_db,clickhouse_user,clickhouse_password,clickhouse_ssl,clickhouse_compress):
    """
    take clickhouse credentials and return connection 
    """
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
    
    return clickhouse_base_options

def write_table_to_clickhouse(df, target_table, clickhouse_base_options):
    """
    write table to clickhouse
    """
    df.write.format("jdbc") \
        .options(**clickhouse_base_options) \
        .option("dbtable", target_table) \
        .mode("append") \
        .save()
    print("--------------------------------")
    print(f"Table {target_table} written to ClickHouse")