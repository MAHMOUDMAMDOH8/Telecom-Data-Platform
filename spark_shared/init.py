from datetime import datetime
from pyspark.sql import SparkSession
from pyspark import SparkConf


def get_spark_session(app_name="ETL-Iceberg", s3_endpoint="http://localhost:4566",
                      s3_bucket="s3a://telecom_lakehouse/bronze_layer", access_key="test", secret_key="test"):

    spark_jars_packages = [
        "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ]

    conf = (
        SparkConf()
        .setAppName(app_name)
        .set("spark.jars.packages", ",".join(spark_jars_packages))
        .set("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .set("spark.hadoop.fs.s3a.access.key", access_key)
        .set("spark.hadoop.fs.s3a.secret.key", secret_key)
        .set("spark.hadoop.fs.s3a.path.style.access", "true")
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        .set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.local.type", "hadoop")
        .set("spark.sql.catalog.local.warehouse", s3_bucket)
    )

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark



def read_from_bronze(table_name, schema, date=None, hour=None, bronze_bucket="s3a://telecom_lakehouse", spark=None, multi_line=True):
    """
    Read JSON data from bronze layer.
    
    Args:
        table_name: Name of the table (e.g., 'payment', 'call', 'sms', 'recharge', 'support')
        date: Optional date in format 'YYYY-MM-DD'. If None, reads all dates.
        hour: Optional hour in format 'HH'. If None, reads all hours for the given date.
        bronze_bucket: S3 bucket path for bronze layer (default: s3a://telecom_lakehouse)
        spark: Optional SparkSession. If None, will get from active session.
        multi_line: If True, reads multi-line JSON files. If False, reads single-line JSON (default: True)
    
    Returns:
        DataFrame with the read data
    """
    if spark is None:
        # Try to get active SparkSession
        try:
            spark = SparkSession.getActiveSession()
            if spark is None:
                spark = get_spark_session()
        except:
            spark = get_spark_session()
    
    # Build the path pattern
    base_path = f"{bronze_bucket}/bronze_layer/{table_name}"
    
    if date:
        base_path = f"{base_path}/{date}"
        if hour:
            base_path = f"{base_path}/{hour}"
    
    # Add wildcard for JSON files
    path_pattern = f"{base_path}/*.json"
    
    try:
        print(f"Reading data from: {path_pattern}")
        
        # Read JSON files
        if multi_line:
            # For multi-line JSON files (each file contains one JSON object)
            df = spark.read.option("multiLine", "true").option("mode", "PERMISSIVE").json(path_pattern)
        else:
            # For single-line JSON files (one JSON object per line)
            df = spark.read.option("mode", "PERMISSIVE").json(path_pattern)
        
        df = df.select(*schema)
        
        count = df.count()
        print(f"Successfully read {count} records from {path_pattern}")
        
        return df
        
    except Exception as e:
        print(f"Error reading data from bronze layer: {e}")
        # Return empty DataFrame with schema if path doesn't exist
        try:
            # Try to read a sample to get schema
            sample_path = f"{base_path}/*.json"
            df = spark.read.option("multiLine", "true").option("mode", "PERMISSIVE").json(sample_path)
            df = df.select(*schema)
            return spark.createDataFrame([], schema)
        except:
            raise


def read_from_iceberg(table_name, spark):
    """
    Read data from Iceberg table.
    
    Args:
        table_name: Name of the Iceberg table (without catalog prefix)
        spark: Optional SparkSession. If None, will get from active session.
    
    Returns:
        DataFrame with the read data
    """
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


def write_to_iceberg(df, table_name):
    spark = df.sparkSession
    full_table = f"local.{table_name}"

    if not spark.catalog.tableExists(full_table):
        print(f"Table {full_table} does not exist. Creating it...")
        df.writeTo(full_table).create()
    else:
        print(f"Appending to existing Iceberg table: {full_table}")
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

def delete_raws_in_bronze(table_name, date=None, hour=None, bronze_bucket="s3a://telecom_lakehouse", spark=None):
    """
    Delete processed raw JSON files from bronze layer.
    
    Args:
        table_name: Name of the table (e.g., 'payment', 'call', 'sms')
        date: Optional date in format 'YYYY-MM-DD'. If None, deletes all dates.
        hour: Optional hour in format 'HH'. If None, deletes all hours for the given date.
        bronze_bucket: S3 bucket path for bronze layer (default: s3a://telecom_lakehouse)
        spark: Optional SparkSession. If None, will get from active session.
    
    Returns:
        Number of files deleted
    """
    if spark is None:
        # Try to get active SparkSession
        try:
            spark = SparkSession.getActiveSession()
            if spark is None:
                spark = get_spark_session()
        except:
            spark = get_spark_session()
    
    # Build the path pattern
    base_path = f"{bronze_bucket}/bronze_layer/{table_name}"
    
    if date:
        base_path = f"{base_path}/{date}"
        if hour:
            base_path = f"{base_path}/{hour}"
    
    # Add wildcard for JSON files
    path_pattern = f"{base_path}/*.json"
    
    try:
        # Get filesystem
        fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark.sparkContext._jsc.hadoopConfiguration()
        )
        
        # Convert path to Hadoop Path
        hadoop_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(base_path)
        
        # Check if path exists
        if not fs.exists(hadoop_path):
            print(f"Path {base_path} does not exist. Nothing to delete.")
            return 0
        
        # List all files in the directory
        file_statuses = fs.listStatus(hadoop_path)
        deleted_count = 0
        
        for file_status in file_statuses:
            if file_status.isFile():
                file_path = file_status.getPath()
                file_name = file_path.getName()
                
                # Only delete JSON files
                if file_name.endswith('.json'):
                    try:
                        fs.delete(file_path, False)  # False = not recursive
                        deleted_count += 1
                        print(f"Deleted: {file_path}")
                    except Exception as e:
                        print(f"Error deleting {file_path}: {e}")
        
        print(f"Successfully deleted {deleted_count} files from {base_path}")
        return deleted_count
        
    except Exception as e:
        print(f"Error deleting files from bronze layer: {e}")
        raise
