from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Spark submit command configuration
# Note: Using docker exec to run spark-submit in spark-master container
# The spark-shared volume is mounted at /opt/spark/shared in Spark containers
spark_submit_command = (
    "docker exec spark-master /opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
    "--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262 "
    "--conf spark.hadoop.fs.s3a.endpoint=https://expert-pancake-jv9wx6vww5w25gj4-4566.app.github.dev/ "
    "--conf spark.hadoop.fs.s3a.access.key=test "
    "--conf spark.hadoop.fs.s3a.secret.key=test "
    "--conf spark.hadoop.fs.s3a.path.style.access=true "
    "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
    "--conf spark.hadoop.fs.s3a.connection.ssl.enabled=true "
    "--conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog "
    "--conf spark.sql.catalog.local.type=hadoop "
    "--conf spark.sql.catalog.local.warehouse=s3a://telecomlakehouse/iceberg "
    "--driver-memory 1g "
    "--executor-memory 2g "
    "--executor-cores 2 "
    "--num-executors 2 "
    "/opt/spark/shared/from_bronze_to_silver.py"
)

# Default arguments for the DAG
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": days_ago(1),
}

# DAG definition
dag = DAG(
    "spark_bronze_to_silver_simple",
    default_args=default_args,
    description="Transform telecom data from Bronze to Silver layer using Spark",
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=["spark", "etl", "bronze", "silver", "telecom"],
)

# Task 1: Check if Spark Master container is running
check_spark_master = BashOperator(
    task_id="check_spark_master",
    bash_command="docker ps | grep spark-master || exit 1",
    dag=dag,
)

# Task 2: Run Spark job using spark-submit in spark-master container
spark_bronze_to_silver = BashOperator(
    task_id="transform_bronze_to_silver",
    bash_command=spark_submit_command,
    dag=dag,
)

# Task 3: Verify transformation completed
verify_transformation = BashOperator(
    task_id="verify_transformation",
    bash_command='echo "Transformation job completed at $(date)"',
    dag=dag,
)

# Define task dependencies
check_spark_master >> spark_bronze_to_silver >> verify_transformation