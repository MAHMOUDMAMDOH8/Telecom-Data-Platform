"""
Airflow DAG to schedule Spark job for transforming data from Bronze to Silver layer.
This DAG runs the Spark transformation job that processes telecom events.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


# Default arguments for the DAG
comand_config = ( 
"spark-submit "
        "--master spark://spark-master:7077 "
        "--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
        "--conf spark.hadoop.fs.s3a.endpoint=https://expert-pancake-jv9wx6vww5w25gj4-4566.app.github.dev/ "
        "--conf spark.hadoop.fs.s3a.access.key=test "
        "--conf spark.hadoop.fs.s3a.secret.key=test "
        "--conf spark.hadoop.fs.s3a.path.style.access=true "
        "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
        "--conf spark.hadoop.fs.s3a.connection.ssl.enabled=true "
        "/opt/airflow/spark-shared/from_bronze_to_silver.py"
)

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

# DAG definition
dag = DAG(
    'spark_bronze_to_silver_simple',
    default_args=default_args,
    description='Transform telecom data from Bronze to Silver layer using Spark (Simple)',
    schedule_interval=timedelta(hours=1),  # Run every hour
    catchup=False,
    tags=['spark', 'etl', 'bronze', 'silver', 'telecom'],
)


# Task 2: Run Spark job using spark-submit
spark_bronze_to_silver = BashOperator(
        task_id = "transform_bronze_to_silver",
        bash_command = comand_config,
        dag=dag
    )



spark_bronze_to_silver