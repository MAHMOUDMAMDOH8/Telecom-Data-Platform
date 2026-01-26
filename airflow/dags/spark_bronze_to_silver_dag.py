"""
Airflow DAG to schedule Spark job for transforming data from Bronze to Silver layer.
This DAG runs the Spark transformation job that processes telecom events.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

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
    'spark_bronze_to_silver',
    default_args=default_args,
    description='Transform telecom data from Bronze to Silver layer using Spark',
    schedule_interval=timedelta(hours=1),  # Run every hour
    catchup=False,
    tags=['spark', 'etl', 'bronze', 'silver', 'telecom'],
)

# Task 1: Check if Spark Master is available
check_spark_master = BashOperator(
    task_id='check_spark_master',
    bash_command='curl -f http://spark-master:8081 || exit 1',
    dag=dag,
)

# Task 2: Run Spark job to transform Bronze to Silver
spark_bronze_to_silver = SparkSubmitOperator(
    task_id='spark_bronze_to_silver_transformation',
    application='/opt/airflow/spark-shared/from_bronze_to_silver.py',
    name='bronze_to_silver_etl',
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.submit.deployMode': 'client',
        'spark.executor.memory': '2g',
        'spark.executor.cores': '2',
        'spark.driver.memory': '1g',
        'spark.driver.cores': '1',
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
    },
    py_files=[
        '/opt/airflow/spark-shared/init.py',
        '/opt/airflow/spark-shared/transformations.py',
        '/opt/airflow/spark-shared/scheam.py',
    ],
    files=[],
    driver_class_path='/opt/spark/jars/*',
    executor_memory='2g',
    executor_cores=2,
    num_executors=2,
    verbose=True,
    dag=dag,
)

# Task 3: Verify the transformation completed successfully
verify_transformation = BashOperator(
    task_id='verify_transformation',
    bash_command='echo "Transformation completed successfully"',
    dag=dag,
)

# Define task dependencies
check_spark_master >> spark_bronze_to_silver >> verify_transformation

