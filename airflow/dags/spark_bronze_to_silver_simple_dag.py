"""
Airflow DAG to schedule Spark job for transforming data from Bronze to Silver layer.
Simplified version using BashOperator for easier setup.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
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
    'spark_bronze_to_silver_simple',
    default_args=default_args,
    description='Transform telecom data from Bronze to Silver layer using Spark (Simple)',
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

# Task 2: Run Spark job using spark-submit
spark_bronze_to_silver = BashOperator(
    task_id='spark_bronze_to_silver_transformation',
    bash_command="""
    /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --driver-memory 1g \
        --driver-cores 1 \
        --executor-memory 2g \
        --executor-cores 2 \
        --num-executors 2 \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        --py-files /opt/airflow/spark-shared/init.py,/opt/airflow/spark-shared/transformations.py,/opt/airflow/spark-shared/scheam.py \
        /opt/airflow/spark-shared/from_bronze_to_silver.py
    """,
    dag=dag,
)

# Task 3: Verify the transformation completed successfully
verify_transformation = BashOperator(
    task_id='verify_transformation',
    bash_command='echo "Transformation completed successfully at $(date)"',
    dag=dag,
)

# Define task dependencies
check_spark_master >> spark_bronze_to_silver >> verify_transformation

