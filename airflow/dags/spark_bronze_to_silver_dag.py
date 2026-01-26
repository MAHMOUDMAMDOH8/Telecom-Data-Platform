

import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def run_spark_transformation(**context):
    # Add spark-shared directory to Python path
    spark_shared_path = '/opt/airflow/spark-shared'
    if spark_shared_path not in sys.path:
        sys.path.insert(0, spark_shared_path)
    
    # Change to the spark-shared directory so imports work correctly
    original_cwd = os.getcwd()
    os.chdir(spark_shared_path)
    
    try:
        # Import and execute the transformation
        # The script will create a Spark session that connects to spark://spark-master:7077
        from from_bronze_to_silver import process_all_transformations
        
        print("=" * 60)
        print("Starting Spark transformation...")
        print(f"Connecting to Spark cluster at spark://spark-master:7077")
        print("=" * 60)
        
        process_all_transformations()
        
        print("=" * 60)
        print("Spark transformation completed successfully!")
        print("=" * 60)
        
        return "Success"
    except Exception as e:
        print(f"Error running Spark transformation: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        os.chdir(original_cwd)

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


# Task 1: Check if Spark Master is accessible
check_spark_master = BashOperator(
    task_id='check_spark_master',
    bash_command='curl -f http://spark-master:8081 || exit 1',
    dag=dag,
)

# Task 2: Run Spark transformation using Python operator
# This connects to the remote Spark cluster at spark://spark-master:7077
spark_bronze_to_silver = PythonOperator(
    task_id="transform_bronze_to_silver",
    python_callable=run_spark_transformation,
    dag=dag
)

# Task 3: Verify transformation completed
verify_transformation = BashOperator(
    task_id='verify_transformation',
    bash_command='echo "Transformation job completed at $(date)"',
    dag=dag,
)

# Define task dependencies
check_spark_master >> spark_bronze_to_silver >> verify_transformation