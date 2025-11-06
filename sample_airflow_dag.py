"""
Sample Apache Airflow DAG
This DAG demonstrates common Airflow patterns and operators.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Python functions for PythonOperator
def extract_data():
    """Extract data from source"""
    print("Extracting data from source...")
    return {"records": 100, "status": "success"}


def transform_data(**context):
    """Transform extracted data"""
    print("Transforming data...")
    # Pull data from previous task using XCom
    ti = context['ti']
    extracted_data = ti.xcom_pull(task_ids='extract')
    print(f"Received data: {extracted_data}")
    transformed_records = extracted_data['records'] * 2
    return {"transformed_records": transformed_records}


def load_data(**context):
    """Load data to destination"""
    print("Loading data to destination...")
    ti = context['ti']
    transformed_data = ti.xcom_pull(task_ids='transform')
    print(f"Loading {transformed_data['transformed_records']} records")
    return "Load completed successfully"


# Define the DAG
with DAG(
    dag_id='sample_etl_pipeline',
    default_args=default_args,
    description='A sample ETL pipeline demonstrating Airflow capabilities',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    tags=['example', 'etl', 'sample'],
) as dag:

    # Start task
    start = DummyOperator(
        task_id='start',
    )

    # Check prerequisites using BashOperator
    check_prerequisites = BashOperator(
        task_id='check_prerequisites',
        bash_command='echo "Checking prerequisites..." && sleep 2 && echo "Prerequisites OK"',
    )

    # Extract task
    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
    )

    # Transform task
    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
        provide_context=True,
    )

    # Load task
    load = PythonOperator(
        task_id='load',
        python_callable=load_data,
        provide_context=True,
    )

    # Data quality check
    quality_check = BashOperator(
        task_id='quality_check',
        bash_command='echo "Running quality checks..." && sleep 1 && echo "Quality checks passed"',
    )

    # Cleanup task
    cleanup = BashOperator(
        task_id='cleanup',
        bash_command='echo "Cleaning up temporary files..."',
    )

    # End task
    end = DummyOperator(
        task_id='end',
    )

    # Define task dependencies
    # Method 1: Using >> operator
    start >> check_prerequisites >> extract >> transform >> load >> quality_check

    # Method 2: Using set_downstream (same as above)
    quality_check >> cleanup >> end

    # Alternative way to define dependencies:
    # start.set_downstream(check_prerequisites)
    # check_prerequisites.set_downstream(extract)
    # etc.
