import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator


default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2000, 1, 1),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}
    
dag = DAG('employee_data',
            default_args=default_args,
            description='Runs an external Python script',
            schedule_interval='@daily',
            catchup=False)
    

with dag:
    run_script_task = BashOperator(
        task_id='extract_data',
        bash_command='python /home/airflow/gcs/dags/scripts/extract.py',
        )
    
    start_pipeline = CloudDataFusionStartPipelineOperator(
    location="us-central1",
    pipeline_name='emp-etl-pipeline',
    instance_name='datafusion-dev',
    task_id="start_datafusion_pipeline",
    )
    
    run_script_task >> start_pipeline