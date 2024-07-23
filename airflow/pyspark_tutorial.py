from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'austin',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['doublehoon2@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Pyspark',
    default_args=default_args,
    description='pyspark ',
    schedule_interval='@once',
    catchup=False,
)

cmd = 'python3 read_csv.py /home/austin/_dataset/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*'

task_start = EmptyOperator(
    task_id= 'start',
    dag=dag
)

task_next = EmptyOperator(
    task_id='next',
    trigger_rule='all_success',
    dag=dag
)

pyspark = BashOperator(
    task_id= 'spark_submit_task',
    dag='pyspark-submit',
    bash_command=cmd
)

task_start >> task_next >> pyspark