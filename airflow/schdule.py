from airflow import DAG
import datetime as dt
from airflow.operators.bash import BashOperator


dag=DAG(
    dag_id="time_delta",
    schedule_interval=dt.timedelta(days=3),
    start_date=dt.datetime(year=2024, month=6, day=12),
    end_date=dt.datetime(year=2024, month=6, day=22)
)

# curl -O http://localhost:5000/events?start_date=2024-06-12&end_date=2024-06-13
fetch_event=BashOperator(
    task_id="fetch_event",
    bash_command=(
    "mkdir -p /data && "
    "curl -o /data/events.json "
    "http://localhost:5000/events?"
    "start_date=2024-06-12&"
    "end_date=2024-06-13"
),
dag=dag,
)

fetch_event=BashOperator(
    task_id="fetch_event",
    bash_command=(
    "mkdir -p /data && "
    "curl -o /data/events.json "
    "http://localhost:5000/events?"
    "start_date={{execution_date.strftime('%Y-%m-%d')}}"
    "end_date={{next_execution_dae.strftime('%Y-%m%d)}}"
),
dag=dag,
)
