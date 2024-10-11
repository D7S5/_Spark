from airflow import DAG
import pendulum
import datetime
from airflow.operators.email import EmailOperator

AIRFLOW_SMTP_SMTP_HOST:"smtp.gmail.com"
AIRFLOW_SMTP_SMTP_USER:"gmail"
AIRFLOW_SMTP_SMTP_PASSWORD:"password"
AIRFLOW_SMTP_SMTP_PORT:"587"
AIRFLOW_SMTP_SMTP_MAIL_FORM:"gmail"

with DAG(
    dag_id="dags_email_operator",
    schedule="0 8 1 * * ",
    start_date = pendulum.datetime(2023.8,1, tz="UTC")
    catchup = False
) as dag:

send_email_task = EmailOperator(
    task_id = 'send_email_task',
    to = "email",
    subject = " ",
    html_content = ""
)



