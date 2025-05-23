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

# DAG 개수에 따른 스케줄링 비용
# dag 관리
# 파이프라인 요청 -> 파이프라인 스케줄 확인 -> DAG 작성 -> 배포

# 다양한 rdbms 통합 필요성 -> object storage 

# API KEY, Secret code 하드코딩 주의 secret 저장소 사용


#정상적으로 스케줄 되지 않은 DAG를 모아서 슬랙채널 알림 - socar 블로그
    
# 데이터 소스가 서비스 DB인 경우, 부하를 주지 않으면서 빠르게 데이터를 가져오려면 어떻게 해야 할까?

# gcp 빅쿼리 사용시 자연스러운 gcp 서비스 사용 나아가 -> GKE

# sol -> cdc 대규모 쿼리 진행시 expaln 사용 

# glue data catalog : meta store

# -> BI

