from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {"retries": 2, "retry_delay": timedelta(minutes=5)}

with DAG(dag_id="orders_etl", schedule="30 2 * * 1-5", start_date=datetime(2024, 1, 1),
         catchup=False, default_args=default_args) as dag:
    start = EmptyOperator(task_id="start")
    extract = BashOperator(task_id="extract", bash_command="python3 jobs/extract.py --date {{ ds }}")
    transform = BashOperator(task_id="transform", bash_command="spark-submit --master yarn jobs/transform.py")
    load = PostgresOperator(task_id="load", sql="INSERT INTO f SELECT * FROM stg WHERE d = CURRENT_DATE")
    notify = PythonOperator(task_id="notify", python_callable=send_mail)
    cleanup = BashOperator(task_id="cleanup", bash_command="rm -rf /tmp/stage")
    start >> extract >> transform >> [load, notify]
    load >> cleanup
