from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

with DAG(
    dag_id="crossdomain_dm_dag",
    schedule_interval=None,
    default_args={
        "owner": "OWNER",
        "depends_on_past": False,
        "start_date": datetime(2023, 5, 18),
        "retries": 0,
    },
    catchup=False,
) as dag:
    task = DummyOperator(task_id="task")
