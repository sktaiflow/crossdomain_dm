from __future__ import annotations

from textwrap import dedent

import pendulum
from airflow import DAG
from airflow.providers.sktvane.operators.nes import NesOperator


with DAG(
    "dag_nes",
    default_args={"retries": 2},
    description="DAG with own plugins",
    schedule="00 10 * * *",
    start_date=pendulum.datetime(2023, 5, 17, tz="KST"),
    catchup=False,
    tags=["demo"],
) as dag:

    dag.doc_md = __doc__

    nes_task = NesOperator(
        task_id="NES_Task",
        input_nb="https://github.com/sktaiflow/crossdomain_dm/blob/main/cross_DM_OS_Expansion_BQ_1cell.ipynb",
    )
    nes_task.doc_md = dedent(
        """\
    #### NES task
    NES를 활용한 cross DM daily update
    """
    )
