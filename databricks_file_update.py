"""Databricks operator copying data to a file location"""

import os
from datetime import datetime

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks_sql import DatabricksCopyIntoOperator


from airflow.decorators import dag

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "databricks_operator"

dag = DAG(
    DAG_ID,
    default_args={"retries": 1},
    tags=["simple"],
    start_date=datetime(2023, 8, 4),
    catchup=False,
)

dag.doc_md = __doc__

test_bash = BashOperator(
        task_id="run_after_loop",
        bash_command='echo "mic test mic test"',
    )

copy_into_databricks = DatabricksCopyIntoOperator(
    task_id="databricks_copy_into",
    databricks_conn_id="databricks_conn_id",
    table_name="vorel",
    file_format="CSV",
    file_location="vorel",
    dag=dag
)

test_bash >> copy_into_databricks