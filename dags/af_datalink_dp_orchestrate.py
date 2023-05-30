from airflow import DAG
from airflow import settings
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.utils.db import provide_session
from airflow.models import Connection, XCom
from airflow.models.param import Param
from airflow.models.xcom_arg import XComArg
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import (
    PythonOperator,
    BranchPythonOperator,
    get_current_context,
)
from datetime import datetime

default_args = {
    "owner": "Airflow User",
    "start_date": datetime(2022, 2, 7),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    dag_id='af_datalink_dp_orchestrate',
    schedule=None,
    default_args=default_args,
    params={
        "customer_id": Param(default=125, type=["integer", "string"], min=1, max=255),
        "state_code": Param(default='VA', type=["string"]),
        "year": Param(default=2023, type=["integer", "string"]),
        "month": Param(default=5 , type=["integer", "string"]),
        "container_name": Param(default="cont-datalink-dp-shared", type=["string"]),
        "root_folder": Param(default="INPUT", type=["string"]),
        "database": Param(default="DEV_OPS_DB", type=["string"]),
        "schema": Param(default="CONFIG", type=["string"]),
        "run_failed": Param(default=True)
    },
) as dag:


    af_sftp_to_adls = TriggerDagRunOperator(
        task_id="af_sftp_to_adls",
        trigger_dag_id="af_sftp_to_adls",
        wait_for_completion=True,
        conf={
            "state_code": """{{params.state_code}}""",
            "year": """{{params.year}}""",
            "month": """{{params.month}}""",
            "customer_id": """{{params.customer_id}}"""
        },
    )

    af_adls_to_snowflake = TriggerDagRunOperator(
        task_id="af_adls_to_snowflake",
        trigger_dag_id="af_adls_to_snowflake",
        wait_for_completion=True,
        conf={
            "customer_id": """{{params.customer_id}}""",
            "container_name": """{{params.container_name}}""",
            "root_folder": """{{params.root_folder}}""",
            "database": """{{params.database}}""",
            "schema": """{{params.schema}}""",
            "run_failed": """{{params.run_failed}}""",
            "state_code": """{{params.state_code}}""",
        },
    )


af_sftp_to_adls >> af_adls_to_snowflake