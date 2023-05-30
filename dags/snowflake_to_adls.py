from airflow import DAG
from airflow.operators.python import PythonOperator, ExternalPythonOperator
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

ENV_ID = "DEV"
DAG_ID = "af_snowflake_to_adls"

def upload_snowflake_adls(**context):
    import sys
    sys.path.append('/usr/local/airflow/include/scripts')
    import snowflake_adls_upload
    snowflake_adls_upload.process(context)

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="A DAG to process data stored in Snowflake to Azure Datalake Storage",
    schedule_interval=timedelta(days=30),
    catchup=False,
    params={
        "state_code": Param(default='VA', type=["string"]),
        "year": Param(default=2023, type=["integer"]),
        "month": Param(default=5 , type=["integer"]),
        "customer_id": Param(default=125, type=["integer"])
    },
) as dag:

    copy_from_snowflake_adls = ExternalPythonOperator(
        provide_context=True,
        task_id="copy_from_snowflake_adls",
        python_callable=upload_snowflake_adls,
        python="/home/astro/.pyenv/versions/snowpark_env/bin/python",
        op_kwargs={
            "params": {
                "state_code": """{{params.state_code}}""",
                "year": """{{params.year}}""",
                "month": """{{params.month}}""",
                "customer_id": """{{params.customer_id}}"""
            }
        },
    )

    copy_from_snowflake_adls