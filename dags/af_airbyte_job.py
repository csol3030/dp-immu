import os, fnmatch, time, uuid
import json
from datetime import datetime

from airflow import DAG
from airflow import settings
from airflow.utils.task_group import TaskGroup
from airflow import DAG
from airflow import settings
from airflow.utils.task_group import TaskGroup
from airflow.utils.db import provide_session
from airflow.models import Connection, XCom
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.providers.airbyte.hooks.airbyte import AirbyteHook
from azure.identity import AzureCliCredential
from azure.keyvault.secrets import SecretClient
from include.scripts import constant

KEYVAULT_AIRBYTE_SECRET = "ImmunizationAirbyteSecret"
KEYVAULT_SNOWFLAKE_SECRET = "ImmunizationSnowflakeSecret"


AIRBYTE_CONN_ID = "datalink_immunization_airbyte_conn1"
SNOWFLAKE_CONN_ID = "datalink_immunization_snowflake_conn1"

ENV_ID = "DEV"
DAG_ID = "af_immunization_trigger_airbyte_job"

default_args = {
    "owner": "Airflow User",
    "start_date": datetime(2022, 2, 7),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

session = settings.Session()

az_credential = AzureCliCredential()
kv_client = SecretClient(vault_url=constant.KEYVAULT_URI, credential=az_credential)


@provide_session
def cleanup_xcom(session=None, **context):
    dag = context["dag"]
    dag_id = dag._dag_id
    # It will delete all xcom of the dag_id
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()


def get_status(**context):
    airbyte_job_id = context["params"]["airbyte_job_id"]
    airbyte_job_run_id = context["airbyte_job_run_id"]
    print("message=====================", airbyte_job_run_id, airbyte_job_id)
    job_status = "IN PROGRESS"
    job_error_details = ""
    created_at = datetime.now()
    update_audit_table(airbyte_job_run_id, airbyte_job_id, job_status, job_error_details, created_at, context)
    try:
        job_sensor = AirbyteJobSensor(
            task_id="airbyte_sensor_snowflake_sqlserver",
            airbyte_conn_id=AIRBYTE_CONN_ID,
            airbyte_job_id=airbyte_job_run_id,
        ).execute(context=context)

        job_status = "SUCCESS"
        # job_error_details =

    except Exception as err:
        print("job_status===============", err)
        job_status = "FAILURE"
        job_error_details = str(err)
        # job_status = "SUCCESS"

    finally:
        update_audit_table(
            airbyte_job_run_id, airbyte_job_id, job_status, job_error_details, created_at, context
        )
        pass


def update_audit_table(airbyte_job_run_id, airbyte_job_id, job_status, job_error_details, created_at, context):
    print("update_audit_table=================================================",airbyte_job_run_id, airbyte_job_id, job_status, job_error_details)
    latest_run=None
    try:
        hook = AirbyteHook(airbyte_conn_id=AIRBYTE_CONN_ID)
        job = hook.get_job(job_id=airbyte_job_run_id)
        latest_run = job.json()["job"]
    except Exception as err:
        print(err)

    print("temp_run=========================", latest_run)

    # if latest_run["is_error"] == True and latest_run["is_complete"] == True:
    #     if latest_run["status_message"] != None:
    #         job_error_details = latest_run["status_message"]

    #     err_obj = {
    #         "error_message": job_error_details,
    #         "job_debug_url": latest_run["href"],
    #     }
    #     job_error_details = err_obj

    job_audit_details = {
        "file_ingestion_details_id":"",
        "job_run_id": str(airbyte_job_run_id),
        "job_id": airbyte_job_id,
        "job_run_duration": "",
        "status": job_status,
        "error_details": job_error_details,
        "start_date": datetime.fromtimestamp(latest_run["createdAt"]),
        "end_date": datetime.fromtimestamp(latest_run["updatedAt"])
    }
    print("job_audit_details=========================", job_audit_details)
    db = context["params"]["config_db"]
    schema = context["params"]["config_schema"]
    table = "AIRBYTE_JOB_DETAILS"
    sql_statement=""

    if job_status=="IN PROGRESS":
        sql_statement = """insert into {}.{}.{} (
            FILE_INGESTION_DETAILS_ID,
            JOB_RUN_ID,
            JOB_ID,
            JOB_RUN_DURATION,
            STATUS,
            ERROR_DETAILS,
            START_DATE
        ) values ('{}','{}','{}','{}','{}','{}','{}')
            """.format(
                db,schema,table,
                job_audit_details["file_ingestion_details_id"],
                job_audit_details["job_run_id"],
                job_audit_details["job_id"],
                job_audit_details["job_run_duration"],
                job_audit_details["status"],
                job_audit_details["error_details"],
                job_audit_details["start_date"]
            )
    else:
        job_audit_details["job_run_duration"] = latest_run["updatedAt"]-latest_run["createdAt"]
        sql_statement = """update {}.{}.{} set
                JOB_RUN_DURATION='{}', STATUS='{}', ERROR_DETAILS='{}', START_DATE='{}',END_DATE='{}'
                where JOB_RUN_ID='{}'
            """.format(
                db,schema,table,
                job_audit_details["job_run_duration"],
                job_audit_details["status"],
                json.dumps(job_audit_details["error_details"]),
                job_audit_details["start_date"],
                job_audit_details["end_date"],
                job_audit_details["job_run_id"]
            )

    print("sql_statement===========================",sql_statement)

    snf_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    snf_hook.run(sql=sql_statement)


def get_kv_secret(secret_name):
    fetched_secret = kv_client.get_secret(secret_name)
    # print(fetched_secret.value)
    return fetched_secret.value


def create_connection():
    airbyte_details = json.loads(get_kv_secret(KEYVAULT_AIRBYTE_SECRET))
    snowflake_details = json.loads(get_kv_secret(KEYVAULT_SNOWFLAKE_SECRET))

    snowflake_conn = Connection(
        conn_id=SNOWFLAKE_CONN_ID,
        conn_type="snowflake",
        login=snowflake_details["username"],
        password=snowflake_details["password"],
        extra=json.dumps(
            {
                "account": snowflake_details["account"],
                "warehouse": snowflake_details["warehouse"],
                "role": snowflake_details["role"],
            }
        ),
    )

    snowflake_conn_obj = (
        session.query(Connection)
        .filter(Connection.conn_id == SNOWFLAKE_CONN_ID)
        .first()
    )
    # airbyte_details={"host":"192.168.12.107","port":"8000","username":"airbyte","password":"password"}

    airbyte_conn = Connection(
        conn_id=AIRBYTE_CONN_ID,
        conn_type="airbyte",
        host="172.30.96.1",
        port="8000",
        login="airbyte",
        password="password",
        # host=airbyte_details["host"],
        # port=airbyte_details["port"],
        # login=airbyte_details["username"],
        # password=airbyte_details["password"],
    )

    airbyte_conn_obj = (
        session.query(Connection).filter(Connection.conn_id == AIRBYTE_CONN_ID).first()
    )

    print("snowflake - ", type(snowflake_conn_obj))
    print("Airbyte - ", type(airbyte_conn))

    if snowflake_conn_obj is None:
        print("Creating snowflake connection....")
        session.add(snowflake_conn)
        session.commit()
        print("Snowflake conn established....")

    if airbyte_conn_obj is None:
        print("Creating airbyte connection....")
        session.add(airbyte_conn)
        session.commit()
        print("airbyte conn established....")


def delete_connection():
    snowflake_conn_obj = (
        session.query(Connection)
        .filter(Connection.conn_id == SNOWFLAKE_CONN_ID)
        .first()
    )

    airbyte_conn_obj = (
        session.query(Connection).filter(Connection.conn_id == AIRBYTE_CONN_ID).first()
    )

    if snowflake_conn_obj is not None:
        print("deleting snowflake connection....")
        session.delete(snowflake_conn_obj)
        
    if airbyte_conn_obj is not None:
        print("deleting airbyte connection....")
        session.delete(airbyte_conn_obj)

    session.commit()
    session.close()


"""
## Initialize Dag
"""
with DAG(
    dag_id=DAG_ID,
    schedule=None,
    default_args=default_args,
    params={
        "email": Param(default="nishant.shah@anblicks.com", type="string"),
        "customer_id": Param(default=120, type=["integer", "string"], min=1, max=255),
        "config_db": Param(default="DEV_IMMUNIZATION_DB", type=["string"], min=1, max=255),
        "config_schema": Param(default="IMMUNIZATION", type=["string"], min=1, max=255),
        "airbyte_job_id": Param(
            default="7cb640c6-e092-4bf9-a4cd-4b7424d2e942",
            type=["string"],
            min=1,
            max=255,
        ),
    }
    # max_active_tasks=2
) as dag:
    # print(SFTP_FILE_COMPLETE_PATH)
    create_conn = PythonOperator(
        task_id="create_connections", python_callable=create_connection
    )

    airbyte_job_snowflake_sqlserver = AirbyteTriggerSyncOperator(
        task_id="airbyte_job_snowflake_sqlserver",
        airbyte_conn_id=AIRBYTE_CONN_ID,
        connection_id="""{{params.airbyte_job_id}}""",
        asynchronous=True,
    )

    # airbyte_sensor_snowflake_sqlserver = AirbyteJobSensor(
    #     task_id="airbyte_sensor_snowflake_sqlserver",
    #     airbyte_conn_id=AIRBYTE_CONN_ID,
    #     airbyte_job_id=airbyte_job_snowflake_sqlserver.output
    # )

    get_job_status = PythonOperator(
        task_id="get_job_status",
        python_callable=get_status,
        op_kwargs={"airbyte_job_run_id": airbyte_job_snowflake_sqlserver.output},
    )

    with TaskGroup(group_id="cleanup") as cleanup:
        del_conn = PythonOperator(
            task_id="delete_connections", python_callable=delete_connection
        )

        clean_xcom = PythonOperator(
            task_id="clean_xcom",
            python_callable=cleanup_xcom,
            provide_context=True,
            dag=dag,
        )

    create_conn >> airbyte_job_snowflake_sqlserver >> get_job_status >> cleanup
