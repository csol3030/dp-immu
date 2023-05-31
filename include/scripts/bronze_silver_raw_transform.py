from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit
from azure.identity import DefaultAzureCredential, AzureCliCredential
from azure.keyvault.secrets import SecretClient
from datetime import datetime, timedelta
import json
import constant

# Tables
BRONZE_TO_SILVER_STEP_DETAILS = "BRONZE_TO_SILVER_STEP_DETAILS"
BRONZE_TO_SILVER_DETAILS = "BRONZE_TO_SILVER_DETAILS"
STM_BRONZE_TO_SILVER = "STM_BRONZE_TO_SILVER"

# Constants
FILE_INGESTION_DETAILS = "FILE_INGESTION_DETAILS"
OBJECT_CONSTRUCT_COLUMN = "ADDITIONAL_DETAILS"
LOAD_DATE_COLUMN = "LOAD_DATE"
REC_SRC = "REC_SRC"

# Trnasformation Statuses
class TransStatus:
    PENDING = 'PENDING'
    IN_PROGRESS = 'IN_PROGRESS'
    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'



def print_log(title, data, status):
    print("-----"*2+title)
    print(data)
    print(status)
    print("-----"*2)

def get_kv_secret(secret_name):
    # connect to Azure Key vault and returns the specified secret value
    az_credential = AzureCliCredential()
    kv_client = SecretClient(vault_url=constant.KEYVAULT_URI, credential=az_credential)
    fetched_secret = kv_client.get_secret(secret_name)
    return fetched_secret.value

def get_snowflake_connection(context):
    # connects to snowflake and return snowpark session
    snowflake_connection_parameters = json.loads(get_kv_secret("SnowflakeSecret"))
    user = snowflake_connection_parameters.pop("username")
    snowflake_connection_parameters.update({
        "database": context["params"]["database"],
        "schema": context["params"]["schema"],
        "user": user
    })
    # create session using snowflake connector
    session = Session.builder.configs(snowflake_connection_parameters).create()
    return session

def insert_into_table(snowflake_session, target_table, source_table, source_columns,
                    target_columns, load_date):
    # Snowflake Insert into statment from a source table
    target_columns_str = ','.join([col for col in target_columns])
    source_columns_str = ','.join([col if "$$" not in col else f"'{col.split('$$')[-1]}'"
                                for col in source_columns])
    insert_into_str = f"""insert into {target_table}({target_columns_str})
                        select {source_columns_str} from {source_table}
                        where {LOAD_DATE_COLUMN}=to_timestamp('{load_date}')
                        """
    print(insert_into_str)
    resp_insert_into = snowflake_session.sql(insert_into_str).collect()
    print_log(f"insert_into_table | {target_table}", resp_insert_into, "")
    return resp_insert_into

def update_brnz_slvr_details(snowflake_session, brnz_slvr_dtls_id, db, schema,
                            status, error_details, date_update_str):
    # Update STATUS and TIMESTAMP in BRONZE_TO_SILVER_DETAILS
    query = f"""update {db}.{schema}.{BRONZE_TO_SILVER_DETAILS}
                set TRANSFORMATION_STATUS = '{status}'{date_update_str},
                ERROR_DETAILS = '{error_details}'
                where BRONZE_TO_SILVER_DETAILS_ID = {brnz_slvr_dtls_id}
            """
    resp = snowflake_session.sql(query).collect()
    print_log(f"update_brnz_slvr_details | {db}.{schema}.BRONZE_TO_SILVER_DETAILS",
            resp, status)

def create_brnz_slvr_step_details(snowflake_session, brnz_slvr_dtls_id, file_ing_dtls_id,
                                db, schema, target_schema, target_table):
    updated_by = snowflake_session.sql("SELECT CURRENT_USER").collect()
    updated_by = f"{updated_by}".split("=")[1].strip("')]'")
    created_date = str(datetime.now())
    table_schema = ["BRONZE_TO_SILVER_DETAILS_ID", "FILE_INGESTION_DETAILS_ID",
        "TARGET_SCHEMA", "TARGET_TABLE", "STATUS", "CREATED_DATE", "UPDATED_BY"]
    data = (brnz_slvr_dtls_id, file_ing_dtls_id, target_schema,
            target_table, TransStatus.PENDING, created_date, updated_by)
    query = f"""insert into {db}.{schema}.{BRONZE_TO_SILVER_STEP_DETAILS}
                ({','.join(table_schema)}) values {str(data)}
            """
    resp = snowflake_session.sql(query).collect()
    print_log(f"create_brnz_slvr_step_details | {db}.{schema}.BRONZE_TO_SILVER_STEP_DETAILS",
            resp, target_table)
    return created_date

def update_brnz_slvr_step_details(snowflake_session, brnz_slvr_dtls_id, file_ing_dtls_id,
                                db, schema, target_schema, target_table, created_date,
                                status, error_details, date_update_str):
    query=f"""UPDATE {db}.{schema}.{BRONZE_TO_SILVER_STEP_DETAILS}
        SET STATUS = '{status}',
        ERROR_DETAILS = '{error_details}'{date_update_str}
        where TARGET_SCHEMA = '{target_schema}'
        AND TARGET_TABLE = '{target_table}'
        AND BRONZE_TO_SILVER_DETAILS_ID = '{brnz_slvr_dtls_id}'
        AND FILE_INGESTION_DETAILS_ID = '{file_ing_dtls_id}'
        AND CREATED_DATE = {created_date}
        """
    resp = snowflake_session.sql(query).collect()
    print_log(f"update_brnz_slvr_step_details | {db}.{schema}.BRONZE_TO_SILVER_STEP_DETAILS",
            resp, f"{target_table}\n{status}")

def get_table_records(snowflake_session, table,
                    filter_dict=None, group_by=None, order_by=None, limit=None):
    query = snowflake_session.table(table)
    if filter_dict:
        for column, value in filter_dict.items():
            query = query.filter(col(column) == lit(value))
    if order_by:
        query = query.orderBy(col(order_by['col']), ascending=order_by['ascending'])
    if limit:
        query = query.limit(limit)
    data = query.to_pandas()
    if group_by:
        data = data.groupby(group_by)
    return data

def get_unmapped_columns(snowflake_session, deleted_columns, source_schema,
                        source_table, source_target_zip, source_config_columns):
    # Compares source table columns with target columns and returns the unmapped columns
    data = dict(source_target_zip)
    source_db, source_schema = source_schema.split('.')
    query = f"""select column_name
                from {source_db}.information_schema.columns
                where table_schema ilike '{source_schema}'
                and table_name ilike '{source_table}'
                order by ordinal_position;"""
    source_columns = snowflake_session.sql(query).to_pandas().values.tolist()
    source_columns_flatten = [item for sublist in source_columns for item in sublist]
    unmapped_columns = [scol for scol in source_columns_flatten
                        if scol not in source_config_columns and scol not in deleted_columns]
    return unmapped_columns

def get_deleted_columns(snowflake_session, file_ing_dtls_id):
    # Get deleted columns from FILE_INGESTION_DETAILS (SCHEMA_DRIFT_COLUMNS)
    file_ing_dtls = get_table_records(snowflake_session, FILE_INGESTION_DETAILS, filter_dict={
                                        "FILE_INGESTION_DETAILS_ID": file_ing_dtls_id
                                    })
    if file_ing_dtls.empty:
        return list()
    deleted_columns = []
    schema_drift_cols = file_ing_dtls[['SCHEMA_DRIFT_COLUMNS']].values.tolist()[0][0]
    if schema_drift_cols:
        deleted_columns = json.loads(schema_drift_cols).get('deleted_columns')
    return deleted_columns

def transformation(brnz_slvr_dtls_dict, context):
    # Refers STM_BRONZE_TO_SILVER table with Config data to get Columns
    # Creates Object Construct if OBJECT_CONSTRUCT_COLUMN is present
    # Insert records into repsective HUB, Satellite and Link tables
    snowflake_session = get_snowflake_connection(context)
    try:
        source_schema = brnz_slvr_dtls_dict['source_schema']
        source_table = brnz_slvr_dtls_dict['source_table']
        src_ld_dt = brnz_slvr_dtls_dict['src_ld_dt']
        file_ing_dtls_id = brnz_slvr_dtls_dict['file_ing_dtls_id']
        brnz_slvr_dtls_id = brnz_slvr_dtls_dict['brnz_slvr_dtls_id']
        trans_status = TransStatus.FAILED
        error_details = ""
        db = context["params"]["database"]
        schema = context["params"]["schema"]

        update_brnz_slvr_details(snowflake_session, brnz_slvr_dtls_id,
                                db, schema, TransStatus.IN_PROGRESS, error_details,
                                f", START_DATE = to_timestamp('{str(datetime.now())}')")

        stm_details_df = get_table_records(snowflake_session, STM_BRONZE_TO_SILVER, filter_dict={
                            "SOURCE_SCHEMA": source_schema,
                            "SOURCE_TABLE": source_table
                        }, group_by=["TARGET_SCHEMA", "TARGET_TABLE"])

        deleted_columns = get_deleted_columns(snowflake_session, file_ing_dtls_id)

        file_ing_dtls_id = int(file_ing_dtls_id)
        to_be_inserted_count = len(stm_details_df)
        inserted_count = 0
        lst_brnz_slvr_step_dtls = []

        for stm_details_grp, stm_dtls in stm_details_df:
            target_schema, target_table = stm_details_grp
            if brnz_slvr_dtls_dict['trans_status'] == TransStatus.FAILED:
                step_details = get_table_records(snowflake_session, BRONZE_TO_SILVER_STEP_DETAILS,
                                                filter_dict={
                    "FILE_INGESTION_DETAILS_ID": file_ing_dtls_id,
                    "TARGET_SCHEMA": target_schema,
                    "TARGET_TABLE": target_table,
                    "BRONZE_TO_SILVER_DETAILS_ID": brnz_slvr_dtls_id
                }, order_by={'col':'BRONZE_TO_SILVER_STEP_ID', 'ascending': False}, limit=1)

                if not step_details.empty:
                    if step_details.at[0, 'STATUS'] != TransStatus.FAILED:
                        to_be_inserted_count -= 1
                        continue
                else:
                    to_be_inserted_count -= 1
                    continue

            created_date = create_brnz_slvr_step_details(snowflake_session, brnz_slvr_dtls_id,
                                                file_ing_dtls_id, db, schema, target_schema,
                                                target_table)
            lst_brnz_slvr_step_dtls.append((target_schema, target_table, stm_dtls, created_date))

        for target_schema, target_table, stm_dtls, created_date in lst_brnz_slvr_step_dtls:
            try:
                step_start_date = str(datetime.now())
                step_status = TransStatus.FAILED
                step_error_details = ""
                update_brnz_slvr_step_details(snowflake_session,
                                            brnz_slvr_dtls_id, file_ing_dtls_id,
                                            db, schema, target_schema, target_table,
                                            f"to_timestamp('{created_date}')",
                                            TransStatus.IN_PROGRESS, "",
                                            f", START_DATE=to_timestamp('{step_start_date}')")
                source_columns, target_columns, source_column_fn = zip(*stm_dtls[
                    ['SOURCE_COLUMN', 'TARGET_COLUMN', 'SOURCE_COLUMN_FN']].values.tolist())
                source_columns = list(source_columns)
                source_column_fn = list(source_column_fn)
                if OBJECT_CONSTRUCT_COLUMN in target_columns:
                    add_dtls_column_index, add_dtls_columns = (
                        target_columns.index(OBJECT_CONSTRUCT_COLUMN), get_unmapped_columns(
                        snowflake_session, deleted_columns, source_schema, source_table,
                        zip(source_columns, target_columns), source_columns ))
                    obj_const_columns_str = ','.join([f"'{col}',{col}" for col in add_dtls_columns])
                    obj_const_str = f"OBJECT_CONSTRUCT ({obj_const_columns_str}) as {OBJECT_CONSTRUCT_COLUMN}"
                    source_columns[add_dtls_column_index] = obj_const_str
                if REC_SRC in target_columns:
                    source_columns[target_columns.index(REC_SRC)] = str(file_ing_dtls_id)
                if LOAD_DATE_COLUMN in target_columns:
                    source_columns[target_columns.index(LOAD_DATE_COLUMN)] = f"to_timestamp('{str(datetime.now())}')"
                for indx, item in enumerate(source_column_fn):
                    if item and item.upper() != 'NULL':
                        source_columns[indx] = source_column_fn[indx]
                source_table_path = f"{source_schema}.{source_table}"
                target_table_path = f"{target_schema}.{target_table}"
                response = insert_into_table(snowflake_session, target_table_path,
                                source_table_path, source_columns, target_columns, src_ld_dt)
                if response:
                    row = response[0].as_dict()
                    if 'number of rows inserted' in row:
                        inserted_count += 1
                        step_status = TransStatus.SUCCESS
                if step_status == TransStatus.FAILED:
                    step_error_details = f"ERROR: Could not Update {target_schema}.{target_table}"
            except Exception as e:
                step_status == TransStatus.FAILED
                step_message = str(e).replace("'", "")
                step_error_details = f"ERROR: Could not Update {target_schema}.{target_table}, {step_message}"
            finally:
                update_brnz_slvr_step_details(snowflake_session,
                                            brnz_slvr_dtls_id, file_ing_dtls_id,
                                            db, schema, target_schema, target_table,
                                            f"to_timestamp('{created_date}')", step_status,
                                            step_error_details,
                                            f", END_DATE=to_timestamp('{str(datetime.now())}')")
        if inserted_count and inserted_count == to_be_inserted_count:
            trans_status = TransStatus.SUCCESS
        else:
            trans_status = TransStatus.FAILED
            error_details = "Could not Update Mapped Target Table(s)"

    except Exception as e:
        trans_status = TransStatus.FAILED
        trans_message = str(e).replace("'", "")
        error_details = f"ERROR: {trans_message}"

    finally:
        update_brnz_slvr_details(snowflake_session, brnz_slvr_dtls_id,
                                db, schema, trans_status, error_details,
                                f", END_DATE = to_timestamp('{str(datetime.now())}')")


def process(context):
    try:
        snowflake_session = get_snowflake_connection(context)
        bronze_slvr_dtls=[]

        for trans_status in (TransStatus.PENDING, TransStatus.FAILED):

            try:
                if not context["params"]["run_failed"] and trans_status == TransStatus.FAILED:
                    continue
                pd_brnz_slvr_dtls = get_table_records(snowflake_session, 'BRONZE_TO_SILVER_DETAILS',
                                        filter_dict={'TRANSFORMATION_STATUS': trans_status})
                for ind in pd_brnz_slvr_dtls.index:
                    brnz_slvr_dtls_dict = {}
                    brnz_slvr_dtls_dict['source_schema'] = pd_brnz_slvr_dtls['SOURCE_SCHEMA'][ind]
                    brnz_slvr_dtls_dict['source_table'] = pd_brnz_slvr_dtls['SOURCE_TABLE'][ind]
                    brnz_slvr_dtls_dict['src_ld_dt'] = str(pd_brnz_slvr_dtls['SOURCE_LOAD_DATE'][ind])
                    brnz_slvr_dtls_dict['file_ing_dtls_id'] = int(pd_brnz_slvr_dtls['FILE_INGESTION_DETAILS_ID'][ind])
                    brnz_slvr_dtls_dict['brnz_slvr_dtls_id'] = int(pd_brnz_slvr_dtls['BRONZE_TO_SILVER_DETAILS_ID'][ind])
                    brnz_slvr_dtls_dict['trans_status'] = pd_brnz_slvr_dtls['TRANSFORMATION_STATUS'][ind]
                    bronze_slvr_dtls.append(brnz_slvr_dtls_dict)
            except Exception as e:
                error_message = str(e).replace("'", "")
                print(f"ERROR: {error_message}")

        return [[brnz_slvr_dtls_dict] for brnz_slvr_dtls_dict in bronze_slvr_dtls]

    except Exception as e:
        error_message = str(e).replace("'", "")
        print(f"ERROR: {error_message}")
