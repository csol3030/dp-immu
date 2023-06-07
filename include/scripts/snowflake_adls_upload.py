from multiprocessing import context
from pydoc import stripid
from snowflake.snowpark import Session
import snowflake.snowpark.functions as F
from snowflake.snowpark.types import StructType,StructField,StringType
import json
from azure.identity import AzureCliCredential
from azure.keyvault.secrets import SecretClient
from datetime import datetime
import pandas as pd
import file_transfer
import constant

KEYVAULT_SNOWFLAKE_SECRET = "SnowflakeImmunizationSecret"
DATABASE = "DEV_IMMUNIZATION_DB"
SCHEMA = "IMMUNIZATION"

year = 2013
month = 5
customer_id = 125
state_code = "VA"

def get_kv_secret(secret_name):
    # connect to Azure Key vault and returns the specified secret value
    az_credential = AzureCliCredential()
    kv_client = SecretClient(vault_url=constant.KEYVAULT_URI, credential=az_credential)
    fetched_secret = kv_client.get_secret(secret_name)
    return fetched_secret.value

def get_snowflake_connection():
    # connects to snowflake and return snowpark session
    snowflake_connection_parameters = json.loads(get_kv_secret(KEYVAULT_SNOWFLAKE_SECRET))
    user = snowflake_connection_parameters.pop("username")
    snowflake_connection_parameters.update({
        "database": DATABASE,
        "schema": SCHEMA,
        "user": user
    })
    # create session using snowflake connector
    session = Session.builder.configs(snowflake_connection_parameters).create()
    return session

session = get_snowflake_connection()

# file_format_config used to export file config, validation details
file_config_dic = {}

def get_state_config(code):

    df = session.table(f"{DATABASE}.{SCHEMA}.CONFIG_STATE").filter(F.col("STATE_CODE") == code)
    first_object = df.first()

    global file_config_dic
    file_config_dic = {
        "file_extension": first_object["FILE_EXTENSION"].split('.')[-1],
        "field_delimiter": first_object["FIELD_DELIMITER"] if first_object["FIXED_LENGTH"] == False else 'None',
        "is_fixed_length": first_object["FIXED_LENGTH"],
        "enclosed_by": first_object["FIELD_ENCLOSE"],
        "max_file_size":  first_object["MAX_SIZE"],
        "input_file_name": first_object["FILE_NAME_FORMAT"],
        "is_header": first_object["IS_HEADER_ROW"],
        "have_validation": first_object["VALIDATION"],
        "is_ascii": first_object["IS_ASCII"],
        "field_exclude": first_object["FIELD_EXCLUDE"],
        "missing_data": first_object["MISSING_DATA"],
        "source_db": first_object["SOURCE_DB"],
        "source_table": first_object["SOURCE_TABLE"],
        "source_schema": first_object["SOURCE_SCHEMA"],
        "state_has_encryption": first_object["HAS_ENCRYPTION"],
        "private_key": first_object["PRIVATE_KEY_SECRETE"],
        "publick_key": first_object["PUBLIC_KEY_SECRETE"],
        "state": first_object["STATE"],
        "state_code": code
    }

    return first_object["STATE_REG_ID"]


def get_filed_config_for_state(id):
    df = session.sql(f"select * from CONFIG_STM where state_reg_id = {id} order by SEQUENCE asc")
    global stm_config_pd
    stm_config_pd = df.to_pandas().to_dict('list')

    generate_member_df(stm_config_pd)

def generate_member_df(stm_config):

    str_select_column = "select " +  prepare_select_query(stm_config) +  f""" from {file_config_dic["source_table"]} where MBR_HOME_STATE_CD = '{file_config_dic["state_code"]}'"""
    global df_member_list
    df_member_list = session.sql(str_select_column)
    # df_member_list.show()
    
    df_member_list = manipulate_and_validate(df_member_list,stm_config)
    
    # print("Before export file df")
    # df_member_list.show()

    export_file(df_member_list, file_config_dic)
    move_file_to_sftp()

def move_file_to_sftp():
    print("ADLS to sftp flow started")
    adls_path = f"""OUTPUT/{customer_id}/{file_config_dic["state"]}/{year}/{month}/"""
    sftp_upload_path = f"""/OUTPUT/{customer_id}/{file_config_dic["state"]}/{year}/{month}/"""
    file_transfer.state_snowflake_export_to_sftp(file_config_dic["publick_key"],bool(file_config_dic["state_has_encryption"]),adls_path,sftp_upload_path)

def prepare_select_query(stm_config):
    source_fields = stm_config["SOURCE_FIELD"]
    target_col = stm_config["TARGET_FIELD"]
    default_values = stm_config["DEFAULT"]

    enumerate_source = enumerate(source_fields)
    arry_field = []
    for index, key in enumerate_source:
        if key:
            arry_field.append(f""" IFNULL({key},'') as {key} """)
        else:
            arry_field.append(f"""'{default_values[index]}' as "{target_col[index]}" """)
    
    return ",".join(arry_field)

def create_file_format(config_dict):
    create_file_format = """create or replace file format input_format
            type = csv
            null_if = ()
            empty_field_as_null = false
            COMPRESSION = NONE"""
    if "file_extension" in config_dict:  
        create_file_format += f""" file_extension = '{config_dict["file_extension"]}'"""
    if "enclosed_by" in config_dict and config_dict["enclosed_by"]: 
        create_file_format += f""" FIELD_OPTIONALLY_ENCLOSED_BY = '{config_dict["enclosed_by"]}'"""
    if "field_delimiter" in config_dict: 
        create_file_format += f""" field_delimiter = '{config_dict["field_delimiter"]}'"""
    print(create_file_format)
    
    return create_file_format

def export_file(input_df, config_dict):
    str_create_file_format = create_file_format(config_dict)
    # print(str_create_file_format)
    session.sql(str_create_file_format).collect()

    date = datetime.now()
    file_name = f"""{config_dict["input_file_name"]}{date.strftime("%m%d%Y")}"""
    if config_dict["max_file_size"] > 0:
        
        input_df.write.copy_into_location(f"""@STAGE_IMMUNIZATION/{customer_id}/{config_dict["state"]}/{year}/{month}/{file_name}""", 
                file_format_name="input_format", overwrite=True, header=config_dict["is_header"],
                single= False, MAX_FILE_SIZE= config_dict["max_file_size"])
    else:
        file_name = f"""{file_name}.{config_dict["file_extension"]}"""
        input_df.write.copy_into_location(f"""@STAGE_IMMUNIZATION/{customer_id}/{config_dict["state"]}/{year}/{month}/{file_name}""", 
                                      file_format_name="input_format", overwrite=True, header=config_dict["is_header"],
                                      single=True)

def manipulate_and_validate(df,stm_config):
    source_fields = stm_config["SOURCE_FIELD"]
    target_col = stm_config["TARGET_FIELD"]

    # Check each field level validations like valid values, default values, dateformat, 
    # Check file level validation missing data, field exclude, 
    df = check_filed_format_validation(df, stm_config)
    df = check_validate_value(df, stm_config)

    if file_config_dic["field_exclude"]:
        df = check_apply_exclude_values(df,source_fields)

    # print(" ********* Before DF col mapped *********")
    # df.show()

    df = map_col_name(df, source_fields, target_col)
    # print(" ********* DF col mapped *********")
    new_seq = []
    for item in target_col:
        new_seq.append((f'"{str(item)}"'))

    df = df.select(new_seq)
    return df

def check_filed_format_validation(df, stm_config):
    source_fields = stm_config["SOURCE_FIELD"]
    enumerate_data = enumerate(source_fields)
    for index, key in enumerate_data:
        df = data_type_format_check(df,stm_config,index)
        df = sufifx_check(df,stm_config,index)
        df = fix_lenght_files(df,index,stm_config)

    return df

def fix_lenght_files(df,index,stm_config):
    source_cols = stm_config["SOURCE_FIELD"]
    target_cols = stm_config["TARGET_FIELD"]
    col_name = source_cols[index] if len(source_cols[index]) > 0 else target_cols[index]
    if (file_config_dic["is_fixed_length"]):
        df = update_df_fixed_lenght_value(df,col_name,index,stm_config)
    return df

def update_df_fixed_lenght_value(df,col,index,stm_config):
        return df.with_column(col,(F.rpad(F.col(col), stm_config['MAX_LENGTH'][index], F.lit("*"))))

def sufifx_check(df,stm_config,index):
    if len(stm_config["SUFFIX"][index]) != 0:
        suffixes = json.loads(stm_config["SUFFIX"][index])
        source_fields = stm_config["SOURCE_FIELD"]
        target_fields = stm_config["TARGET_FIELD"]
        col_name = source_fields[index] if len(source_fields[index]) > 0 else target_fields[index]
        print("Column name: " + col_name)
        if len(suffixes.keys()) > 0:
            array_suffix = suffixes['suffix_strings']
            for val in array_suffix:
                df = remove_suffix(df,True,val,col_name)   
        return df
    else:
        return df

def data_type_format_check(df,stm_config,index):
        source_fields = stm_config["SOURCE_FIELD"]
        data_format = stm_config["FORMAT"][index]
        if len(stm_config["FORMAT"][index]) > 0:
            if stm_config["DATA_TYPE"][index] == "datetime":
                df = update_date_format(df,index, source_fields,data_format)
            elif stm_config["DATA_TYPE"][index] == "integer":
                df = update_number_format(df,index, source_fields,data_format)
            elif stm_config["DATA_TYPE"][index] == "varchar":
                df = update_string_format(df,index, source_fields,data_format)
        return df

def update_number_format(df,index,source_fields,data_format):
    print("update_number_format") 
    return df

def update_string_format(df,index,source_fields,data_format):
    print("update_string_format")
    return df

def update_date_format(df,index,source_fields,data_format):
    print("update_date_format") 
    print(source_fields[index])
    col = source_fields[index]    
    df = df.with_column(col, F.sql_expr(f"""to_varchar(try_to_date({col}, 'YYYY-MM-DD'), '{data_format}')"""))
    return df

def remove_suffix(df,isSuffix,value,col):
    print("remove_suffix")
    if isSuffix == True:
        df = df.with_column(col, F.trim(F.rtrim(col, F.lit(f"""{value}""")))) 
    else:
        df = df.with_column(col, F.trim(F.ltrim(col, F.lit(f"""{value}""")))) 
    return df
    
def check_apply_exclude_values(df,source_fields):
    print("\n ************* check_apply_exclude_values *************\n")
    ex_json=json.loads(file_config_dic["field_exclude"])
    if len(ex_json.keys()) > 0:
        print(len(ex_json.keys()))   
        array_exclude_char = ex_json['ex_string'] 
        print(array_exclude_char)
        for exclude_item in array_exclude_char:  
            df = update_df_exclude(df,exclude_item,source_fields)  
    
    return df

def map_col_name(df,source_col, target_col):
    enumerate_data = enumerate(source_col)
    for index, key in enumerate_data:
        if len(key) > 0:
            df = update_df_col_name(df, key, target_col[index])
    return df

def update_df_col_name(df,key, newkey): 
     return df.with_column_renamed(F.col(key), f'"{newkey}"')

def update_df_exclude(df1, exclude_item, source_fields):  
    # df1.show() 
    for col in source_fields:
        df1 = remove_value(df1, col, exclude_item)
    return df1
    
def remove_value(df1, col, exclude_item):
    return df1.with_column(col, (F.regexp_replace(F.col(col), F.lit(f"""{exclude_item}"""), F.lit("")))) 

def check_validate_value(df,stm_config):
    print("***********************VALIDATION****************************")
    temp_df = pd.DataFrame.from_dict(stm_config)
    print(temp_df)
    for i in range(len(temp_df)):
        if(temp_df['VALID_VALUES'][i]!=''):
            df=update_validated_df(df,temp_df['SOURCE_FIELD'][i],temp_df['VALID_VALUES'][i],temp_df['DEFAULT'][i])
    return df

def update_validated_df(df,source,valid_values,default):
    print("*********************UPDATION VALIDATION*************************")
    valid_list=valid_values.split(',')
    l=df.count()
    df = df.withColumn(source, F.when(F.col(source).isin(valid_list), F.col(source)).otherwise(default))
    return df

def set_context_param(context, state):
        print("context: ")
        print(context)
        global year, month, customer_id,state_code
        state_code = state.strip()
        year = context['params']['year']
        month = context['params']['month']
        customer_id = context['params']['customer_id']

def process(context):
    try:
        state_codes = context['params']['state_code'].split(",")
        for state in state_codes:
            set_context_param(context, state)
            state_reg_id = get_state_config(state_code)
            get_filed_config_for_state(state_reg_id)

    except Exception as e:
        print(e)
    finally:
        if session:
            session.close()

def download(context):
    try:
        state_codes = context['params']['state_code'].split(",")
        for state in state_codes:
            set_context_param(context, state)
            get_state_config(state_code)
            download_sftp_path=f"""/INPUT/{customer_id}/{file_config_dic["state"]}/{year}/{month}"""

            file_transfer.state_sftp_file_to_adls(
                file_config_dic["private_key"],
                download_sftp_path,
                download_sftp_path,
                bool(file_config_dic["state_has_encryption"])
                )    
    except Exception as e:
        print(e)
    finally:
        if session:
            session.close()

# context = {'ds': '2023-06-05', 'ds_nodash': '20230605', 'expanded_ti_count': None, 'inlets': [], 'next_ds': '2023-06-05', 'next_ds_nodash': '20230605', 'outlets': [], 'prev_ds': '2023-06-05', 'prev_ds_nodash': '20230605', 'run_id': 'manual__2023-06-05T08:48:36+00:00', 'task_instance_key_str': 'af_snowflake_to_adls__copy_from_snowflake_adls__20230605', 'test_mode': False, 'tomorrow_ds': '2023-06-06', 'tomorrow_ds_nodash': '20230606', 'ts': '2023-06-05T08:48:36+00:00', 'ts_nodash': '20230605T084836', 'ts_nodash_with_tz': '20230605T084836+0000', 'yesterday_ds': '2023-06-04', 'yesterday_ds_nodash': '20230604', 'params': {'state_code': 'OH', 'year': '2023', 'month': '5', 'customer_id': '125'}, 'templates_dict': None}
# state_codes = context['params']['state_code'].split(",")
# for state in state_codes:
#     set_context_param(context,state)
#     state_reg_id = get_state_config(state_code)
#     get_filed_config_for_state(state_reg_id)
