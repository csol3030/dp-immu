from multiprocessing import context
from snowflake.snowpark import Session
import snowflake.snowpark.functions as F
from snowflake.snowpark.types import StructType,StructField,StringType
import json
from azure.identity import AzureCliCredential
from azure.keyvault.secrets import SecretClient
from datetime import datetime
import pandas as pd
import file_encr_decr
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
file_format_config = {}

def get_state_config(code):

    df = session.table(f"{DATABASE}.{SCHEMA}.CONFIG_STATE").filter(F.col("STATE_CODE") == code)
    first_object = df.first()

    global file_format_config
    file_format_config = {
        "file_extension": first_object["FILE_EXTENSION"].split('.')[-1],
        "field_delimiter": first_object["FIELD_DELIMITER"],
        "is_fixed_length":first_object["FIXED_LENGTH"],
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
        "private_key": first_object["PUBLIC_KEY_SECRETE"],
        "publick_key": first_object["PRIVATE_KEY_SECRETE"],
        "state": first_object["STATE"],
        "state_code": code
    }

    return first_object["STATE_REG_ID"]


def get_filed_config_for_state(id):
    df = session.sql(f"select * from CONFIG_STM where state_reg_id = {id} order by SEQUENCE asc")
    global dict_panda_df
    dict_panda_df = df.to_pandas().to_dict('list')
    generate_member_df(dict_panda_df)
    print(dict_panda_df)
    # export_file(df_member_list, file_format_config)
    # SOURCE_FIELD

def generate_member_df(field_config_df):

    str_select_column = "select " +  prepare_select_query(field_config_df) +  f""" from {file_format_config["source_table"]} where MBR_HOME_STATE_CD = '{file_format_config["state_code"]}'"""
    print(str_select_column)
    global df_member_list
    df_member_list = session.sql(str_select_column)
    df_member_list.show()

    df_member_list = manipulate_and_validate(df_member_list,field_config_df)
    
    print("Before export file df")
    df_member_list.show()
    export_file(df_member_list, file_format_config)

    # file_encr_decr.download_blob_container()
    # file_encr_decr.process_folder_files_encryption()

    # ADLS to sftp flow
    move_file_to_adls()

def move_file_to_adls():
    print("ADLS to sftp flow started")
    path_info = file_transfer.download_blob_container(customer_id, file_format_config["state"], year, month)

    if bool(file_format_config["state_has_encryption"]) == True:
        print('state_has_encryption')
        # file_encr_decr.get_public_private_key(file_format_config["private_key"],file_format_config["publick_key"])
        folder_encryption = file_encr_decr.process_folder_files_encryption(path_info)
        local_path, upload_path = folder_encryption
        file_transfer.upload_files_to_sftp(folder_encryption)
        file_transfer.remove_all_files_from_path(local_path)
    else:
        print('Not have state_has_encryption')
        local_path, directory_structure = path_info 
        file_transfer.upload_files_to_sftp(path_info)
        file_transfer.remove_all_files_from_path(local_path)

def prepare_select_query(field_config_df):
    source_fields = field_config_df["SOURCE_FIELD"]
    target_col = field_config_df["TARGET_FIELD"]
    default_values = field_config_df["DEFAULT"]

    enumerate_source = enumerate(source_fields)
    arry_field = []

    for index, key in enumerate_source:
        if key:
            arry_field.append(key)
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

def manipulate_and_validate(df,field_config_df):
    source_fields = field_config_df["SOURCE_FIELD"]
    target_col = field_config_df["TARGET_FIELD"]

    # Check each field level validations like valid values, default values, dateformat, 
    # Check file level validation missing data, field exclude, 
    df = check_filed_format_validation(df, field_config_df)
    df = check_validate_value(df, field_config_df)

    if file_format_config["field_exclude"]:
        df = check_apply_exclude_values(df,source_fields)

    df = map_col_name(df, source_fields, target_col)
    return df

def check_filed_format_validation(df, field_config_df):
    source_fields = field_config_df["SOURCE_FIELD"]
    enumerate_data = enumerate(source_fields)
    for index, key in enumerate_data:
        df = data_type_format_check(df,field_config_df,index)
        df = sufifx_check(df,field_config_df,index)

    return df

def sufifx_check(df,field_config_df,index):
    print(" ********** suffix ********** ")
    if len(field_config_df["SUFFIX"][index]) != 0:
        suffixes = json.loads(field_config_df["SUFFIX"][index])
        source_fields = field_config_df["SOURCE_FIELD"]
        col_name = source_fields[index]
        if len(suffixes.keys()) > 0:
            array_suffix = suffixes['suffix_strings']
            for val in array_suffix:
                df = remove_suffix(df,True,val,col_name)   
        return df
    else:
        return df

def data_type_format_check(df,field_config_df,index):
        source_fields = field_config_df["SOURCE_FIELD"]
        data_format = field_config_df["FORMAT"][index]
        if len(field_config_df["FORMAT"][index]) > 0:
            if field_config_df["DATA_TYPE"][index] == "datetime":
                df = update_date_format(df,index, source_fields,data_format)
            elif field_config_df["DATA_TYPE"][index] == "integer":
                df = update_number_format(df,index, source_fields,data_format)
            elif field_config_df["DATA_TYPE"][index] == "varchar":
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
    ex_json=json.loads(file_format_config["field_exclude"])
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
     return df.with_column_renamed(F.col(key), newkey)

def update_df_exclude(df1, exclude_item, source_fields):  
    df1.show() 
    for col in source_fields:
        df1 = remove_value(df1, col, exclude_item)
    return df1
    
def remove_value(df1, col, exclude_item):
    return df1.with_column(col, (F.regexp_replace(F.col(col), F.lit(f"""{exclude_item}"""), F.lit("")))) 

def check_validate_value(df,field_config_df):
    print("***********************VALIDATION****************************")
    temp_df = pd.DataFrame.from_dict(field_config_df)
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

def set_context_param(context):
        global year, month, customer_id,state_code
        state_code = context['params']['state_code']
        year = context['params']['year']
        month = context['params']['month']
        customer_id = context['params']['customer_id']

def process(context):
    try:
        set_context_param(context)
        state_reg_id = get_state_config(state_code)
        get_filed_config_for_state(state_reg_id)

    except Exception as e:
        print(e)
    finally:
        if session:
            session.close()

def download(context):
    try:
        set_context_param(context)
        get_state_config(state_code)

        # SFTP to ADLS flow 
        if bool(file_format_config["state_has_encryption"]) == True:
            path_info = file_transfer.download_files_from_sftp(customer_id, file_format_config["state"], year, month)
            upload_from = file_encr_decr.process_folder_file_decryption(path_info)
        else:
            path_info = file_transfer.download_files_from_sftp(customer_id, file_format_config["state"], year, month)
            download_directory, upload_from = path_info

        file_transfer.upload_files_to_blob_storage(upload_from, bool(file_format_config["state_has_encryption"]))
    
    except Exception as e:
        print(e)
    finally:
        if session:
            session.close()

# context = {'ds': '2023-06-01', 'ds_nodash': '20230601', 'expanded_ti_count': None, 'inlets': [], 'next_ds': '2023-06-01', 'next_ds_nodash': '20230601', 'outlets': [], 'prev_ds': '2023-06-01', 'prev_ds_nodash': '20230601', 'run_id': 'manual__2023-06-01T16:43:36.717907+00:00', 'task_instance_key_str': 'af_snowflake_to_adls__copy_from_snowflake_adls__20230601', 'test_mode': False, 'tomorrow_ds': '2023-06-02', 'tomorrow_ds_nodash': '20230602', 'ts': '2023-06-01T16:43:36.717907+00:00', 'ts_nodash': '20230601T164336', 'ts_nodash_with_tz': '20230601T164336.717907+0000', 'yesterday_ds': '2023-05-31', 'yesterday_ds_nodash': '20230531', 'params': {'state_code': 'VA', 'year': '2023', 'month': '5', 'customer_id': '125'}, 'templates_dict': None}
# set_context_param(context)
# state_reg_id = get_state_config(state_code)
# get_filed_config_for_state(state_reg_id)
