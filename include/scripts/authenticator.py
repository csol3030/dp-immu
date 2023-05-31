
import json
from azure.identity import AzureCliCredential
from azure.keyvault.secrets import SecretClient
from snowflake.snowpark import Session
import constant


KEYVAULT_SNOWFLAKE_SECRET = "SnowflakeImmunizationSecret"
DATABASE = "DEV_IMMUNIZATION_DB"
SCHEMA = "IMMUNIZATION"


def keyvault_value_with_secret(keyvault_uri, secret_name):
    # connect to Azure Key vault and returns the specified secret value
    az_credential = AzureCliCredential()
    kv_client = SecretClient(vault_url=constant.KEYVAULT_URI, credential=az_credential)
    fetched_secret = kv_client.get_secret(secret_name)
    return fetched_secret.value

def snowflake_connection(snowflake_uri,keyvault_snowflake_secret,databases, schema):
    # connects to snowflake and return snowpark session
    snowflake_connection_parameters = json.loads(keyvault_value_with_secret(snowflake_uri,keyvault_snowflake_secret))
    user = snowflake_connection_parameters.pop("username")
    snowflake_connection_parameters.update({
        "database": databases,
        "schema": schema,
        "user": user
    })
    # create session using snowflake connector
    session = Session.builder.configs(snowflake_connection_parameters).create()
    return session