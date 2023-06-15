import pysftp
import os
import io
import json
from azure.identity import AzureCliCredential
from azure.keyvault.secrets import SecretClient

import json
import urllib.parse
from azure.identity import AzureCliCredential
from azure.storage.blob import BlobServiceClient
from azure.keyvault.secrets import SecretClient
import constant
from pysftp import Connection
import pgpy
import base64
from io import StringIO

KEYVAULT_BLOB_STORAGE_SECRET = "ADLSBlobConnSTR"

def get_kv_secret(secret_name):
    # connect to Azure Key vault and returns the specified secret value
    az_credential = AzureCliCredential()
    kv_client = SecretClient(vault_url=constant.KEYVAULT_URI, credential=az_credential)
    fetched_secret = kv_client.get_secret(secret_name)
    return fetched_secret.value
    

cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

def get_azure_connection():
    container_name =  "cont-datalink-dp-shared"
    # create a client to interact with blob storage
    blob_details = json.loads(get_kv_secret(KEYVAULT_BLOB_STORAGE_SECRET))
    connection_str = blob_details.get('connection_string')
    blob_service_client = BlobServiceClient.from_connection_string(connection_str)
    account_name = urllib.parse.urlsplit(blob_details.get('host')).hostname.split('.')[0]
    # use the client to connect to the container
    container_client = blob_service_client.get_container_client(container_name)
    sas = connection_str.split('SharedAccessSignature=')[1]
    connection = (blob_service_client, container_client, account_name, sas)
    return connection


def state_sftp_file_to_adls(vault_key,download_sftp_path,upload_adls_path,encryption_enabled):
    try:
        if encryption_enabled == True:
            private_key = base64.b64decode(get_kv_secret(vault_key)).decode('ascii')
            key_private, _ = pgpy.PGPKey.from_blob(private_key)

        azure_connection = get_azure_connection()
        blob_service_client, azure_session, account_name, sas = azure_connection
        
        sftp_details = json.loads(get_kv_secret('SFTPSecret'))

        with Connection(sftp_details['host'], username=sftp_details['username'], password = sftp_details['password'], cnopts=cnopts) as sftp:
            # read folder from SFTP
            inputfiles=sftp.listdir(download_sftp_path)
            for file in inputfiles:
                filepath=download_sftp_path+'/'+file
                print(filepath)
                x = sftp.open(filepath, 'rb').read()
                toread = io.BytesIO()
                toread.write(x)
                toread.seek(0)

                # descrypt file
                if encryption_enabled == True:
                    pgp_file = pgpy.PGPMessage().from_blob(toread.read())
                    decrypted_data = key_private.decrypt(pgp_file).message
                
                # upload file to ADLS
                azure_session.upload_blob(data=(decrypted_data if encryption_enabled == True else toread.read()), name= os.path.join(upload_adls_path,file.replace(".pgp",".txt")),overwrite=True)
            
            sftp.close()

    except Exception as e:
        print(e)

def state_snowflake_export_to_sftp(vault_key,encryption_enabled,adls_download_from,sftp_upload_to):
    try:
        # root_folder,customer_id,state,year,month,encryption_enabled,
        if encryption_enabled == True:
            public_key = base64.b64decode(get_kv_secret(vault_key)).decode('ascii')
            key_public, _ = pgpy.PGPKey.from_blob(public_key)

        azure_connection = get_azure_connection()
        blob_service_client, azure_session, account_name, sas = azure_connection

        sftp_details = json.loads(get_kv_secret('SFTPSecret'))
        blob_list = azure_session.list_blobs(adls_download_from)

        try:
            with Connection(sftp_details['host'], username=sftp_details['username'], password = sftp_details['password'], cnopts=cnopts) as sftp:
                for blob in blob_list:
                    blob_client = blob_service_client.get_blob_client(container=blob.container, 
                                                                blob=blob.name)
                    # download data into stream
                    download_stream = blob_client.download_blob()
                    bytes_data = str(download_stream.readall(), "UTF-8")

                    # file encryption 
                    if encryption_enabled == True:
                        print("Encryption start")
                        pgp_file = pgpy.PGPMessage.new(bytes_data)
                        encrypted_data = key_public.encrypt(pgp_file) 

                    # write file to SFTP
                    sftp.makedirs(sftp_upload_to)
                    file_name=os.path.basename(blob.name).split('/')[-1]
                    file_path = f"""{sftp_upload_to}/{file_name}.pgp""" if encryption_enabled == True else f"""{sftp_upload_to}/{file_name}""" 
                    sftp.putfo(StringIO(str(encrypted_data if encryption_enabled == True else bytes_data)), file_path)
            sftp.close()
        except Exception as e:
            print("SFTP connection: " + str(e))
 
    except Exception as e:
        print(e)

# state_snowflake_export_to_sftp("VA-IMMUNIZATION-PUBLIC-KEY","OUTPUT",125,"Utah",2023,5, False)
# download_from = f"""/{root_folder}/{customer_id}/{state}/{year}/{month}"""
# state_sftp_file_to_adls("VA-IMMUNIZATION-SECRET-KEY","INPUT",125,"Ohio",2023,5, False)