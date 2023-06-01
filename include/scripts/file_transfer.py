import pysftp
import os
import io
from os import listdir
from os.path import isfile, join
import json
from azure.identity import AzureCliCredential
from azure.keyvault.secrets import SecretClient
import shutil
from datetime import datetime

import json
import urllib.parse
from azure.identity import DefaultAzureCredential, AzureCliCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.keyvault.secrets import SecretClient
import constant

KEYVAULT_SFTP_SECRET= "SFTPSecret"
KEYVAULT_BLOB_STORAGE_SECRET = "ADLSBlobConnSTR"

def get_kv_secret(secret_name):
    # connect to Azure Key vault and returns the specified secret value
    az_credential = AzureCliCredential()
    kv_client = SecretClient(vault_url=constant.KEYVAULT_URI, credential=az_credential)
    fetched_secret = kv_client.get_secret(secret_name)
    return fetched_secret.value
    
print("get_kv_secret")
sftp_details = json.loads(get_kv_secret(KEYVAULT_SFTP_SECRET))
print(sftp_details)
host=sftp_details['host']
user_name=sftp_details['username']
sftp_password=sftp_details['password']

cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

currentMonth = datetime.now().month
currentYear = datetime.now().year

def upload_files_to_sftp(pathinfo):
    try:
        print("******* upload_files_to_sftp  start *******")
        inputpath,outputpath = pathinfo
        # inputpath = "./adsl_encrypted_files/OUTPUT/125/Virginia/2023/5"
        # outputpath = "/OUTPUT/125/Virginia/2023/5"
        outputdir=outputpath.split('/')
        # outputdir=outputdir[1:]
        onlyfiles = [f for f in listdir(inputpath) if isfile(join(inputpath, f))]
        directory=''
        with pysftp.Connection(host, username=user_name, password=sftp_password, cnopts=cnopts) as sftp:
            for i in outputdir:
                directory=directory+'/'
                directory=directory+i
                if(sftp.lexists(directory)):
                    pass
                else:
                    sftp.mkdir(directory)
            for file in onlyfiles:
                ippath=os.path.join(inputpath, file)
                with sftp.cd(directory):
                    sftp.put(ippath)
        sftp.close()
        print("******* upload_files_to_sftp end *******")

    except Exception as e:
        print(e)

def download_files_from_sftp(customer_id, state, year, month):
    try:
        print("************* download_files_from_sftp started ***************")
        sftppath=f"""/INPUT/{customer_id}/{state}/{year}/{month}"""
        # downloadpath='./sftp_download_files'

        download_directory = f"""INPUT/{customer_id}/{state}/{year}/{month}"""
        local_download_folder = os.path.join('./sftp_download_files', download_directory)
        os.makedirs(local_download_folder, exist_ok=True)
        
        print("local_download_folder : " + local_download_folder)
        with pysftp.Connection(host, username=user_name, password=sftp_password, cnopts=cnopts) as sftp:
             inputfiles=sftp.listdir(sftppath)
             for file in inputfiles:
                  filepath=sftppath+'/'+file
                  local_download_folder=local_download_folder+'/'+file

                  with sftp.cd(sftppath):
                    sftp.get(filepath,local_download_folder)
        sftp.close()
        path_info = (local_download_folder, download_directory)
        print("************ download_files_from_sftp end **************")
        return path_info
    except Exception as e:
         print(e)

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

def download_blob_container(customer_id, state, year, month):
    try:
        print("******** download_blob_container start ********")
        azure_connection = get_azure_connection()
        blob_service_client, azure_session, account_name, sas = azure_connection

        download_directory = f"""OUTPUT/{customer_id}/{state}/{year}/{month}/"""

        local_download_folder = os.path.join('./adls_download', download_directory)
        os.makedirs(local_download_folder, exist_ok=True)

        blob_list = azure_session.list_blobs(download_directory)
        for blob in blob_list:
            download_file_path = os.path.join(local_download_folder, os.path.basename(blob.name).split('/')[-1]) 
            with open(file=download_file_path, mode="wb") as download_file:
                download_file.write(azure_session.download_blob(blob.name).readall())
        print("******** download_blob_container end ********")
        path_info = (local_download_folder,download_directory)
        return path_info
    except Exception as e:
            print(e)

def upload_files_to_blob_storage(upload_directory, state_has_encryption):
    try:
        print("******** upload_files_to_blob_storage start ********")
        azure_connection = get_azure_connection()
        blob_service_client, azure_session, account_name, sas = azure_connection

        try:
            input_upload = os.path.join('./sftp_decrypted_files', upload_directory) if state_has_encryption == True else os.path.join('./sftp_download_files', upload_directory)
            print(str(state_has_encryption) +" ******** upload_files_to_blob_storage from ******** " + input_upload)
            # inputpath = "./sftp_decrypted_files"
            onlyfiles = [f for f in listdir(input_upload) if isfile(join(input_upload, f))]
            for file in onlyfiles:
                with open(file=os.path.join(input_upload, file), mode="rb") as data:
                    azure_session.upload_blob(data=data, name= os.path.join(upload_directory,file),overwrite=True)
                                
            # cleaup decrypted files folder
            remove_all_files_from_path(input_upload)
            print("******** upload_files_to_blob_storage end ********")
        except Exception as e:
            print("Error at upload_files_to_blob_storage: " + str(e))
    except Exception as e:
            print("Connection error: " + str(e))

def remove_all_files_from_path(folder):
    print("***************** remove_all_files_from_path start *****************")
    print(folder)
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
            print("***************** remove_all_files_from_path end *****************")
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))
