import pysftp
import os
import io
from os import listdir
from os.path import isfile, join
import json
from azure.identity import AzureCliCredential
from azure.keyvault.secrets import SecretClient

from datetime import datetime

import json
import urllib.parse

KEYVAULT_URI = "https://kv-datalink-dp-pilot.vault.azure.net"
KEYVAULT_SFTP_SECRET= "SFTPSecret"

def get_kv_secret(secret_name):
    # connect to Azure Key vault and returns the specified secret value
    az_credential = AzureCliCredential()
    kv_client = SecretClient(vault_url=KEYVAULT_URI, credential=az_credential)
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

def get_adls_encrypted_files(pathinfo):
    try:
        print("******* get_adls_encrypted_files *******")
        inputpath,outputpath = pathinfo
        # inputpath = "./adsl_encrypted_files/OUTPUT/125/Virginia/2023/5"
        # outputpath = "/OUTPUT/125/Virginia/2023/5"
        print(inputpath)
        print(outputpath)
        outputdir=outputpath.split('/')
        # outputdir=outputdir[1:]
        onlyfiles = [f for f in listdir(inputpath) if isfile(join(inputpath, f))]
        print(inputpath)
        print(onlyfiles)
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
        print("******* End get_adls_encrypted_files *******")

    except Exception as e:
        print(e)

def get_sftp_download_files():
    try:
        print("*************downloading started***************")
        sftppath="/INPUT/125/VIRGINIA/2023/23"
        # downloadpath='./sftp_download_files'

        download_directory = f"""INPUT/125/VIRGINIA/{currentYear}/{currentMonth}"""
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
        print(path_info)
        print("************downloaded**************")
        return path_info
    except Exception as e:
         print(e)

# get_sftp_download_files()

        # sftppath= f"""/INPUT/{customer_id}/{state}/{year}/{month}"""
        # # downloadpath='./sftp_download_files'

        # download_directory = f"""INPUT/{customer_id}/{state}/{year}/{month}"""
