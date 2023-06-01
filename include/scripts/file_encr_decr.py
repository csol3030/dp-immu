import pgpy
import shutil
import io
import os
from datetime import datetime
from os import listdir
from os.path import isfile, join
import base64
from azure.identity import DefaultAzureCredential, AzureCliCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.keyvault.secrets import SecretClient
import constant


KEYVAULT_SNOWFLAKE_SECRET = "SnowflakeImmunizationSecret"
DATABASE = "DEV_IMMUNIZATION_DB"
SCHEMA = "IMMUNIZATION"

currentMonth = datetime.now().month
currentYear = datetime.now().year

def get_kv_secret(secret_name):
    # connect to Azure Key vault and returns the specified secret value
    az_credential = AzureCliCredential()
    kv_client = SecretClient(vault_url=constant.KEYVAULT_URI, credential=az_credential)
    fetched_secret = kv_client.get_secret(secret_name)
    return fetched_secret.value

# /Volumes/Data/Ankit_s_Data/Ankit_Work/All_Running_Projects/2023/Data Link/poc/snowpark-test
# Define the paths to public and private keys
key_folder = "./include/scripts"

public_key = base64.b64decode(get_kv_secret('VA-IMMUNIZATION-PUBLIC-KEY')).decode('ascii')
private_key = base64.b64decode(get_kv_secret('VA-IMMUNIZATION-SECRET-KEY')).decode('ascii')

# Define paths to files you want to try decrypting
path_original_files = f"""{key_folder}/sample_0_0_0.txt"""
path_decrypted_files = f"""{key_folder}/decrypted_file.txt"""
path_encrypted_files= f"""{key_folder}/encrypted_file.gpg"""

key_public, _ = pgpy.PGPKey.from_blob(public_key)
key_private, _ = pgpy.PGPKey.from_blob(private_key)

def file_encrypt(path_original_file, path_encrypted_file, key_public):
    # Create a PGP file, compressed with ZIP DEFLATE by default unless otherwise specified
    pgp_file = pgpy.PGPMessage.new(path_original_file, file=True)

    # Encrypt the data with the public key
    encrypted_data = key_public.encrypt(pgp_file)

    # Write the encryped data to the encrypted destination
    text_file = open(path_encrypted_file, 'w')
    text_file.write(str(encrypted_data))
    text_file.close()

def file_decrypt(path_encrypted_file, path_decrypted_file, key_private):
    # Load a previously encryped message from a file
    pgp_file = pgpy.PGPMessage.from_file(path_encrypted_file)

    # Decrypt the data with the given private key
    decrypted_data = key_private.decrypt(pgp_file).message

    # Read in the bytes of the decrypted data
    toread = io.BytesIO()
    toread.write(bytes(decrypted_data, "utf-8"))
    toread.seek(0)  # reset the pointer

    # Write the data to the location
    with open(path_decrypted_file, 'wb') as f:
        shutil.copyfileobj(toread, f)
        f.close()
    
def process_folder_files_encryption(inputpath):
    try:
        print("****** process_folder_files_encryption start ******")
        local_download_folder, download_directory = inputpath
        # print("process_folder_files_encryption")
        # print(inputpath)

        output_encrypted_files = os.path.join('./adls_encrypted_files', download_directory)
        os.makedirs(output_encrypted_files, exist_ok=True)

        onlyfiles = [f for f in listdir(local_download_folder) if isfile(join(local_download_folder, f))]
        for file in onlyfiles:
            file_encrypt(os.path.join(local_download_folder, file), os.path.join(output_encrypted_files, file.replace(".txt", ".pgp")), key_public)

        remove_all_files_from_path(local_download_folder)
        path_info = (output_encrypted_files, download_directory)
        return path_info
    except Exception as e:
        print("Error at process_folder_files_encryption: " + str(e))

def process_folder_file_decryption(path_info):
    try:
        print("******* process_folder_file_decryption start *******")
        # inputpath = "./sftp_download_files"
        # outputpath = "./sftp_decrypted_files"
        local_download_folder, download_directory = path_info
        inputpath = os.path.join('./sftp_download_files', download_directory)
        outputpath = os.path.join('./sftp_decrypted_files', download_directory)

        os.makedirs(outputpath, exist_ok=True)

        onlyfiles = [f for f in listdir(inputpath) if isfile(join(inputpath, f))]
        for file in onlyfiles:
            file_decrypt(os.path.join(inputpath, file), os.path.join(outputpath, file.replace(".pgp",".txt")), key_private)

        # cleaup download folder   
        remove_all_files_from_path(inputpath)

        print("******* process_folder_file_decryption end *******")
        return download_directory
    except Exception as e:
        print("Error at process_folder_file_decryption : " + str(e))


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


# download_blob_container()
# path_info = download_blob_container()
# folder_encryption = process_folder_files_encryption(path_info)
# print(folder_encryption)
# remove_all_files_from_path(str_path)
# remove_all_files_from_path(folder_encryption)

# process_folder_files_encryption()
# process_folder_file_decryption()
# upload_files_to_blob_storage()
# file_encrypt(path_original_files, path_encrypted_files,key_public)
# file_decrypt(path_encrypted_files, path_decrypted_files,key_private)