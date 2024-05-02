from google.oauth2 import service_account
from google.cloud import storage
import json

from SecretManagerUtils import SecretManagerUtils

SecretManagerUtils = SecretManagerUtils()

class StorageUtils:

    def __init__(self):
        self.storage_client = self.authenticate()
    
    def authenticate(self):
        try:
            # Fetch the service account credentials from Secret Manager
            credentials_json = SecretManagerUtils.get_secret("test_fifco")
            credentials_info = json.loads(credentials_json)
            if credentials_info:
                credentials = service_account.Credentials.from_service_account_info(credentials_info)
                # Create a Storage client
                storage_client = storage.Client(credentials=credentials)
                print("Storage authentication done!")
                return storage_client 
            else:
                print("Credentials not found or invalid.")
                return None
        except Exception as e:
            print(f"Authentication failed: {e}")
            return None

    """def authenticate(self):
        try:
            # Authenticate using service account credentials
            creds = service_account.Credentials.from_service_account_file("fifco-marketing-dev-6d91fabea6e4.json")
            # Create a Storage client
            storage_client = storage.Client(credentials=creds)
            print("Storage uthentication Done!")
            return storage_client 
        except Exception as e:
            print(f"Authentication failed: {e}")
            return None"""

    def download_document(self, bucket_name: str, folder_name:str, file_name: str):
        try: 
            print(f"file: {bucket_name}/{file_name}")
            # Get the specified bucket and blob
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(f"{folder_name}/{file_name}")

            # Download the file to the local system
            blob.download_to_filename(file_name)
            print((f"file: {bucket_name}/{file_name} downloaded"))
            return(f"file: {bucket_name}/{file_name} downloaded"), None
            
        except Exception as e:
            print(f"File cannot be downloaded {e}")
            return None, f"Error: {e}"

    def list_folders_in_directory(self, bucket_name: str, directory_path: str):
        try:
            # Get the specified bucket
            bucket = self.storage_client.bucket(bucket_name)
            # Use a set to store unique folder names
            folders = set()
            # List objects in the bucket with a specific prefix (directory path)
            blobs = bucket.list_blobs(prefix=directory_path)
            # Extract folder names from the listed objects
            for blob in blobs:
                relative_path = blob.name.split(directory_path, 1)[-1].lstrip('/')
                folder_path = relative_path.split('/')[0] if '/' in relative_path else relative_path
                if folder_path:
                    folders.add(folder_path)
            # Print and return the list of folders found                    
            if folders:
                print(f"Folders in directory '{directory_path}': {', '.join(folders)}")
                return folders
            else:
                print(f"No folders found in directory '{directory_path}'")
                return set()

        except Exception as e:
            print(f"Error while listing folders: {str(e)}")
            return set()
        
    def list_folders_in_bucket(self, bucket_name: str):
        try:
            # Get the specified bucket
            bucket = self.storage_client.bucket(bucket_name)
            folders = set() 
            blobs = bucket.list_blobs()
            # Iterate through objects to extract prefixes as folders
            for blob in blobs:
                folder_path = blob.name.split('/')[0]
                if folder_path:
                    folders.add(folder_path)
            # Print and return the list of folders found                    
            if folders:
                print(f"Folders in bucket '{bucket_name}': {', '.join(folders)}")
                return folders
            else:
                print(f"No folders found in bucket '{bucket_name}'")
                return set()

        except Exception as e:
            print(f"Error while listing folders: {str(e)}")
            return set()
            
