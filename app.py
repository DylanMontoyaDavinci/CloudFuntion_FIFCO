import os
import json
import flask
from flask import request

from StorageUtils import StorageUtils
from BigQueryUtils import BigQueryUtils
from SecretManagerUtils import SecretManagerUtils

StorageUtils = StorageUtils()
BigQueryUtils = BigQueryUtils()

app = flask.Flask(__name__)
secret_key = os.urandom(24)
secret_key_hex = secret_key.hex()
app.secret_key = secret_key_hex

#PROJECT_ID = "fifco-data-lake-dev"
#LOCATION = "us"
#BUCKET_NAME = "fifco-marketing-cma-dev"

PROJECT_ID = os.environ.get("PROJECT_ID", "fifco-marketing-dev")
LOCATION = os.environ.get("LOCATION", "us")
BUCKET_NAME = os.environ.get("BUCKET_NAME", "fifco-marketing-cma-dev")

SecretManagerUtils = SecretManagerUtils()

@app.get("/")
def read_root():
    return {"Status": "Working DEV"}

@app.route("/app", methods=['POST'])
def entry():
    try:
        request_json = request.get_json(silent=True)

        file_name = None
        table_name = None
        folder_name = None
        
        # Extract data from request body if present
        if request_json and 'file_name' in request_json:
            file_name = request_json['file_name']
        if request_json and 'folder_name' in request_json:
            folder_name = request_json['folder_name']
        if request_json and 'table_name' in request_json:
            table_name = request_json['table_name']
        
        print(file_name)
        print(folder_name)
        print(table_name)

        # Check if required parameters are present, if not, return an error
        if file_name == None or table_name == None or folder_name == None:
            return {"Error": f"Parameters file_name, folder_name and table_name are required"}, 404
        else:
            return run(file_name, folder_name, table_name)
        
    except Exception as e:
        print(e)
        return {"error": f"Error: {e}"}, 404

def run(file_name, folder_name, table_name):
    try:
        # Download CSV file from Google Cloud Storage
        download_result, download_error = StorageUtils.download_document(bucket_name=BUCKET_NAME, 
                                                                         folder_name = folder_name, 
                                                                         file_name=file_name)
        if download_error:
            print(download_error)
            return {"error": f"{file_name} NOT dowloaded. Error: {download_error}"}, 404
        
        # Load CSV file into a BigQuery table
        load_result, load_error = BigQueryUtils.load_csv_to_table(file_path=file_name, table_name=table_name)
        if load_error:
            print(load_error)
            return {"error": f"{file_name} NOT uploaded. Error: {load_error}"}, 404
        return f"{file_name} uploaded at test_funtion:{table_name} ({load_result})", 200
    
    except Exception as e:
        print(e)
        # Handle other exceptions (if any)
        return {"error": f"{file_name} NOT uploaded Error: {e}"}, 404


@app.route("/test_secret", methods=['POST'])
def secret():
    print("TEST SECRET")
    secret_id = "test_fifco"
    credentials_file = SecretManagerUtils.get_secret(secret_id)
    cred = json.loads(credentials_file)
    print(f"cred: {cred}")


"""@app.get("/test_storage")
def setFiles():
    try:
        StorageUtils.list_folders_in_directory(bucket_name="fifco-marketing-cma-dev", directory_path = "Nielsen")
        StorageUtils.download_document(bucket_name="fifco-marketing-cma-dev", file_name="Extraccion-bases-BAS.csv")
        
        return "Storage OK", 200
    except Exception as e:
        print(e)
        # Handle other exceptions (if any)
        return {"error": f"Error {e}"}, 404
    
@app.get("/test_bigquery")
def getFiles():
    try:
        BigQueryUtils.load_csv_to_table_test(file_path="Extraccion-bases-BAS.csv")
        return "BigQuery OK", 200
    except Exception as e:
        print(e)
        # Handle other exceptions (if any)
        return {"error": f"Error {e}"}, 404"""
    

if __name__ == '__main__':
    # When running locally, disable OAuthlib's HTTPs verification.
    # ACTION ITEM for developers: debug=True
    # When running in production *do not* leave this option enabled.
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
    # Specify a hostname and port that are set as a valid redirect URI
    # for your API project in the Google API Console.
    app.run('127.0.0.1', port=3000, debug=True)


















# #import functions_framework
# import os
# import flask
# from flask import request

# from StorageUtils import StorageUtils
# from BigQueryUtils import BigQueryUtils

# StorageUtils = StorageUtils()
# BigQueryUtils = BigQueryUtils()

# PROJECT_ID = os.environ.get("PROJECT_ID", "fifco-data-lake-dev")
# LOCATION = os.environ.get("LOCATION", "us")
# BUCKET_NAME = os.environ.get("BUCKET_NAME", "fifco-marketing-cma-dev")



# #@functions_framework.http
# def app(request):
#     try:
#         request_json = request.get_json(silent=True)

#         file_name = None
#         table_name = None
#         folder_name = None
        
#         # Extract data from request body if present
#         if request_json and 'file_name' in request_json:
#             file_name = request_json['file_name']
#         if request_json and 'folder_name' in request_json:
#             folder_name = request_json['folder_name']
#         if request_json and 'table_name' in request_json:
#             table_name = request_json['table_name']
        
#         print(file_name)
#         print(folder_name)
#         print(table_name)

#         # Check if required parameters are present, if not, return an error
#         if file_name == None or table_name == None or folder_name == None:
#             return {"Error": f"Parameters file_name, folder_name and table_name are required"}, 404
#         else:
#             return run(file_name, folder_name, table_name)
        
#     except Exception as e:
#         print(e)
#         return {"error": f"Error: {e}"}, 404

# def run(file_name, folder_name, table_name):
#     try:
#         # Download CSV file from Google Cloud Storage
#         download_result, download_error = StorageUtils.download_document(bucket_name=BUCKET_NAME, folder_name = folder_name, file_name=file_name)
#         if download_error:
#             # Si hay un error en la descarga, imprimir y manejar el error
#             print(download_error)
#             return {"error": f"{file_name} NOT dowloaded. Error: {download_error}"}, 404
        
#         # Load CSV file into a BigQuery table
#         load_result, load_error = BigQueryUtils.load_csv_to_table(file_path=file_name,table_name=table_name)
#         if load_error:
#             # Si hay un error en la descarga, imprimir y manejar el error
#             print(load_error)
#             return {"error": f"{file_name} NOT uploaded. Error: {load_error}"}, 404
#         return f"{file_name} uploaded in fifco_raw_transactional:{table_name} ({load_result})", 200
    
#     except Exception as e:
#         print(e)
#         # Handle other exceptions (if any)
#         return {"error": f"{file_name} NOT uploaded Error: {e}"}, 404