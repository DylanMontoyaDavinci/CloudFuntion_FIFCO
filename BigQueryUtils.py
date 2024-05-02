from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import json

from SecretManagerUtils import SecretManagerUtils

SecretManagerUtils = SecretManagerUtils()

class BigQueryUtils:

    def __init__(self):
        self.bigquery_client = self.authenticate()

    def authenticate(self):
        try:
            # Fetch the service account credentials from Secret Manager
            credentials_json = SecretManagerUtils.get_secret("test_fifco")
            credentials_info = json.loads(credentials_json)
            if credentials_info:
                credentials = service_account.Credentials.from_service_account_info(credentials_info)
                # Create a BigQuery client
                bigquery_client = bigquery.Client(credentials=credentials, project="fifco-data-lake-dev")
                print("BigQuery authentication done!")
                return bigquery_client
            else:
                print("Credentials not found or invalid.")
                return None
        except Exception as e:
            print(f"Authentication failed: {e}")
            return None

    def create_table(self, table_id, schema):
        try:
            # Check if the table exists in BigQuery dataset
            table_ref = self.bigquery_client.dataset("test_function").table(table_id)
            self.bigquery_client.get_table(table_ref)
        except Exception as e:
            # If the table doesn't exist, create it.
            if "Not found" in str(e):
                table = bigquery.Table(table_ref, schema=schema)
                self.bigquery_client.create_table(table)
                print(f"Table {table_id} created successfully.")
                
                
    def load_csv_to_table(self, file_path, table_name, schema=None):
        # Read CSV file using Pandas
        try:
            data = pd.read_csv(file_path)
            data = data.astype(str)
            data.columns = data.columns.str.replace('/', '_').str.replace('.', '_')  # Reemplazar '/' con '_'
        except Exception as e:
            print(f"Error al leer el archivo CSV: {e}")
            return None

        # Create table or ensure its existence
        self.create_table(table_name, schema)

        # Configure job to load data from CSV to BigQuery table
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV, skip_leading_rows=0
            )

        if schema is None:
            # Si no se proporciona un esquema externo, define uno con todos los campos como STRING
            schema = [bigquery.SchemaField(column, "STRING") for column in data.columns]

        job_config.schema = schema

        table_ref = self.bigquery_client.dataset("test_function").table(table_name)

        # Load data into the table
        try:
            job = self.bigquery_client.load_table_from_dataframe(data, table_ref, job_config=job_config)
        except Exception as e:
            print(f"Load file failed: {e}")
            return None, f"Load file failed: {e}"

        try:
            job.result()
        except Exception as job_error:
            print(f"Error obtaining job result: {job_error}")
            return None, f"Error obtaining job result: {job_error}"

        # Check and print errors during the load process
        if job.error_result:
            print("Errors encountered during the load:")
            for error in job.errors:
                print(error["message"])
            return None, f"Check Logs: {error}"

        table = self.bigquery_client.get_table(table_ref)
        return f"Loaded {table.num_rows} rows and {len(table.schema)} columns", None
    
    
    
    










# from google.cloud import bigquery
# from google.oauth2 import service_account
# import pandas as pd
# import json
# import os

# from SecretManagerUtils import SecretManagerUtils

# PROJECT_ID = os.environ.get("PROJECT_ID", "fifco-data-lake-dev")
# SECRET_ID = os.environ.get("SECRET_ID", "fifco_marketing_nold_cf_secret")

# SecretManagerUtils = SecretManagerUtils()

# class BigQueryUtils:

#     def __init__(self):
#         self.bigquery_client = self.authenticate()

#     def authenticate(self):
#         try:
#             # Fetch the service account credentials from Secret Manager
#             credentials_json = SecretManagerUtils.get_secret(SECRET_ID)
#             credentials_info = json.loads(credentials_json)
#             if credentials_info:
#                 credentials = service_account.Credentials.from_service_account_info(credentials_info)
#                 # Create a BigQuery client
#                 bigquery_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
#                 print("BigQuery authentication done!")
#                 return bigquery_client
#             else:
#                 print("Credentials not found or invalid.")
#                 return None
#         except Exception as e:
#             print(f"Authentication failed: {e}")
#             return None

#     def create_table(self, table_id, schema):
#         try:
#             # Check if the table exists in BigQuery dataset
#             table_ref = self.bigquery_client.dataset("test_function").table(table_id) #fifco_raw_transactional
#             self.bigquery_client.get_table(table_ref)
#         except Exception as e:
#             # If the table doesn't exist, create it.
#             if "Not found" in str(e):
#                 table = bigquery.Table(table_ref, schema=schema)
#                 self.bigquery_client.create_table(table)
#                 print(f"Table {table_id} created successfully.")

#     def load_csv_to_table(self, file_path, table_name, schema = None):
#         # Read CSV file using Pandas
#         try:
#             data = pd.read_csv(file_path)
#             data = data.astype(str)
#             data.columns = data.columns.str.replace('/', '_').str.replace('.', '_')  # Reemplazar '/' con '_'
            
#         except Exception as e:
#             print(f"Error al leer el archivo CSV: {e}")
#             return None

#         # Create table or ensure its existence
#         self.create_table(table_name, schema)

#         # Configure job to load data from CSV to BigQuery table
#         job_config = bigquery.LoadJobConfig(
#             source_format=bigquery.SourceFormat.CSV, 
#             skip_leading_rows=0, 
#             autodetect=True
#         )
#         table_ref = self.bigquery_client.dataset("test_function").table(table_name)
        
#         # Load data into the table
#         try: 
#             job = self.bigquery_client.load_table_from_dataframe(data, table_ref, job_config=job_config)
#         except Exception as e:
#             print(f"Load file failed: {e}")
#             return None, f"Load file failed: {e}"
        
#         try:
#             job.result()
#         except Exception as job_error:
#             print(f"Error obtaining job result: {job_error}")
#             return None, f"Error obtaining job result: {e}"

#         # Check and print errors during the load process
#         if job.error_result:
#             print("Errors encountered during the load:")
#             for error in job.errors:
#                 print(error["message"])
#             return None, f"Check Logs: {error}"

#         table = self.bigquery_client.get_table(table_ref)
#         return f"Loaded {table.num_rows} rows and {len(table.schema)} columns", None