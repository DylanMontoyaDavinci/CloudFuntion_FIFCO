import os
from google.cloud import secretmanager

PROJECT_ID = os.environ.get("PROJECT_ID", "fifco-marketing-dev")
LOCATION = os.environ.get("LOCATION", "us")
BUCKET_NAME = os.environ.get("BUCKET_NAME", "fifco-marketing-cma-dev")


class SecretManagerUtils:

    def __init__(self):
        pass

    def get_secret(self, secret_id):
        try:
            client = secretmanager.SecretManagerServiceClient()
            name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/latest"
            response = client.access_secret_version(request={"name": name})
            return response.payload.data.decode("UTF-8")
        except Exception as e:
            print(f"Error fetching secret: {e}")
            return None
def get_secret(secret_id):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        print(f"Error fetching secret: {e}")
        return None

print(get_secret("test_fifco"))
