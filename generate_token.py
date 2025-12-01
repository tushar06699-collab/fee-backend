from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
import os

SCOPES = ["https://www.googleapis.com/auth/drive.file"]

def create_token():
    flow = InstalledAppFlow.from_client_secrets_file(
        "credentials.json", SCOPES
    )
    creds = flow.run_local_server(port=0)  # VERY IMPORTANT

    with open("token.json", "w") as token:
        token.write(creds.to_json())

    print("token.json created successfully!")

if __name__ == "__main__":
    create_token()
