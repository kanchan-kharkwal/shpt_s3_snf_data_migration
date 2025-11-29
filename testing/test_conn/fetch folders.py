import os
import requests
from msal import PublicClientApplication
from dotenv import load_dotenv

# Load AWS credentials (still included for future use, e.g., S3 upload)
load_dotenv(dotenv_path="config/aws_creds.env")

# SharePoint & Graph API Configuration
TENANT_ID = "....fc75e"
CLIENT_ID = "....0fff8"
SHAREPOINT_SITE_PATH = "abc.sharepoint.com:/teams/abc"

# Folder location to parse (NO "root/" prefix here)
BASE_FOLDER = "Data Integration/Source Data/Adaptive/Archive"

# Auth to Microsoft Identity
app = PublicClientApplication(
    CLIENT_ID,
    authority=f"https://login.microsoftonline.com/{TENANT_ID}"
)
result = app.acquire_token_interactive(scopes=["Sites.Read.All"])
access_token = result["access_token"]
headers = {"Authorization": f"Bearer {access_token}"}

# Get Site ID
site_resp = requests.get(f"https://graph.microsoft.com/v1.0/sites/{SHAREPOINT_SITE_PATH}", headers=headers)
site_resp.raise_for_status()
site_id = site_resp.json()["id"]

# Get Drive ID
drive_resp = requests.get(f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive", headers=headers)
drive_resp.raise_for_status()
drive_id = drive_resp.json()["id"]

# List contents of BASE_FOLDER
folder_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{BASE_FOLDER}:/children"
resp = requests.get(folder_url, headers=headers)
resp.raise_for_status()
items = resp.json().get("value", [])

print(f"📂 Files inside {BASE_FOLDER}:")
for item in items:
    if "file" in item:   # Only list files
        print(f"  {item['name']}")

