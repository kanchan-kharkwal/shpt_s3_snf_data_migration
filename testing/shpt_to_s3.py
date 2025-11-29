import os
import requests
import pandas as pd
from msal import PublicClientApplication
from io import StringIO, BytesIO
from dotenv import load_dotenv
import boto3
from datetime import datetime
import time

from DBconnection import db_connections

# Load credentials 
load_dotenv(dotenv_path="config/aws.env")
load_dotenv(dotenv_path="config/shpt.env")

# --- CONFIG ---
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID") 
SHAREPOINT_SITE_PATH = os.getenv("SHAREPOINT_SITE_PATH")
BASE_PARENT_FOLDER = os.getenv("BASE_PARENT_FOLDER")
S3_BUCKET = "eda-dev-abc"
S3_KEY_BASE = "Enterprise_Metrics_Data/incoming/"

# Boto3 client
s3_client = boto3.client("s3")

# SNOWFLAKE CONNECTION
def get_snowflake_conn():
    try:
        conn = db_connections("SNOWFLAKE", "Service")
        if conn is None:
            raise Exception("Snowflake connection returned None")
        print("Connected to Snowflake inside pipeline")
        return conn
    except Exception as e:
        print(f"Failed to connect to Snowflake: {e}")
        return None

#  AUTH 
def setup_auth():
    """Authenticate with Microsoft Graph and return headers + drive_id."""
    app = PublicClientApplication(CLIENT_ID, authority=f"https://login.microsoftonline.com/{TENANT_ID}")
    # result = app.acquire_token_interactive(scopes=["Sites.Read.All", "Files.ReadWrite.All"])
    result = app.acquire_token_interactive(scopes=["Sites.Read.All"])
    access_token = result["access_token"]
    headers = {"Authorization": f"Bearer {access_token}"}

    # Get site & drive IDs
    site_resp = requests.get(f"https://graph.microsoft.com/v1.0/sites/{SHAREPOINT_SITE_PATH}", headers=headers)
    site_resp.raise_for_status()
    site_id = site_resp.json()["id"]

    drive_resp = requests.get(f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive", headers=headers)
    drive_resp.raise_for_status()
    drive_id = drive_resp.json()["id"]

    return headers, drive_id


#  HELPERS 
def download_file_with_retry(url, headers, retries=3, delay=5):
    """Retry download on 503 errors with exponential backoff."""
    for attempt in range(retries):
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            return resp
        elif resp.status_code == 503:
            print(f" Got 503, retrying in {delay} sec (attempt {attempt+1}/{retries})...")
            time.sleep(delay)
            delay *= 2
        else:
            resp.raise_for_status()
    resp.raise_for_status()


def extract_process_name(filename: str) -> str:
    """Extracts process name by removing date and extension."""
    name_only = os.path.splitext(filename)[0]
    parts = name_only.split("_")

    if parts[-1].isdigit() and len(parts[-1]) == 8:
        parts = parts[:-1]

    process_name = "_".join(parts)
    return process_name.lower()


def list_subfolders(drive_id, headers, folder_path):
    """List immediate subfolders under a given SharePoint path."""
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{folder_path}:/children"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return [item for item in resp.json()["value"] if "folder" in item]



def check_archive_folder_exists(drive_id, headers, client_path):
    """Check if Archive folder exists in the client path."""
    try:
        archive_path = f"{client_path}/Archive"
        url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{archive_path}"
        resp = requests.get(url, headers=headers)
        return resp.status_code == 200
    except:
        return False


def create_archive_folder(drive_id, headers, client_path):
    """Create Archive folder if it doesn't exist."""
    try:
        url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{client_path}:/children"
        folder_data = {
            "name": "Archive",
            "folder": {},
            "@microsoft.graph.conflictBehavior": "rename"
        }
        resp = requests.post(url, headers=headers, json=folder_data)
        resp.raise_for_status()
        print(f"Created Archive folder at: {client_path}/Archive")
        return True
    except Exception as e:
        print(f"Failed to create Archive folder: {e}")
        return False


def move_file_to_archive(drive_id, headers, file_item, source_path, client_path):
    """Move file from Incoming folder to Archive folder."""
    try:
        # Check if Archive folder exists, create if it doesn't
        archive_exists = check_archive_folder_exists(drive_id, headers, client_path)
        if not archive_exists:
            if not create_archive_folder(drive_id, headers, client_path):
                print(f"Cannot move file {file_item['name']} - Archive folder creation failed")
                return False

        # Generate timestamp for archived filename to avoid conflicts
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = file_item['name']
        name_without_ext, ext = os.path.splitext(file_name)
        archived_filename = f"{name_without_ext}_archived_{timestamp}{ext}"

        # Move file to Archive folder
        file_id = file_item['id']
        archive_path = f"{client_path}/Archive"
        
        move_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{file_id}"
        move_data = {
            "parentReference": {
                "path": f"/drive/root:/{archive_path}"
            },
            "name": archived_filename
        }
        
        resp = requests.patch(move_url, headers=headers, json=move_data)
        resp.raise_for_status()
        
        print(f" Moved file '{file_name}' to Archive as '{archived_filename}'")
        return True
        
    except Exception as e:
        print(f" Failed to move file {file_item['name']} to Archive: {e}")
        return False


# FILE CLEANING 
def remove_leading_empty_rows(df, file_type):
    if file_type in ("xls", "xlsx"):
        first_valid_row_index = df.notna().any(axis=1).idxmax()
        df = df.iloc[first_valid_row_index:]
        df.columns = df.iloc[0]
        df = df[1:]
        df = df.reset_index(drop=True)
    return df


def remove_inline_empty_rows(df):
    df = df.dropna(how="all").reset_index(drop=True)
    if df.tail(1).isna().all(axis=1).any():
        df = df.iloc[:-1]
    return df


def read_csv_with_fallback(content, encodings=("utf-8", "latin1", "cp1252")):
    for enc in encodings:
        try:
            return pd.read_csv(StringIO(content.decode(enc)), low_memory=False)
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError(" Could not decode CSV with given encodings.")


def read_file_convert_to_csv(content, file_name):
    """Convert SharePoint file content to cleaned CSV buffer."""
    try:
        ext = file_name.split(".")[-1].lower()
        if ext == "csv":
            df = read_csv_with_fallback(content)
        elif ext in ("xls", "xlsx"):
            df = pd.read_excel(BytesIO(content), header=None)
            df = remove_leading_empty_rows(df, file_type=ext)
        else:
            print(f" Skipping unsupported file type: {file_name}")
            return None

        df = remove_inline_empty_rows(df)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, lineterminator="\n")
        return csv_buffer

    except Exception as e:
        print(f" Failed to process {file_name}: {e}")
        return None


#  MAIN PROCESS 
def process_folder(drive_id, headers, folder_path, client_name, client_path, sf_conn):
    """Recursively process folder, clean files, and upload to S3."""
    folder_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{folder_path}:/children"
    resp = requests.get(folder_url, headers=headers)
    resp.raise_for_status()
    items = resp.json()["value"]

    files_to_archive = [] 

    for item in items:
        item_name = item["name"]
        item_path = f"{folder_path}/{item_name}"

        if "folder" in item:
            process_folder(drive_id, headers, item_path, client_name, client_path, sf_conn)

        elif "file" in item and item_name.lower().endswith((".csv", ".xls", ".xlsx")):
            process_name = extract_process_name(item_name)
            client_name_lower = client_name.lower()

            print(f" Client: {client_name_lower}, Process: {process_name}")
            print(f"Found file: {item_path}")

            file_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{item_path}:/content"
            file_resp = download_file_with_retry(file_url, headers)
            print("downloaded")

            try:
                csv_buffer = read_file_convert_to_csv(file_resp.content, item_name)
                if csv_buffer is None:
                    continue

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                s3_filename = f"{item_name.rsplit('.', 1)[0]}.csv"

                s3_client.put_object(
                    Bucket=S3_BUCKET,
                    Key=S3_KEY_BASE + s3_filename,
                    Body=csv_buffer.getvalue()
                )

                print(f" Uploaded to s3://{S3_BUCKET}/{S3_KEY_BASE + s3_filename}")
                files_to_archive.append(item)

                if sf_conn:
                    try:
                        cursor = sf_conn.cursor()
                        # Example: call your SP (replace with actual name/params)
                        cursor.execute(f"CALL COPY_FROM_S3_TO_RAW_TEST('{client_name}','{process_name}');")
                        print(f" Triggered Snowflake SP for {client_name} - {process_name}")
                    except Exception as e:
                        print(f" Failed to call Snowflake SP for {client_name} - {process_name}: {e}")
                    finally:
                        cursor.close()

            except Exception as e:
                print(f" Failed to process {item_path}: {e}")

    # # Move successfully processed files to Archive
    # print(f"\n Moving {len(files_to_archive)} files to Archive...")
    # for file_item in files_to_archive:
    #     move_file_to_archive(drive_id, headers, file_item, folder_path, client_path)



def main():
    headers, drive_id = setup_auth()

    sf_conn = get_snowflake_conn()

    subfolders = list_subfolders(drive_id, headers, BASE_PARENT_FOLDER)
    print(f" Found {len(subfolders)} client subfolders in {BASE_PARENT_FOLDER}")

    for folder in subfolders:
        client_name = folder["name"]
        client_path = f"{BASE_PARENT_FOLDER}/{client_name}"

        print(f"\n Processing client: {client_name}")

        client_items = list_subfolders(drive_id, headers, client_path)
        incoming_folders = [f for f in client_items if f["name"].lower() == "incoming"]

        if not incoming_folders:
            print(f" Skipping {client_name}, no Incoming folder found.")
            continue

        for incoming in incoming_folders:
            incoming_path = f"{client_path}/{incoming['name']}"
            # s3_prefix = f"{S3_KEY_BASE}{client_name.lower()}/{incoming['name'].lower()}/"

            print(f"\n Parsing Incoming folder: {incoming_path}")
            process_folder(drive_id, headers, incoming_path, client_name, client_path, sf_conn)

    if sf_conn:
        sf_conn.close()
        print(" Snowflake connection closed")


    print("\n Pipeline completed successfully!")


if __name__ == "__main__":
    main()

