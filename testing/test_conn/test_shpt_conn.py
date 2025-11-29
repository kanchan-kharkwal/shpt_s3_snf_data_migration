import os
import requests
import pandas as pd
from msal import PublicClientApplication
from io import StringIO, BytesIO
from dotenv import load_dotenv
import boto3
from datetime import datetime
import time

# Load AWS credentials from .env
load_dotenv(dotenv_path="config/aws_creds.env")

# --- CONFIG ---
TENANT_ID = "....c75e"
CLIENT_ID = "....fff8"
SHAREPOINT_SITE_PATH = "abc.sharepoint.com:/teams/abc"
BASE_PARENT_FOLDER = "Data Integration/Source Data"
S3_BUCKET = "eda-dev-abc"
S3_KEY_BASE = "dataload/"

# Boto3 client
s3_client = boto3.client("s3")

# ================== AUTH ==================
def setup_auth():
    """Authenticate with Microsoft Graph and return headers + drive_id."""
    app = PublicClientApplication(
        CLIENT_ID, authority=f"https://login.microsoftonline.com/{TENANT_ID}"
    )
    result = app.acquire_token_interactive(scopes=["Sites.Read.All"])
    access_token = result["access_token"]
    headers = {"Authorization": f"Bearer {access_token}"}

    # Get site & drive IDs
    site_resp = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{SHAREPOINT_SITE_PATH}", headers=headers
    )
    site_resp.raise_for_status()
    site_id = site_resp.json()["id"]

    drive_resp = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive", headers=headers
    )
    drive_resp.raise_for_status()
    drive_id = drive_resp.json()["id"]

    return headers, drive_id

def download_file_with_retry(url, headers, retries=3, delay=5):
    for attempt in range(retries):
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            return resp
        elif resp.status_code == 503:
            print(f"⚠️ Got 503, retrying in {delay} sec (attempt {attempt+1}/{retries})...")
            time.sleep(delay)
            delay *= 2
        else:
            resp.raise_for_status()
    resp.raise_for_status()

def extract_process_name(filename: str) -> str:
    """Extracts process name by removing date and extension."""
    name_only = os.path.splitext(filename)[0]
    parts = name_only.split("_")

    # If last part looks like YYYYMMDD (8 digits), drop it
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


# ================== FILE CLEANING ==================
def remove_leading_empty_rows(df, file_type):
    """Removes any leading rows that are completely empty (all NaN)."""
    if file_type in ("xls", "xlsx"):
        first_valid_row_index = df.notna().any(axis=1).idxmax()
        df = df.iloc[first_valid_row_index:]
        df.columns = df.iloc[0]
        df = df[1:]
        df = df.reset_index(drop=True)
    return df


def remove_inline_empty_rows(df):
    """Removes completely empty rows in between data."""
    df = df.dropna(how="all").reset_index(drop=True)

    # Drop last row if it's empty
    if df.tail(1).isna().all(axis=1).any():
        df = df.iloc[:-1]
    return df


def read_csv_with_fallback(content, encodings=("utf-8", "latin1", "cp1252")):
    """Try multiple encodings when reading CSV."""
    for enc in encodings:
        try:
            return pd.read_csv(StringIO(content.decode(enc)), low_memory=False)
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("❌ Could not decode CSV with given encodings.")


def read_file_convert_to_csv(content, file_name):
    """
    Reads file content (CSV or Excel) into a cleaned DataFrame, returns CSV buffer.
    """
    try:
        ext = file_name.split(".")[-1].lower()

        if ext == "csv":
            df = read_csv_with_fallback(content)
        elif ext in ("xls", "xlsx"):
            df = pd.read_excel(BytesIO(content), header=None)
            df = remove_leading_empty_rows(df, file_type=ext)
        else:
            print(f"⚠️ Skipping unsupported file type: {file_name}")
            return None

        df = remove_inline_empty_rows(df)

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, lineterminator="\n")
        return csv_buffer

    except Exception as e:
        print(f"❌ Failed to process {file_name}: {e}")
        return None

def process_folder(drive_id, headers, folder_path, local_prefix, client_name):
    """Recursively process a folder and download/clean CSV/Excel files."""
    folder_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{folder_path}:/children"
    resp = requests.get(folder_url, headers=headers)
    resp.raise_for_status()
    items = resp.json()["value"]

    for item in items:
        item_name = item["name"]
        item_path = f"{folder_path}/{item_name}"
        local_path = os.path.join(local_prefix, item_name)

        if "folder" in item:
            os.makedirs(local_path, exist_ok=True)
            process_folder(drive_id, headers, item_path, local_path, client_name)

        elif "file" in item and item_name.lower().endswith((".csv", ".xls", ".xlsx")):
            process_name = extract_process_name(item_name)
            client_name_lower = client_name.lower()

            print(f"📌 Client: {client_name_lower}, Process: {process_name}")
            print(f"Found file: {item_path}")

            # Download file content
            file_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{item_path}:/content"
            file_resp = download_file_with_retry(file_url, headers)

            try:
                csv_buffer = read_file_convert_to_csv(file_resp.content, item_name)
                if csv_buffer is None:
                    continue

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                local_filename = f"{os.path.splitext(item_name)[0]}_{timestamp}.csv"
                local_file_path = os.path.join(local_prefix, local_filename)

                with open(local_file_path, "w", encoding="utf-8") as f:
                    f.write(csv_buffer.getvalue())

                s3_filename = f"{item_name.rsplit('.', 1)[0]}_{timestamp}.csv"

                # s3_client.put_object(
                #     Bucket=S3_BUCKET,
                #     Key=s3_prefix + s3_filename,
                #     Body=csv_buffer.getvalue()
                # )

                # print(f"Uploaded to s3://{S3_BUCKET}/{s3_prefix + s3_filename}")

                print(f"Saved cleaned file at {local_file_path}")
            except Exception as e:
                print(f"❌ Failed to process {item_path}: {e}")


def main():
    headers, drive_id = setup_auth()

    # Get all client subfolders
    subfolders = list_subfolders(drive_id, headers, BASE_PARENT_FOLDER)
    print(f"📂 Found {len(subfolders)} client subfolders in {BASE_PARENT_FOLDER}")

    os.makedirs(LOCAL_BASE_DIR, exist_ok=True)

    # Process Incoming folders for each client
    for folder in subfolders:
        client_name = folder["name"]
        client_path = f"{BASE_PARENT_FOLDER}/{client_name}"

        client_items = list_subfolders(drive_id, headers, client_path)
        incoming_folders = [f for f in client_items if f["name"].lower() == "incoming"]

        if not incoming_folders:
            print(f"⏭️ Skipping {client_name}, no Incoming folder found.")
            continue

        for incoming in incoming_folders:
            incoming_path = f"{client_path}/{incoming['name']}"
            local_incoming_dir = os.path.join(LOCAL_BASE_DIR, client_name, incoming["name"].lower())
            os.makedirs(local_incoming_dir, exist_ok=True)

            print(f"\n🔍 Parsing Incoming folder: {incoming_path}")
            process_folder(drive_id, headers, incoming_path, local_incoming_dir, client_name)


if __name__ == "__main__":
    main()

