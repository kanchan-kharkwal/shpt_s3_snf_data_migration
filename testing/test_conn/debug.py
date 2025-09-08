import os
import requests
from msal import PublicClientApplication

# --- SharePoint & Graph API Configuration ---
TENANT_ID = "e4f40be7-af4a-48c6-aca7-b06218cfc75e"
CLIENT_ID = "16d55656-56ba-4251-b4a5-7696e710fff8"
SHAREPOINT_SITE_PATH = "eversana.sharepoint.com:/teams/FIN-EVERSANAOptics"

def get_auth_token():
    """
    Acquires an access token for Microsoft Graph API.
    """
    try:
        app = PublicClientApplication(CLIENT_ID, authority=f"https://login.microsoftonline.com/{TENANT_ID}")
        # Attempt to acquire the token interactively
        result = app.acquire_token_interactive(scopes=["Sites.Read.All"])
        
        # Add debugging information to help diagnose the issue
        print("\n--- MSAL Interactive Login Result ---")
        print("result fetched ")
        print("-----------------------------------")

        if "access_token" in result:
            print("✅ Successfully acquired access token.")
            return result["access_token"]
        else:
            # Provide more specific error messages for common failure cases
            if "error_description" in result:
                print(f"❌ Error acquiring token: {result['error_description']}")
            elif "error" in result:
                print(f"❌ Error acquiring token: {result['error']}")
            else:
                print("❌ Failed to acquire access token. The result dictionary is missing the 'access_token' key.")
                print("Please ensure you completed the interactive login process in your browser.")
            return None
            
    except Exception as e:
        print(f"❌ An unexpected error occurred during authentication: {e}")
        return None

def get_site_and_drive_ids(access_token):
    """
    Retrieves the Site ID and Drive ID for the specified SharePoint site.
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    
    try:
        site_resp = requests.get(f"https://graph.microsoft.com/v1.0/sites/{SHAREPOINT_SITE_PATH}", headers=headers)
        site_resp.raise_for_status()
        site_id = site_resp.json()["id"]
        print(f"✅ Site ID: {site_id}")

        drive_resp = requests.get(f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive", headers=headers)
        drive_resp.raise_for_status()
        drive_id = drive_resp.json()["id"]
        print(f"✅ Drive ID: {drive_id}")
        return site_id, drive_id, headers
    except requests.exceptions.HTTPError as e:
        print(f"❌ Error getting site or drive IDs: {e}")
        return None, None, None

def explore_folder_structure(drive_id, headers, folder_path="", level=0):
    """
    Recursively explores and prints the folder and file structure of the SharePoint drive.
    
    Args:
        drive_id (str): The ID of the SharePoint drive.
        headers (dict): The authorization headers for the Graph API.
        folder_path (str): The path of the current folder being explored.
        level (int): The current recursion depth for indentation.
    """
    indent = "  " * level
    
    if folder_path == "":
        folder_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children"
        print(f"{indent}📁 ROOT FOLDER:")
    else:
        folder_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{folder_path}:/children"
        print(f"{indent}📁 {folder_path}:")
    
    try:
        resp = requests.get(folder_url, headers=headers)
        resp.raise_for_status()
        items = resp.json()["value"]
        
        for item in items:
            item_name = item["name"]
            if "folder" in item:
                # print(f"{indent}  📂 {item_name}/")
                # Only go 3 levels deep to avoid too much output
                if level < 3:
                    if folder_path == "":
                        new_path = item_name
                    else:
                        new_path = f"{folder_path}/{item_name}"
                    explore_folder_structure(drive_id, headers, new_path, level + 1)
            else:
                file_size = item.get("size", 0)
                print(f"{indent}  📄 {item_name} ({file_size} bytes)")
                
    except requests.exceptions.HTTPError as e:
        print(f"{indent}❌ Error accessing '{folder_path}': {e}")
    except Exception as e:
        print(f"{indent}❌ An unexpected error occurred: {e}")

if __name__ == "__main__":
    try:
        # 1. Authenticate and get necessary IDs
        access_token = get_auth_token()
        if access_token:
            site_id, drive_id, headers = get_site_and_drive_ids(access_token)
            
            if all([site_id, drive_id, headers]):
                # 2. Explore the folder structure of the site
                print("\n🔍 EXPLORING SHAREPOINT FOLDER STRUCTURE:")
                print("="*50)
                # This will start the exploration at the root folder to find the correct path
                explore_folder_structure(drive_id, headers)
            else:
                print("❌ Exiting due to failure to get SharePoint IDs.")
        else:
            print("❌ Exiting due to authentication failure.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
