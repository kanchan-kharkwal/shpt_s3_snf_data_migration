import boto3
import os
from dotenv import load_dotenv

# Load env file
load_dotenv("config/aws.env")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    aws_session_token=os.getenv("AWS_SESSION_TOKEN"), 
    region_name=os.getenv("AWS_DEFAULT_REGION")
)

try:
    response = s3_client.list_buckets()
    print("✅ S3 Connection Successful! Buckets:")
    for bucket in response["Buckets"]:
        print(f" - {bucket['Name']}")
except Exception as e:
    print("❌ Failed to connect to S3:", e)
