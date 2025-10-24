#!/usr/bin/env python3
"""Download video from R2 Processing bucket"""
import os
import sys
import boto3

VIDEO_ID = os.environ.get('VIDEO_ID')
R2_ACCESS_KEY_ID = os.environ.get('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.environ.get('R2_SECRET_ACCESS_KEY')
R2_ENDPOINT = os.environ.get('R2_ENDPOINT')
R2_BUCKET = os.environ.get('R2_BUCKET')

s3 = boto3.client(
    's3',
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    region_name='auto'
)

# Download ZIP file
zip_key = f"{VIDEO_ID}/{VIDEO_ID}.zip"
zip_file = f"{VIDEO_ID}.zip"

print(f"Downloading {zip_key} from R2...")
s3.download_file(R2_BUCKET, zip_key, zip_file)
print(f"✅ Downloaded: {zip_file}")

# Extract
import zipfile
with zipfile.ZipFile(zip_file, 'r') as zip_ref:
    zip_ref.extractall(VIDEO_ID)
print(f"✅ Extracted to: {VIDEO_ID}/")

# Download analysis JSON
json_key = f"{VIDEO_ID}/{VIDEO_ID}_analysis.json"
json_file = f"{VIDEO_ID}_analysis.json"

print(f"Downloading {json_key} from R2...")
s3.download_file(R2_BUCKET, json_key, json_file)
print(f"✅ Downloaded: {json_file}")
