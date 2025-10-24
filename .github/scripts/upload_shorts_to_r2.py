#!/usr/bin/env python3
"""Upload shorts to R2 Shorts bucket and save to database"""
import os
import sys
import boto3
import psycopg2
from datetime import datetime

VIDEO_ID = os.environ.get('VIDEO_ID')
R2_ACCESS_KEY_ID = os.environ.get('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.environ.get('R2_SECRET_ACCESS_KEY')
R2_ENDPOINT = os.environ.get('R2_ENDPOINT')
R2_BUCKET = os.environ.get('R2_BUCKET')
DATABASE_URL = os.environ.get('DATABASE_URL')

# R2 client
s3 = boto3.client(
    's3',
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    region_name='auto'
)

# Database connection
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# Upload shorts
shorts_folder = 'shorts'
uploaded = []

for filename in os.listdir(shorts_folder):
    if filename.endswith('.mp4'):
        filepath = os.path.join(shorts_folder, filename)
        
        # Upload to R2
        key = f"{VIDEO_ID}/{filename}"
        print(f"Uploading {filename} to R2...")
        s3.upload_file(filepath, R2_BUCKET, key)
        
        # Get public URL
        r2_url = f"{R2_ENDPOINT.replace('.r2.cloudflarestorage.com', '.r2.dev')}/{R2_BUCKET}/{key}"
        
        # Save to database
        video_id_full = filename.replace('.mp4', '')
        cur.execute("""
            INSERT INTO videos (video_id, r2_url, created_at)
            VALUES (%s, %s, %s)
            ON CONFLICT (video_id) DO UPDATE
            SET r2_url = EXCLUDED.r2_url
        """, (video_id_full, r2_url, datetime.utcnow()))
        
        uploaded.append(filename)
        print(f"✅ Uploaded: {filename}")

conn.commit()
cur.close()
conn.close()

print(f"✅ All {len(uploaded)} shorts uploaded and saved to database!")
