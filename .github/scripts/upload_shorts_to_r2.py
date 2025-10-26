#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Upload shorts to R2 storage
Uploads all shorts for a video_id to R2 bucket
"""

import os
import sys
import boto3
from botocore.exceptions import ClientError
import psycopg2

# Get credentials from environment
R2_ACCESS_KEY_ID = os.environ.get('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.environ.get('R2_SECRET_ACCESS_KEY')
R2_ENDPOINT = os.environ.get('R2_ENDPOINT')
R2_BUCKET = os.environ.get('R2_BUCKET')
VIDEO_ID = os.environ.get('VIDEO_ID')

# Database credentials
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT', '5432')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')

# Validate environment variables
missing = []
if not R2_ACCESS_KEY_ID:
    missing.append('R2_ACCESS_KEY_ID')
if not R2_SECRET_ACCESS_KEY:
    missing.append('R2_SECRET_ACCESS_KEY')
if not R2_ENDPOINT:
    missing.append('R2_ENDPOINT')
if not R2_BUCKET:
    missing.append('R2_BUCKET')
if not VIDEO_ID:
    missing.append('VIDEO_ID')

if missing:
    print(f"❌ Error: Missing required environment variables: {', '.join(missing)}")
    sys.exit(1)

print("=" * 80)
print(f"UPLOADING SHORTS TO R2")
print("=" * 80)
print()
print(f"📦 Bucket: {R2_BUCKET}")
print(f"🔑 Access Key: {R2_ACCESS_KEY_ID[:8]}...")
print(f"🌐 Endpoint: {R2_ENDPOINT}")
print(f"🎬 Video ID: {VIDEO_ID}")
print()

# Create S3 client
try:
    s3 = boto3.client(
        's3',
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name='auto'
    )
    print("✓ S3 client created")
except Exception as e:
    print(f"❌ Error creating S3 client: {e}")
    sys.exit(1)

# Connect to database
try:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()
    print("✓ Database connected")
except Exception as e:
    print(f"❌ Error connecting to database: {e}")
    sys.exit(1)

# Find shorts files for this video
shorts_dir = "shorts"
if not os.path.exists(shorts_dir):
    print(f"❌ Error: Shorts directory not found: {shorts_dir}")
    sys.exit(1)

# Get all shorts for this video_id
shorts_files = [f for f in os.listdir(shorts_dir) if f.startswith(VIDEO_ID) and f.endswith('.mp4')]

if not shorts_files:
    print(f"❌ Error: No shorts found for video {VIDEO_ID} in {shorts_dir}/")
    sys.exit(1)

print(f"📁 Found {len(shorts_files)} shorts to upload")
print()

# Upload each file
success_count = 0
failed_count = 0

for filename in shorts_files:
    filepath = os.path.join(shorts_dir, filename)
    
    # R2 key structure: VIDEO_ID/filename
    key = f"{VIDEO_ID}/{filename}"
    
    file_size = os.path.getsize(filepath) / (1024 * 1024)  # MB
    
    print(f"▶️  Uploading {filename} ({file_size:.2f} MB)...")
    print(f"   📍 Key: {key}")
    
    try:
        # Upload to R2
        s3.upload_file(
            filepath,
            R2_BUCKET,
            key,
            ExtraArgs={
                'ContentType': 'video/mp4',
                'Metadata': {
                    'video_id': VIDEO_ID,
                    'original_filename': filename
                }
            }
        )
        
        # Verify upload
        try:
            s3.head_object(Bucket=R2_BUCKET, Key=key)
            print(f"   ✅ Upload successful!")
            
            # Insert into database with filename
            r2_url = f"{R2_ENDPOINT}/{R2_BUCKET}/{key}"
            short_name = filename.replace('.mp4', '')  # Remove extension for video_id
            
            cur.execute("""
                INSERT INTO videos (video_id, filename, r2_url, created_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (video_id) DO UPDATE
                SET filename = EXCLUDED.filename,
                    r2_url = EXCLUDED.r2_url
            """, (short_name, filename, r2_url))
            conn.commit()
            print(f"   ✓ Database updated")
            
            success_count += 1
        except ClientError:
            print(f"   ⚠️  Uploaded but verification failed")
            success_count += 1
            
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_msg = e.response['Error']['Message']
        print(f"   ❌ Upload failed: {error_code} - {error_msg}")
        
        if error_code == 'InvalidAccessKeyId':
            print(f"   💡 Invalid Access Key ID")
            print(f"   → Check R2_ACCESS_KEY_ID secret in GitHub")
        elif error_code == 'SignatureDoesNotMatch':
            print(f"   💡 Invalid Secret Access Key")
            print(f"   → Check R2_SECRET_ACCESS_KEY secret in GitHub")
        elif error_code in ['Unauthorized', 'AccessDenied']:
            print(f"   💡 No permission to upload")
            print(f"   → Check R2 API token permissions (need Object Write)")
        
        failed_count += 1
        continue
        
    except Exception as e:
        print(f"   ❌ Unexpected error: {e}")
        failed_count += 1
        continue
    
    print()

# Summary
print("=" * 80)
print("UPLOAD SUMMARY")
print("=" * 80)
print(f"✅ Successful: {success_count}/{len(shorts_files)}")
print(f"❌ Failed: {failed_count}/{len(shorts_files)}")
print()

if success_count > 0:
    print(f"📦 Uploaded to: {R2_ENDPOINT}/{R2_BUCKET}/{VIDEO_ID}/")
print("="  * 80)

# Close database connection
if 'cur' in locals():
    cur.close()
if 'conn' in locals():
    conn.close()
    print("✓ Database connection closed")

# Exit with error if no files uploaded
if success_count == 0:
    print("❌ No files were uploaded successfully")
    sys.exit(1)

print("✅ Upload complete!")
sys.exit(0)
