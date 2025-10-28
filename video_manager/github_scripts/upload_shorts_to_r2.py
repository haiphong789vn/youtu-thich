#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Upload shorts to R2 storage
Uploads all shorts for a video_id to R2 bucket
"""

import os
import sys
import json
import boto3
from botocore.exceptions import ClientError
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

Base = declarative_base()

class Video(Base):
    """Video model matching the database schema"""
    __tablename__ = 'videos'
    
    id = Column(Integer, primary_key=True)
    video_id = Column(String(50), unique=True, nullable=False, index=True)
    filename = Column(String(255), nullable=False)
    title = Column(String(255))
    description = Column(Text)
    duration = Column(Float)
    r2_url = Column(String(500))
    r2_key = Column(String(500))
    tiktok_description = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

# Get credentials from environment
R2_ACCESS_KEY_ID = os.environ.get('R2_ACCESS_KEY')
R2_SECRET_ACCESS_KEY = os.environ.get('R2_SECRET_KEY')
R2_ENDPOINT = os.environ.get('R2_ENDPOINT')
R2_BUCKET = os.environ.get('R2_BUCKET')
R2_PUBLIC_URL = os.environ.get('R2_PUBLIC_URL')
VIDEO_ID = os.environ.get('VIDEO_ID')
DATABASE_URL = os.environ.get('DATABASE_URL')

# Validate required variables
if not all([VIDEO_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT, R2_BUCKET, R2_PUBLIC_URL]):
    print("❌ Missing required R2 environment variables")
    sys.exit(1)

# Validate DATABASE_URL
if not DATABASE_URL:
    print("❌ DATABASE_URL is not set!")
    print("⚠️  Please configure DATABASE_URL in GitHub Secrets")
    sys.exit(1)

# Validate DATABASE_URL format (must be cloud database, not local)
if 'localhost' in DATABASE_URL or '127.0.0.1' in DATABASE_URL or '/var/run/postgresql' in DATABASE_URL:
    print("❌ DATABASE_URL points to local database!")
    print(f"⚠️  Current DATABASE_URL: {DATABASE_URL[:50]}...")
    print("⚠️  Please use a cloud database (e.g., Neon, Supabase, Railway)")
    sys.exit(1)

# Validate it starts with postgresql
if not DATABASE_URL.startswith('postgresql'):
    print("❌ DATABASE_URL must start with 'postgresql://' or 'postgresql+pg8000://'")
    print(f"⚠️  Current DATABASE_URL: {DATABASE_URL[:50]}...")
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
print(f"🔌 Connecting to database...")
print(f"   Database host: {DATABASE_URL.split('@')[1].split('/')[0] if '@' in DATABASE_URL else 'unknown'}")

try:
    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_recycle=300,
        connect_args={
            "connect_timeout": 10,
        }
    )
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Test connection
    session.execute("SELECT 1")
    print("✅ Connected to database successfully\n")
except Exception as e:
    print(f"\n❌ Database connection failed!")
    print(f"Error: {e}\n")
    print("🔍 Troubleshooting:")
    print("  1. Make sure DATABASE_URL is set in GitHub Secrets")
    print("  2. Verify the database URL format is correct")
    print("  3. Check if database server is accessible from GitHub Actions")
    print("  4. Ensure database credentials are valid\n")
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
            r2_url = f"{R2_PUBLIC_URL}/videos/{filename}"
            short_name = filename.replace('.mp4', '')  # Remove extension for video_id
            
            # Check if already exists
            existing = session.query(Video).filter_by(video_id=short_name).first()
            if existing:
                existing.r2_url = r2_url
                existing.r2_key = key
                existing.updated_at = datetime.utcnow()
            else:
                new_video = Video(
                    video_id=short_name,
                    filename=filename,
                    r2_url=r2_url,
                    r2_key=key
                )
                session.add(new_video)
            
            session.commit()
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
if 'session' in locals():
    session.close()
    print("✓ Database connection closed")

# Exit with error if no files uploaded
if success_count == 0:
    print("❌ No files were uploaded successfully")
    sys.exit(1)

print("✅ Upload complete!")
sys.exit(0)
