"""Upload processed shorts to R2 storage and update database"""

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

def get_video_info_from_filename(filename, analysis_segments):
    """Extract video information from filename and analysis"""
    # Format: {video_id}_{title}_{segment_number}.mp4
    parts = filename.replace('.mp4', '').split('_')
    
    if len(parts) < 3:
        return None
    
    video_id = parts[0]
    segment_num = int(parts[-1])
    title_parts = parts[1:-1]
    title = ' '.join(title_parts)
    
    # Find matching segment from analysis
    if segment_num <= len(analysis_segments):
        segment = analysis_segments[segment_num - 1]
        description = segment.get('description', '')
        duration = segment.get('end', 0) - segment.get('start', 0)
    else:
        description = ''
        duration = 0
    
    return {
        'video_id': video_id,
        'title': title,
        'description': description,
        'duration': duration
    }

def upload_shorts():
    """Upload all shorts to R2 and update database"""
    
    # Get environment variables
    video_id = os.environ.get('VIDEO_ID')
    r2_access_key = os.environ.get('R2_ACCESS_KEY')
    r2_secret_key = os.environ.get('R2_SECRET_KEY')
    r2_endpoint = os.environ.get('R2_ENDPOINT')
    r2_bucket = os.environ.get('R2_BUCKET')
    r2_public_url = os.environ.get('R2_PUBLIC_URL')
    database_url = os.environ.get('DATABASE_URL')
    
    # Validate required variables
    if not all([video_id, r2_access_key, r2_secret_key, r2_endpoint, r2_bucket, r2_public_url]):
        print("❌ Missing required R2 environment variables")
        sys.exit(1)
    
    # Validate DATABASE_URL
    if not database_url:
        print("❌ DATABASE_URL is not set!")
        print("⚠️  Please configure DATABASE_URL in GitHub Secrets")
        sys.exit(1)
    
    # Validate DATABASE_URL format (must be cloud database, not local)
    if 'localhost' in database_url or '127.0.0.1' in database_url or '/var/run/postgresql' in database_url:
        print("❌ DATABASE_URL points to local database!")
        print(f"⚠️  Current DATABASE_URL: {database_url[:50]}...")
        print("⚠️  Please use a cloud database (e.g., Neon, Supabase, Railway)")
        sys.exit(1)
    
    # Validate it starts with postgresql
    if not database_url.startswith('postgresql'):
        print("❌ DATABASE_URL must start with 'postgresql://' or 'postgresql+pg8000://'")
        print(f"⚠️  Current DATABASE_URL: {database_url[:50]}...")
        sys.exit(1)
    
    print(f"\n{'='*60}")
    print(f"☁️  Uploading shorts to R2 and updating database")
    print(f"{'='*60}\n")
    
    # Initialize S3 client for shorts R2
    s3_client = boto3.client(
        's3',
        endpoint_url=r2_endpoint,
        aws_access_key_id=r2_access_key,
        aws_secret_access_key=r2_secret_key,
        region_name='auto'
    )
    
    # Initialize database connection
    print(f"🔌 Connecting to database...")
    print(f"   Database host: {database_url.split('@')[1].split('/')[0] if '@' in database_url else 'unknown'}")
    
    try:
        # Add connection arguments for cloud databases
        engine = create_engine(
            database_url,
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
    
    # Load analysis for metadata
    analysis_path = f"temp/{video_id}/{video_id}_analysis.json"
    with open(analysis_path, 'r', encoding='utf-8') as f:
        analysis = json.load(f)
    segments = analysis.get('segments', [])
    
    # Get all short files
    shorts_dir = "shorts"
    short_files = [f for f in os.listdir(shorts_dir) if f.endswith('.mp4')]
    
    print(f"📦 Found {len(short_files)} short(s) to upload\n")
    
    uploaded = 0
    skipped = 0
    failed = 0
    
    for filename in short_files:
        print(f"Processing: {filename}")
        
        file_path = os.path.join(shorts_dir, filename)
        
        # Get video info
        video_info = get_video_info_from_filename(filename, segments)
        
        if not video_info:
            print(f"  ⚠️  Could not parse filename, skipping")
            skipped += 1
            continue
        
        # Generate unique video_id for this short
        short_video_id = f"{video_info['video_id']}_short_{filename.split('_')[-1].replace('.mp4', '')}"
        
        # Check if already exists in database
        existing = session.query(Video).filter_by(video_id=short_video_id).first()
        if existing and existing.r2_url:
            print(f"  ⚠️  Already uploaded, skipping")
            skipped += 1
            continue
        
        try:
            # Upload to R2
            object_key = f"videos/{filename}"
            
            with open(file_path, 'rb') as file_data:
                s3_client.put_object(
                    Bucket=r2_bucket,
                    Key=object_key,
                    Body=file_data,
                    ContentType='video/mp4'
                )
            
            # Generate public URL
            r2_url = f"{r2_public_url}/{object_key}"
            
            file_size = os.path.getsize(file_path) / (1024 * 1024)
            print(f"  ✅ Uploaded to R2 ({file_size:.2f} MB)")
            
            # Add or update in database
            if existing:
                existing.r2_url = r2_url
                existing.r2_key = object_key
                existing.updated_at = datetime.utcnow()
                print(f"  ✅ Updated in database")
            else:
                new_video = Video(
                    video_id=short_video_id,
                    filename=filename,
                    title=video_info['title'],
                    description=video_info['description'],
                    duration=video_info['duration'],
                    r2_url=r2_url,
                    r2_key=object_key
                )
                session.add(new_video)
                print(f"  ✅ Added to database")
            
            session.commit()
            uploaded += 1
            
        except Exception as e:
            print(f"  ❌ Error: {e}")
            session.rollback()
            failed += 1
        
        print()
    
    session.close()
    
    # Summary
    print(f"{'='*60}")
    print(f"✅ Upload completed")
    print(f"📊 Summary:")
    print(f"  - Total files: {len(short_files)}")
    print(f"  - Uploaded: {uploaded}")
    print(f"  - Skipped: {skipped}")
    print(f"  - Failed: {failed}")
    print(f"{'='*60}\n")
    
    return uploaded > 0

if __name__ == '__main__':
    success = upload_shorts()
    sys.exit(0 if success else 1)
