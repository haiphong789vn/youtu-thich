"""
Complete workflow: Download from YouTube → Process → Upload shorts to R2 + Database
No intermediate R2 storage needed
"""

import os
import sys
import json
import subprocess
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

def download_youtube_video(video_id, output_dir, cookies_file=None):
    """Download video from YouTube using yt-dlp"""
    print(f"📥 Downloading video from YouTube: {video_id}")
    
    os.makedirs(output_dir, exist_ok=True)
    output_template = os.path.join(output_dir, f"{video_id}.%(ext)s")
    
    cmd = [
        'yt-dlp',
        '-f', 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
        '--merge-output-format', 'mp4',
        '-o', output_template,
        '--write-info-json',
    ]
    
    if cookies_file and os.path.exists(cookies_file):
        print(f"  🍪 Using cookies: {cookies_file}")
        cmd.extend(['--cookies', cookies_file])
    
    cmd.append(f'https://www.youtube.com/watch?v={video_id}')
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        
        if result.returncode == 0:
            video_file = os.path.join(output_dir, f"{video_id}.mp4")
            if os.path.exists(video_file):
                size_mb = os.path.getsize(video_file) / (1024 * 1024)
                print(f"  ✅ Downloaded: {size_mb:.2f} MB")
                return video_file
        
        print(f"  ❌ yt-dlp failed: {result.stderr}")
        return None
        
    except subprocess.TimeoutExpired:
        print(f"  ❌ Download timeout")
        return None
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return None

def get_video_info(video_id, downloads_dir):
    """Get video info from downloaded JSON"""
    info_file = os.path.join(downloads_dir, f"{video_id}.info.json")
    
    if os.path.exists(info_file):
        with open(info_file, 'r', encoding='utf-8') as f:
            info = json.load(f)
            return {
                'title': info.get('title', video_id),
                'description': info.get('description', ''),
                'duration': info.get('duration', 0)
            }
    
    return {'title': video_id, 'description': '', 'duration': 0}

def get_video_transcript(video_id):
    """Get video transcript - simplified version"""
    print(f"📝 Fetching transcript...")
    
    try:
        # Simple approach without transcript API
        # We'll just create segments based on duration
        print(f"  ⚠️  Using duration-based segmentation (transcript not available)")
        return None
    except Exception as e:
        print(f"  ⚠️  Transcript unavailable: {e}")
        return None

def create_segments(video_id, video_file, video_info):
    """Create segments for processing"""
    print(f"📊 Creating segments...")
    
    duration = video_info.get('duration', 0)
    
    # Get actual duration using ffprobe if not available
    if duration == 0:
        try:
            cmd = [
                'ffprobe', '-v', 'error',
                '-show_entries', 'format=duration',
                '-of', 'default=noprint_wrappers=1:nokey=1',
                video_file
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            duration = float(result.stdout.strip())
        except:
            duration = 60
    
    # Create segments (60 second clips, max 10 segments)
    segments = []
    segment_duration = 60
    num_segments = min(int(duration // segment_duration), 10)
    
    title = video_info.get('title', video_id)
    
    for i in range(num_segments):
        start = i * segment_duration
        end = min(start + segment_duration, duration)
        
        segment = {
            'start': start,
            'end': end,
            'title': f"{title} - Part {i+1}",
            'description': f"Part {i+1} of {num_segments}"
        }
        segments.append(segment)
    
    print(f"  ✅ Created {len(segments)} segments")
    return segments

def sanitize_filename(title, max_length=50):
    """Sanitize filename"""
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        title = title.replace(char, '')
    title = title.replace(' ', '_')
    title = ''.join(c for c in title if c.isalnum() or c in ['_', '-'])
    return title[:max_length].strip('_')

def format_timestamp(seconds):
    """Convert seconds to HH:MM:SS format"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    ms = int((seconds % 1) * 1000)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}.{ms:03d}"

def cut_video_segment(input_file, output_file, start_time, end_time):
    """Cut video segment using FFmpeg"""
    try:
        duration = end_time - start_time
        
        cmd = [
            'ffmpeg', '-ss', format_timestamp(start_time),
            '-i', input_file,
            '-t', format_timestamp(duration),
            '-c:v', 'libx264', '-c:a', 'aac',
            '-preset', 'fast', '-crf', '23',
            '-y', output_file
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0 and os.path.exists(output_file):
            size_mb = os.path.getsize(output_file) / (1024 * 1024)
            print(f"    ✅ Created: {size_mb:.2f} MB")
            return True
        else:
            print(f"    ❌ FFmpeg error")
            return False
            
    except Exception as e:
        print(f"    ❌ Error: {e}")
        return False

def upload_shorts_to_r2_and_db(video_id, shorts_dir, video_info, r2_config, database_url):
    """Upload shorts to R2 and sync to database"""
    print(f"\n☁️  Uploading shorts to R2 and database...")
    
    # Initialize R2 client
    s3_client = boto3.client(
        's3',
        endpoint_url=r2_config['endpoint'],
        aws_access_key_id=r2_config['access_key'],
        aws_secret_access_key=r2_config['secret_key'],
        region_name='auto'
    )
    
    # Initialize database
    print(f"  🔌 Connecting to database...")
    try:
        engine = create_engine(
            database_url,
            pool_pre_ping=True,
            pool_recycle=300,
            connect_args={"connect_timeout": 10}
        )
        Session = sessionmaker(bind=engine)
        session = Session()
        session.execute("SELECT 1")
        print(f"  ✅ Connected to database")
    except Exception as e:
        print(f"  ❌ Database connection failed: {e}")
        return False
    
    # Get all shorts
    shorts = [f for f in os.listdir(shorts_dir) if f.endswith('.mp4')]
    print(f"\n  📦 Found {len(shorts)} short(s) to upload\n")
    
    uploaded = 0
    for filename in shorts:
        file_path = os.path.join(shorts_dir, filename)
        short_video_id = filename.replace('.mp4', '')
        
        print(f"  Short: {filename}")
        
        # Check if already uploaded
        existing = session.query(Video).filter_by(video_id=short_video_id).first()
        if existing and existing.r2_url:
            print(f"    ⚠️  Already uploaded, skipping")
            continue
        
        try:
            # Upload to R2
            object_key = f"videos/{filename}"
            
            with open(file_path, 'rb') as file_data:
                s3_client.put_object(
                    Bucket=r2_config['bucket'],
                    Key=object_key,
                    Body=file_data,
                    ContentType='video/mp4'
                )
            
            r2_url = f"{r2_config['public_url']}/{object_key}"
            file_size = os.path.getsize(file_path) / (1024 * 1024)
            
            print(f"    ✅ Uploaded to R2 ({file_size:.2f} MB)")
            
            # Add/update database
            if existing:
                existing.r2_url = r2_url
                existing.r2_key = object_key
                existing.updated_at = datetime.utcnow()
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
            
            session.commit()
            print(f"    ✅ Synced to database")
            uploaded += 1
            
        except Exception as e:
            print(f"    ❌ Failed: {e}")
            session.rollback()
    
    session.close()
    
    print(f"\n  ✅ Upload completed: {uploaded}/{len(shorts)} shorts")
    return uploaded > 0

def main():
    """Main workflow"""
    print("="*60)
    print("🎬 YOUTUBE VIDEO PROCESSING")
    print("="*60)
    print()
    
    # Get environment variables
    video_id = os.environ.get('VIDEO_ID')
    r2_access_key = os.environ.get('R2_SHORTS_ACCESS_KEY')
    r2_secret_key = os.environ.get('R2_SHORTS_SECRET_KEY')
    r2_endpoint = os.environ.get('R2_SHORTS_ENDPOINT')
    r2_bucket = os.environ.get('R2_SHORTS_BUCKET')
    r2_public_url = os.environ.get('R2_SHORTS_PUBLIC_URL')
    database_url = os.environ.get('DATABASE_URL')
    
    # Validate
    if not all([video_id, r2_access_key, r2_secret_key, r2_endpoint, r2_bucket, r2_public_url, database_url]):
        print("❌ Missing required environment variables")
        sys.exit(1)
    
    print(f"🎬 Video ID: {video_id}\n")
    
    # Directories
    downloads_dir = "downloads"
    shorts_dir = "shorts"
    os.makedirs(downloads_dir, exist_ok=True)
    os.makedirs(shorts_dir, exist_ok=True)
    
    # Find cookies
    cookies_file = None
    for path in ['youtube_cookies.txt', '.github/youtube_cookies.txt']:
        if os.path.exists(path):
            cookies_file = path
            break
    
    # Step 1: Download from YouTube
    video_file = download_youtube_video(video_id, downloads_dir, cookies_file)
    if not video_file:
        print("❌ Download failed")
        sys.exit(1)
    print()
    
    # Step 2: Get video info
    video_info = get_video_info(video_id, downloads_dir)
    print(f"📄 Title: {video_info['title']}")
    print(f"⏱️  Duration: {video_info['duration']:.0f}s\n")
    
    # Step 3: Get transcript (optional)
    transcript = get_video_transcript(video_id)
    print()
    
    # Step 4: Create segments
    segments = create_segments(video_id, video_file, video_info)
    print()
    
    # Step 5: Process with FFmpeg
    print(f"🎬 Processing {len(segments)} segment(s)...\n")
    
    successful = 0
    for i, segment in enumerate(segments, 1):
        safe_title = sanitize_filename(segment['title'])
        output_filename = f"{video_id}_{safe_title}_{i}.mp4"
        output_path = os.path.join(shorts_dir, output_filename)
        
        print(f"  Segment {i}/{len(segments)}: {segment['title']}")
        print(f"    Time: {segment['start']:.0f}s - {segment['end']:.0f}s")
        
        if cut_video_segment(video_file, output_path, segment['start'], segment['end']):
            successful += 1
    
    print(f"\n  ✅ Created {successful}/{len(segments)} shorts\n")
    
    if successful == 0:
        print("❌ No shorts created")
        sys.exit(1)
    
    # Step 6: Upload to R2 and database
    r2_config = {
        'access_key': r2_access_key,
        'secret_key': r2_secret_key,
        'endpoint': r2_endpoint,
        'bucket': r2_bucket,
        'public_url': r2_public_url
    }
    
    success = upload_shorts_to_r2_and_db(video_id, shorts_dir, video_info, r2_config, database_url)
    
    if success:
        print("\n" + "="*60)
        print("✅ WORKFLOW COMPLETED SUCCESSFULLY")
        print("="*60)
        sys.exit(0)
    else:
        print("\n" + "="*60)
        print("❌ WORKFLOW FAILED")
        print("="*60)
        sys.exit(1)

if __name__ == '__main__':
    main()
