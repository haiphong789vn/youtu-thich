"""
Download video from YouTube and upload to R2 Processing bucket
This is the first step in the workflow
"""

import os
import sys
import json
import subprocess
import zipfile
import boto3
from botocore.exceptions import ClientError

def download_youtube_video(video_id, output_dir, cookies_file=None):
    """Download video from YouTube using yt-dlp"""
    print(f"📥 Downloading video from YouTube: {video_id}")
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Output template
    output_template = os.path.join(output_dir, f"{video_id}.%(ext)s")
    
    # yt-dlp command
    cmd = [
        'yt-dlp',
        '-f', 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
        '--merge-output-format', 'mp4',
        '-o', output_template,
        '--write-info-json',
        '--write-description',
        '--write-thumbnail',
    ]
    
    # Add cookies if available
    if cookies_file and os.path.exists(cookies_file):
        print(f"  Using cookies file: {cookies_file}")
        cmd.extend(['--cookies', cookies_file])
    else:
        print(f"  ⚠️  No cookies file, may encounter bot detection")
    
    cmd.append(f'https://www.youtube.com/watch?v={video_id}')
    
    print(f"  Executing: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        
        if result.returncode == 0:
            # Find the downloaded video file
            video_file = os.path.join(output_dir, f"{video_id}.mp4")
            if os.path.exists(video_file):
                size_mb = os.path.getsize(video_file) / (1024 * 1024)
                print(f"✅ Downloaded: {size_mb:.2f} MB")
                return video_file
            else:
                print(f"❌ Video file not found: {video_file}")
                return None
        else:
            print(f"❌ yt-dlp failed: {result.stderr}")
            return None
            
    except subprocess.TimeoutExpired:
        print(f"❌ Download timeout (600 seconds)")
        return None
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def get_video_transcript(video_id):
    """Get video transcript using youtube-transcript-api"""
    print(f"📝 Fetching transcript for: {video_id}")
    
    try:
        from youtube_transcript_api import YouTubeTranscriptApi
        
        # Try to get transcript
        transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
        
        # Try manual first, then auto-generated
        try:
            transcript = transcript_list.find_manually_created_transcript(['vi', 'en'])
        except:
            transcript = transcript_list.find_generated_transcript(['vi', 'en'])
        
        transcript_data = transcript.fetch()
        
        print(f"✅ Fetched {len(transcript_data)} transcript entries")
        return transcript_data
        
    except Exception as e:
        print(f"⚠️  Could not fetch transcript: {e}")
        return None

def create_analysis_json(video_id, video_file, transcript, output_dir):
    """Create analysis JSON file"""
    print(f"📊 Creating analysis file...")
    
    # Get video duration using ffprobe
    try:
        cmd = [
            'ffprobe',
            '-v', 'error',
            '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            video_file
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        duration = float(result.stdout.strip())
    except:
        duration = 0
    
    # Read video info if available
    info_file = os.path.join(output_dir, f"{video_id}.info.json")
    title = video_id
    description = ""
    
    if os.path.exists(info_file):
        with open(info_file, 'r', encoding='utf-8') as f:
            info = json.load(f)
            title = info.get('title', video_id)
            description = info.get('description', '')
    
    # Create basic analysis structure
    analysis = {
        "video_id": video_id,
        "title": title,
        "description": description,
        "duration": duration,
        "transcript": transcript if transcript else [],
        "segments": []
    }
    
    # If we have transcript, create segments (simple split every 60 seconds)
    if transcript and duration > 0:
        segment_duration = 60  # 60 second segments
        num_segments = int(duration // segment_duration)
        
        for i in range(min(num_segments, 10)):  # Max 10 segments
            start = i * segment_duration
            end = min(start + segment_duration, duration)
            
            # Get transcript text for this segment
            segment_text = ""
            for entry in transcript:
                if start <= entry['start'] < end:
                    segment_text += entry['text'] + " "
            
            segment = {
                "start": start,
                "end": end,
                "duration": end - start,
                "title": f"{title} - Part {i+1}",
                "description": segment_text.strip()[:200] if segment_text else f"Segment {i+1}",
                "reason": "Auto-generated segment"
            }
            analysis["segments"].append(segment)
    
    # Save analysis file
    analysis_file = os.path.join(output_dir, f"{video_id}_analysis.json")
    with open(analysis_file, 'w', encoding='utf-8') as f:
        json.dump(analysis, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Created analysis with {len(analysis['segments'])} segments")
    return analysis_file

def create_zip_archive(video_id, video_file, output_dir):
    """Create ZIP archive of the video"""
    print(f"📦 Creating ZIP archive...")
    
    zip_path = os.path.join(output_dir, f"{video_id}.zip")
    
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Add video file
        zipf.write(video_file, os.path.basename(video_file))
        
        # Add info files if they exist
        for ext in ['.info.json', '.description', '.jpg', '.webp']:
            info_file = os.path.join(output_dir, f"{video_id}{ext}")
            if os.path.exists(info_file):
                zipf.write(info_file, os.path.basename(info_file))
    
    size_mb = os.path.getsize(zip_path) / (1024 * 1024)
    print(f"✅ Created archive: {size_mb:.2f} MB")
    return zip_path

def upload_to_r2(video_id, zip_path, analysis_path, r2_config):
    """Upload files to R2"""
    print(f"☁️  Uploading to R2...")
    
    # Initialize S3 client
    s3 = boto3.client(
        's3',
        endpoint_url=r2_config['endpoint'],
        aws_access_key_id=r2_config['access_key'],
        aws_secret_access_key=r2_config['secret_key'],
        region_name='auto'
    )
    
    bucket = r2_config['bucket']
    
    try:
        # Upload ZIP file
        zip_key = f"{video_id}/{video_id}.zip"
        print(f"  Uploading: {zip_key}")
        s3.upload_file(zip_path, bucket, zip_key)
        print(f"  ✅ Uploaded ZIP")
        
        # Upload analysis JSON
        json_key = f"{video_id}/{video_id}_analysis.json"
        print(f"  Uploading: {json_key}")
        s3.upload_file(analysis_path, bucket, json_key)
        print(f"  ✅ Uploaded analysis")
        
        print(f"✅ All files uploaded to R2")
        return True
        
    except ClientError as e:
        print(f"❌ R2 upload failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def main():
    """Main workflow"""
    print("="*60)
    print("📥 DOWNLOAD FROM YOUTUBE & UPLOAD TO R2")
    print("="*60)
    print()
    
    # Get environment variables
    video_id = os.environ.get('VIDEO_ID')
    r2_access_key = os.environ.get('R2_PROCESSING_ACCESS_KEY')
    r2_secret_key = os.environ.get('R2_PROCESSING_SECRET_KEY')
    r2_endpoint = os.environ.get('R2_PROCESSING_ENDPOINT')
    r2_bucket = os.environ.get('R2_PROCESSING_BUCKET')
    
    # Validate
    if not all([video_id, r2_access_key, r2_secret_key, r2_endpoint, r2_bucket]):
        print("❌ Missing required environment variables")
        print(f"VIDEO_ID: {'✓' if video_id else '✗'}")
        print(f"R2_PROCESSING_ACCESS_KEY: {'✓' if r2_access_key else '✗'}")
        print(f"R2_PROCESSING_SECRET_KEY: {'✓' if r2_secret_key else '✗'}")
        print(f"R2_PROCESSING_ENDPOINT: {'✓' if r2_endpoint else '✗'}")
        print(f"R2_PROCESSING_BUCKET: {'✓' if r2_bucket else '✗'}")
        sys.exit(1)
    
    print(f"🎬 Video ID: {video_id}")
    print(f"📦 R2 Bucket: {r2_bucket}")
    print()
    
    # Create temp directory
    temp_dir = f"temp/{video_id}"
    os.makedirs(temp_dir, exist_ok=True)
    
    # Check for cookies file (in repository root or current directory)
    cookies_file = None
    possible_paths = ['youtube_cookies.txt', '.github/youtube_cookies.txt', '../youtube_cookies.txt']
    for path in possible_paths:
        if os.path.exists(path):
            cookies_file = path
            print(f"🍪 Found cookies file: {path}")
            break
    
    if not cookies_file:
        print(f"⚠️  No cookies file found. YouTube may block download.")
        print(f"   Searched in: {', '.join(possible_paths)}")
    
    print()
    
    # Step 1: Download from YouTube
    video_file = download_youtube_video(video_id, temp_dir, cookies_file)
    if not video_file:
        print("❌ Failed to download video")
        sys.exit(1)
    print()
    
    # Step 2: Get transcript
    transcript = get_video_transcript(video_id)
    print()
    
    # Step 3: Create analysis
    analysis_file = create_analysis_json(video_id, video_file, transcript, temp_dir)
    print()
    
    # Step 4: Create ZIP archive
    zip_file = create_zip_archive(video_id, video_file, temp_dir)
    print()
    
    # Step 5: Upload to R2
    r2_config = {
        'access_key': r2_access_key,
        'secret_key': r2_secret_key,
        'endpoint': r2_endpoint,
        'bucket': r2_bucket
    }
    
    success = upload_to_r2(video_id, zip_file, analysis_file, r2_config)
    
    if success:
        print()
        print("="*60)
        print("✅ WORKFLOW COMPLETED SUCCESSFULLY")
        print("="*60)
        print(f"Video {video_id} is now ready for processing")
        sys.exit(0)
    else:
        print()
        print("="*60)
        print("❌ WORKFLOW FAILED")
        print("="*60)
        sys.exit(1)

if __name__ == '__main__':
    main()
