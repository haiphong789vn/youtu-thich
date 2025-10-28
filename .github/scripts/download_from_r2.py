"""Download video and analysis files from R2 Processing storage"""

import os
import sys
import boto3
from botocore.exceptions import ClientError

def download_from_r2():
    """Download video archive and analysis JSON from R2"""
    
    # Get environment variables
    video_id = os.environ.get('VIDEO_ID')
    r2_access_key = os.environ.get('R2_PROCESSING_ACCESS_KEY')
    r2_secret_key = os.environ.get('R2_PROCESSING_SECRET_KEY')
    r2_endpoint = os.environ.get('R2_PROCESSING_ENDPOINT')
    r2_bucket = os.environ.get('R2_PROCESSING_BUCKET')
    
    if not all([video_id, r2_access_key, r2_secret_key, r2_endpoint, r2_bucket]):
        print("❌ Missing required environment variables")
        print(f"VIDEO_ID: {'✓' if video_id else '✗'}")
        print(f"R2_PROCESSING_ACCESS_KEY: {'✓' if r2_access_key else '✗'}")
        print(f"R2_PROCESSING_SECRET_KEY: {'✓' if r2_secret_key else '✗'}")
        print(f"R2_PROCESSING_ENDPOINT: {'✓' if r2_endpoint else '✗'}")
        print(f"R2_PROCESSING_BUCKET: {'✓' if r2_bucket else '✗'}")
        sys.exit(1)
    
    print(f"📥 Downloading files for video: {video_id}")
    print(f"📦 R2 Bucket: {r2_bucket}")
    print(f"🔑 Access Key: {r2_access_key[:10]}...")
    print(f"🌐 Endpoint: {r2_endpoint}")
    print()
    
    # Initialize S3 client
    s3_client = boto3.client(
        's3',
        endpoint_url=r2_endpoint,
        aws_access_key_id=r2_access_key,
        aws_secret_access_key=r2_secret_key,
        region_name='auto'
    )
    
    # Create temp directory
    temp_dir = f"temp/{video_id}"
    os.makedirs(temp_dir, exist_ok=True)
    
    try:
        # Download video archive
        zip_key = f"{video_id}/{video_id}.zip"
        zip_path = os.path.join(temp_dir, f"{video_id}.zip")
        
        print(f"📦 Checking if file exists: {zip_key}")
        
        # Check if file exists first
        try:
            s3_client.head_object(Bucket=r2_bucket, Key=zip_key)
            print(f"✓ File exists in R2")
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                print(f"❌ File not found in R2: {zip_key}")
                print(f"   Bucket: {r2_bucket}")
                print(f"   Please ensure the video has been uploaded to R2 first")
                return False
            elif e.response['Error']['Code'] == '403':
                print(f"❌ Access denied (403 Forbidden)")
                print(f"   Key: {zip_key}")
                print(f"   Bucket: {r2_bucket}")
                print(f"   This usually means:")
                print(f"   1. R2 credentials don't have read permission")
                print(f"   2. Bucket name is incorrect")
                print(f"   3. File exists but credentials can't access it")
                print(f"   Current credentials: {r2_access_key[:10]}...")
                return False
            else:
                raise
        
        print(f"⬇️  Downloading {zip_key}...")
        s3_client.download_file(r2_bucket, zip_key, zip_path)
        
        file_size = os.path.getsize(zip_path) / (1024 * 1024)
        print(f"✅ Downloaded video archive: {file_size:.2f} MB")
        
        # Download analysis JSON
        json_key = f"{video_id}/{video_id}_analysis.json"
        json_path = os.path.join(temp_dir, f"{video_id}_analysis.json")
        
        print(f"⬇️  Downloading {json_key}...")
        s3_client.download_file(r2_bucket, json_key, json_path)
        print(f"✅ Downloaded analysis JSON")
        
        print(f"\n✅ All files downloaded successfully to: {temp_dir}")
        return True
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        print(f"\n❌ R2 Client Error ({error_code})")
        print(f"   Message: {e}")
        if error_code == '403':
            print(f"\n💡 Troubleshooting 403 Forbidden:")
            print(f"   - Verify R2_PROCESSING_ACCESS_KEY is correct")
            print(f"   - Verify R2_PROCESSING_SECRET_KEY is correct")
            print(f"   - Check R2 API token has 'Object Read' permission")
            print(f"   - Verify bucket name: {r2_bucket}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

if __name__ == '__main__':
    success = download_from_r2()
    sys.exit(0 if success else 1)
