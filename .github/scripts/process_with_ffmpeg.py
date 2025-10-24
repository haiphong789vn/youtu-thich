#!/usr/bin/env python3
"""Process video with FFmpeg to create shorts"""
import os
import sys
import json
import subprocess

VIDEO_ID = os.environ.get('VIDEO_ID')

# Load analysis
with open(f'{VIDEO_ID}_analysis.json', 'r', encoding='utf-8') as f:
    analysis = json.load(f)

segments = analysis.get('segments', [])
print(f"Processing {len(segments)} segments...")

# Create shorts folder
os.makedirs('shorts', exist_ok=True)

# Find video file
video_file = None
for file in os.listdir(VIDEO_ID):
    if file.endswith(('.mp4', '.mkv', '.webm')):
        video_file = os.path.join(VIDEO_ID, file)
        break

if not video_file:
    print("❌ Video file not found!")
    sys.exit(1)

print(f"Using video: {video_file}")

# Process each segment
for i, segment in enumerate(segments, 1):
    start_time = segment['start']
    end_time = segment['end']
    
    output_file = f"shorts/{VIDEO_ID}_short_{i:02d}.mp4"
    
    cmd = [
        'ffmpeg', '-y',
        '-ss', str(start_time),
        '-to', str(end_time),
        '-i', video_file,
        '-c:v', 'libx264',
        '-c:a', 'aac',
        '-strict', 'experimental',
        output_file
    ]
    
    print(f"Creating short {i}/{len(segments)}...")
    subprocess.run(cmd, check=True)
    print(f"✅ Created: {output_file}")

print(f"✅ All {len(segments)} shorts created!")
