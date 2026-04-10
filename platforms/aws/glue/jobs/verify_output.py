import boto3
from dotenv import load_dotenv
import os

load_dotenv()

s3     = boto3.client('s3', region_name='us-east-1')
bucket = os.getenv('PROCESSED_BUCKET')

print(f"\nFiles in s3://{bucket}/dev/\n")

resp = s3.list_objects_v2(Bucket=bucket, Prefix='dev/')
objects = resp.get('Contents', [])

if not objects:
    print("  No files found — Glue job may not have written output")
else:
    for obj in objects:
        size_kb = obj['Size'] / 1024
        print(f"  {obj['Key']:<70}  {size_kb:.1f} KB")

print()