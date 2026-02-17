import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
import time

# Change this to your bucket name
BUCKET_NAME = "dezoomcamp_hw4_2026_yousri"

# Authentication
CREDENTIALS_FILE = "gcs.json"
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)

# Updated BASE_URL to point to the GitHub Release assets
# Note: GitHub release files for these years are usually .csv.gz
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_"
YEARS = [2019, 2020]
MONTHS = [f"{i:02d}" for i in range(1, 13)]
DOWNLOAD_DIR = "."

CHUNK_SIZE = 8 * 1024 * 1024

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
bucket = client.bucket(BUCKET_NAME)

def download_file(year_month):
    year, month = year_month
    # Constructing the filename: yellow_tripdata_2019-01.csv.gz
    file_name = f"yellow_tripdata_{year}-{month}.csv.gz"
    url = f"{BASE_URL}{year}-{month}.csv.gz"
    file_path = os.path.join(DOWNLOAD_DIR, file_name)

    try:
        if os.path.exists(file_path):
            print(f"File already exists: {file_path}")
            return file_path
            
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")
        return file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None

def create_bucket(bucket_name):
    try:
        client.get_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' exists. Proceeding...")
    except NotFound:
        bucket = client.create_bucket(bucket_name)
        print(f"Created bucket '{bucket_name}'")
    except Forbidden:
        print(f"Bucket '{bucket_name}' exists but is inaccessible. Change the name.")
        sys.exit(1)

def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)

def upload_to_gcs(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            
            if verify_gcs_upload(blob_name):
                print(f"Uploaded and Verified: gs://{BUCKET_NAME}/{blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path}: {e}")
        
        time.sleep(5)
    print(f"Giving up on {file_path} after {max_retries} attempts.")

if __name__ == "__main__":
    create_bucket(BUCKET_NAME)

    # Create a list of tuples (year, month) for all requested data
    download_tasks = [(year, month) for year in YEARS for month in MONTHS]

    # Download files using ThreadPool
    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_file, download_tasks))

    # Upload files to GCS
    print("\nStarting GCS Uploads...")
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, filter(None, file_paths))

    print("\nAll files processed and verified.")