import os
import json
import re
import requests
import time
import traceback
from io import StringIO
from datetime import datetime
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import threading

# Defining parameters for metastore URl, metadata file and output directory
API_URL_CMS = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "processed_csvs_output")
METADATA_FILE = os.path.join(OUTPUT_DIR, "metadata_last_update.json")

###SLEEP_INTERVAL = 86400  # 24 hours in seconds for rerun


# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)


#UDF Definitions


def log(msg):
    print(f"[{datetime.now()}] {msg}")


#Retrive last updated date from metadata file
def load_last_run_time():
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r") as f:
            data = json.load(f)
            return datetime.fromisoformat(data.get("last_run"))
    return datetime.min

#Update the last update date to metadata file
def update_last_run_time():
    with open(METADATA_FILE, "w") as f:
        json.dump({"last_run": datetime.now().isoformat()}, f)

#Convert the column names to snake case
def to_snake_case(s):
    s = re.sub(r"[^\w\s]", "", s)  # Remove special characters
    s = re.sub(r"\s+", "_", s)     # Replace spaces with underscores
    return s.lower().strip()

#Fetch the datasets from the given URL
def fetch_datasets():
    response = requests.get(API_URL_CMS)
    response.raise_for_status()
    return response.json()

#Filter the 'Hospital' datasets after last modified date
def filter_hospital_datasets(datasets, last_run_time):
    filtered = []
    for dataset in datasets:
        title = dataset.get("title", "")
        modified_str = dataset.get("modified", "")
        dist = dataset.get("distribution", [])
        csv_url = dist[0].get("downloadURL") if dist else ""

        if "hospital" in title.lower() and csv_url:
            try:
                modified = datetime.fromisoformat(modified_str.replace("Z", "+00:00"))
                if modified > last_run_time:
                    filtered.append({
                        "title": title,
                        "csv_url": csv_url,
                        "modified": modified_str
                    })
            except ValueError:
                log(f"Warning: skipping dataset '{title}' due to invalid modified date.")
    return filtered

#Processing the files
def download_and_process_csv(entry):
    title = entry["title"]
    url = entry["csv_url"]
    thread_name = threading.current_thread().name

    try:
        log(f"[{thread_name}] Downloading: {title}")
        response = requests.get(url)
        response.raise_for_status()

        df = pd.read_csv(StringIO(response.text), low_memory=False)
        df.columns = [to_snake_case(col) for col in df.columns]

        filename = to_snake_case(title) + ".csv"
        output_path = os.path.join(OUTPUT_DIR, filename)
        df.to_csv(output_path, index=False)
        log(f"[{thread_name}] Saved: {output_path}")

    except (requests.RequestException, pd.errors.ParserError, UnicodeDecodeError) as e:
        log(f"[{thread_name}] Error processing {title}: {e}")

#rerun the job for modified files
def run_etl_job():
    log("CMS Hospital ETL Job Started...")
    last_run_time = load_last_run_time()

    try:
        datasets = fetch_datasets()
        hospital_datasets = filter_hospital_datasets(datasets, last_run_time)

        log(f"Found {len(hospital_datasets)} new/updated hospital datasets.")

        if hospital_datasets:
            with ThreadPoolExecutor(max_workers=5) as executor:
                executor.map(download_and_process_csv, hospital_datasets)

            update_last_run_time()
            log("ETL job completed and metadata updated.")
        else:
            log("No updates found. Job skipped.")

    except (requests.RequestException, json.JSONDecodeError, ValueError) as e:
        log(f"ETL job failed: {type(e).__name__}: {e}")
        traceback.print_exc()

    except Exception as e:
        log(f"Unexpected error: {type(e).__name__}: {e}")
        traceback.print_exc()



if __name__ == "__main__":
    run_etl_job()
