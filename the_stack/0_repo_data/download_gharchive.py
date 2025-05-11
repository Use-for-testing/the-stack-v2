import requests
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from tqdm.auto import tqdm
import os
from pathlib import Path

BUFFER_SIZE = 10 * 1024 * 1024

# Local storage path instead of S3
local_data_dir = Path("./data")
gharchive_dir = local_data_dir / "gharchive"

# Create directory if it doesn't exist
gharchive_dir.mkdir(parents=True, exist_ok=True)

# List existing files
existing_files = [str(f) for f in gharchive_dir.glob("*.json.gz")]


def download_and_upload(url):
    filename = url.split("/")[-1]
    local_path = gharchive_dir / filename
    if not filename.endswith(".json.gz"):
        return

    response = requests.head(url)
    response_size = int(response.headers.get("content-length", 0))

    # Check if file exists and has the same size
    if local_path.exists():
        local_file_size = local_path.stat().st_size
        if local_file_size == response_size:
            return

    # Download the file to local storage
    response = requests.get(url, stream=True)
    with open(local_path, "wb") as fout:
        for chunk in response.iter_content(chunk_size=BUFFER_SIZE):
            if chunk:
                fout.write(chunk)


def parse_xml(response):
    root = ET.fromstring(response.text)
    keys = [
        item.text for item in root.iter("{http://doc.s3.amazonaws.com/2006-03-01}Key")
    ]
    next_marker = root.find("{http://doc.s3.amazonaws.com/2006-03-01}NextMarker")
    if hasattr(next_marker, "text"):
        next_marker = next_marker.text
    return keys, next_marker


def get_file_urls(base_url, initial_marker):
    urls = []
    next_marker = initial_marker

    while True:
        response = requests.get(f"{base_url}/?marker={next_marker}")
        keys, next_marker = parse_xml(response)
        urls.extend([f"{base_url}/{key}" for key in keys])
        if next_marker is None:
            break
        print(urls[-1])

    return urls


if __name__ == "__main__":
    base_url = "https://data.gharchive.org"
    initial_marker = "2011-01-01-0.json.gz"
    print("Collecting urls...")
    file_urls = get_file_urls(base_url, initial_marker)
    print(f"Downloading the files to {gharchive_dir}...")
    with ThreadPoolExecutor(max_workers=64) as executor:
        list(tqdm(executor.map(download_and_upload, file_urls), total=len(file_urls)))
