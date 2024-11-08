import os
import sys
import gzip
import json
from pathlib import Path

def print_gz_file_urls(directory):
    # Ensure the directory exists
    if not os.path.isdir(directory):
        print(f"Error: '{directory}' is not a valid directory.")
        sys.exit(1)

    all_files = []
    for root, _, files in os.walk(directory):
        all_files.extend([os.path.join(root, fname) for fname in files if fname.endswith('.gz')])
    # sort by filename which is numeric
    all_files.sort(key=lambda x: int(Path(x).stem))
    for file_path in all_files:
        with gzip.open(file_path, 'rt') as f:
            data = json.load(f)
            url = data.get('url', 'URL not found')
        print(f"{Path(file_path).name}: {url}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script_name.py <data_directory>")
        sys.exit(1)

    data_directory = sys.argv[1]
    print_gz_file_urls(data_directory)
