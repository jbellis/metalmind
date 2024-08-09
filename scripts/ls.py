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

    # Walk through the directory
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.gz'):
                file_path = os.path.join(root, file)
                try:
                    with gzip.open(file_path, 'rt') as f:
                        data = json.load(f)
                        url = data.get('url', 'URL not found')
                    print(f"{file}: {url}")
                except Exception as e:
                    print(f"Error processing {file}: {str(e)}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script_name.py <data_directory>")
        sys.exit(1)

    data_directory = sys.argv[1]
    print_gz_file_urls(data_directory)
