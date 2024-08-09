import util
util.update_sys_path()

import os
import sys
import gzip
import json
from pathlib import Path

import fingerprint


# compare the last file's fingerprint with all the others
def compare_files(directory):
    # Ensure the directory exists
    if not os.path.isdir(directory):
        print(f"Error: '{directory}' is not a valid directory.")
        sys.exit(1)

    all_files = []
    for root, _, files in os.walk(directory):
        all_files.extend([os.path.join(root, fname) for fname in files if fname.endswith('.gz')])
    # sort by filename which is numeric
    all_files.sort(key=lambda x: int(Path(x).stem))

    # compare the fingerprint of the last with the others
    first_file_path = all_files[0]
    print(f"Comparing with {first_file_path}")
    with gzip.open(first_file_path, 'rt') as f:
        data = json.load(f)
        last_fingerprint = fingerprint.encode(data['text_content'])

    # Walk through the directory
    for file_path in all_files:
        with gzip.open(file_path, 'rt') as f:
            data = json.load(f)
        similarity = fingerprint.similarity(last_fingerprint, fingerprint.encode(data['text_content']))
        print(f"{Path(file_path).name}: {similarity}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script_name.py <data_directory>")
        sys.exit(1)

    data_directory = sys.argv[1]
    compare_files(data_directory)
