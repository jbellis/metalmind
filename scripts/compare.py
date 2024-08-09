import util

util.update_sys_path()
import os
import sys
import gzip
import json
from pathlib import Path
import fingerprint


def compare_files(directory, master_index):
    # Ensure the directory exists
    if not os.path.isdir(directory):
        print(f"Error: '{directory}' is not a valid directory.")
        sys.exit(1)

    all_files = []
    for root, _, files in os.walk(directory):
        all_files.extend([os.path.join(root, fname) for fname in files if fname.endswith('.gz')])

    # sort by filename which is numeric
    all_files.sort(key=lambda x: int(Path(x).stem))

    # Check if the index is valid
    if master_index < 0 or master_index >= len(all_files):
        print(f"Error: Invalid index {master_index}. Must be between 0 and {len(all_files) - 1}.")
        sys.exit(1)

    # compare the fingerprint of the file at the given index with the others
    master_file_path = all_files[master_index]
    print(f"Comparing with {master_file_path}")

    with gzip.open(master_file_path, 'rt') as f:
        data = json.load(f)
    master_fingerprint = fingerprint.encode(data['text_content'])

    # Walk through the directory
    for file_path in all_files:
        with gzip.open(file_path, 'rt') as f:
            data = json.load(f)
        similarity = fingerprint.similarity(master_fingerprint, fingerprint.encode(data['text_content']))
        print(f"{Path(file_path).name}: {similarity}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script_name.py <data_directory> <index>")
        sys.exit(1)

    data_directory = sys.argv[1]
    try:
        index = int(sys.argv[2])
    except ValueError:
        print("Error: Index must be an integer.")
        sys.exit(1)

    compare_files(data_directory, index)