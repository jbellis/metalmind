import os
import gzip
import json
from tqdm import tqdm
import random
from typing import Dict, Any
from uuid import UUID, getnode

from config import db, tr_data_dir
from logic import save_if_new


def is_processed(file_path: str) -> bool:
    """Check if a marker file exists for the given file path."""
    marker_path = f"{file_path}.processed"
    return os.path.exists(marker_path)


_last_timestamp = None
def uuid_from_timestamp(nanoseconds: int) -> UUID:
    """
    Create a UUID (version 1) from a nanosecond timestamp.  Mostly copied from UUID.uuid1 source.
    """
    global _last_timestamp
    timestamp = nanoseconds // 100 + 0x01b21dd213814000
    if _last_timestamp is not None and timestamp <= _last_timestamp:
        timestamp = _last_timestamp + 1
    _last_timestamp = timestamp
    clock_seq = random.getrandbits(14)
    time_low = timestamp & 0xffffffff
    time_mid = (timestamp >> 32) & 0xffff
    time_hi_version = (timestamp >> 48) & 0x0fff
    clock_seq_low = clock_seq & 0xff
    clock_seq_hi_variant = (clock_seq >> 8) & 0x3f
    node = getnode()
    return UUID(fields=(time_low, time_mid, time_hi_version,
                        clock_seq_hi_variant, clock_seq_low, node), version=1)

def process_file(file_path: str) -> Dict[str, Any]:
    try:
        user_id = os.path.basename(os.path.dirname(file_path))
        file = os.path.basename(file_path)

        # Read and parse the gzipped JSON file
        with gzip.open(file_path, 'rt') as f:
            data: Dict[str, Any] = json.load(f)
        # Extract necessary information
        url = data['url']
        title = data['title']
        text_content = data['text_content']
        # Extract timestamp from filename and create UUID
        timestamp_ns = int(file.split('.')[0])
        saved_at_uuid = uuid_from_timestamp(timestamp_ns)
        # save to db
        is_new_content = save_if_new(db, url, title, text_content, str(user_id), saved_at_uuid)
        # Mark processed
        marker_path = f"{file_path}.processed"
        open(marker_path, 'w').close()
        return {"success": True, "is_new_content": is_new_content, "file_path": file_path}
    except Exception as e:
        return {"success": False, "error": str(e), "file_path": file_path}

def rehydrate():
    # load all filenames
    all_files = []
    for root, _, files in os.walk(tr_data_dir):
        all_files.extend([os.path.join(root, fname) for fname in files if fname.endswith('.gz')])

    # filter files
    unprocessed_files = [file for file in tqdm(all_files, desc="Checking processed files") if not is_processed(file)]

    # Process files
    n_saved = 0
    n_errors = 0
    n_duplicates = 0
    for file in tqdm(unprocessed_files, desc="Processing files"):
        result = process_file(file)
        if result["success"]:
            if result["is_new_content"]:
                n_saved += 1
            else:
                n_duplicates += 1
        else:
            n_errors += 1
            print(f"Error processing file {result['file_path']}: {result['error']}")

    n_already_processed = len(all_files) - len(unprocessed_files)

    print(f"Saved {n_saved} new pages, skipped {n_duplicates} duplicates, and skipped {n_already_processed} already-processed.")
    print(f"Encountered {n_errors} errors during processing.")

if __name__ == "__main__":
    rehydrate()