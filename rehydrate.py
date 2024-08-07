import threading
import os
import gzip
import json
from tqdm import tqdm
import random
from typing import Dict, Any
from uuid import UUID, getnode
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import db, tr_data_dir
from logic import save_if_new

def is_processed(file_path: str) -> bool:
    """Check if a marker file exists for the given file path."""
    marker_path = f"{file_path}.processed"
    return os.path.exists(marker_path)

_last_timestamp = None
_timestamp_lock = threading.Lock()

def uuid_from_timestamp(nanoseconds: int) -> UUID:
    """
    Create a UUID (version 1) from a nanosecond timestamp.  Mostly copied from UUID.uuid1 source.
    """
    global _last_timestamp
    with _timestamp_lock:
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

def process_file(file_path: str) -> bool:
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
    return is_new_content

def rehydrate():
    # load all filenames
    all_files = []
    for root, _, files in os.walk(tr_data_dir):
        all_files.extend([os.path.join(root, fname) for fname in files if fname.endswith('.gz')])

    # filter using multithreading
    unprocessed_files = []
    with ThreadPoolExecutor() as executor:
        future_to_file = {executor.submit(is_processed, file): file for file in all_files}
        for future in tqdm(as_completed(future_to_file), total=len(all_files), desc="Checking processed files"):
            file = future_to_file[future]
            if not future.result():
                unprocessed_files.append(file)

    # Process files using multithreading
    n_saved = 0
    with ThreadPoolExecutor() as executor:
        future_to_file = {executor.submit(process_file, file): file for file in unprocessed_files}
        for future in tqdm(as_completed(future_to_file), total=len(unprocessed_files), desc="Processing files"):
            if future.result():
                n_saved += 1

    n_already_processed = len(all_files) - len(unprocessed_files)
    n_duplicates = len(unprocessed_files) - n_saved

    print(f"Saved {n_saved} new pages, skipped {n_duplicates} duplicates, and skipped {n_already_processed} already-processed.")

if __name__ == "__main__":
    rehydrate()