import multiprocessing
import os
import gzip
import json
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
    # Generate clock sequence
    # 0x01b21dd213814000 is the number of 100-ns intervals between the
    # UUID epoch 1582-10-15 00:00:00 and the Unix epoch 1970-01-01 00:00:00.
    timestamp = nanoseconds // 100 + 0x01b21dd213814000
    global _last_timestamp
    if _last_timestamp is not None and timestamp <= _last_timestamp:
        timestamp = _last_timestamp + 1
    _last_timestamp = timestamp
    clock_seq = random.getrandbits(14) # instead of stable storage
    time_low = timestamp & 0xffffffff
    time_mid = (timestamp >> 32) & 0xffff
    time_hi_version = (timestamp >> 48) & 0x0fff
    clock_seq_low = clock_seq & 0xff
    clock_seq_hi_variant = (clock_seq >> 8) & 0x3f
    node = getnode()
    return UUID(fields=(time_low, time_mid, time_hi_version,
                        clock_seq_hi_variant, clock_seq_low, node), version=1)


def process_file(file, file_path):
    user_id = os.path.basename(os.path.dirname(file_path))

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
    print(f"Processing: {file_path}")
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
    # filter using multithreading, this touches disk so we want to accelerate it
    with multiprocessing.Pool() as pool:
        processed_files = pool.map(is_processed, all_files)
        files = [file for file, processed in zip(all_files, processed_files) if not processed]
        n_saved = sum(1 for r in pool.map(process_file, files) if r)
    n_already_processed = len(all_files) - len(files)
    n_duplicates = len(files) - n_saved

    print(f"Saved {n_saved} new pages, skipped {n_duplicates} duplicates, and skipped {n_already_processed} already-processed.")


if __name__ == "__main__":
    rehydrate()
