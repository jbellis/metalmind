import os
import gzip
import json
import random
from typing import Dict, Any
from uuid import UUID, uuid1, getnode

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


def rehydrate():
    n_already_processed = 0
    n_saved = 0
    n_duplicates = 0
    # Walk through the tr_data_dir
    for root, dirs, files in os.walk(tr_data_dir):
        for file in files:
            # 1722853248890137710.gz
            # the first part is time.ns when it was saved
            if file.endswith('.gz'):
                file_path = os.path.join(root, file)
                
                # Skip if already processed
                if is_processed(file_path):
                    n_already_processed += 1
                    continue

                user_id = os.path.basename(os.path.dirname(file_path))

                # Read and parse the gzipped JSON file
                with gzip.open(file_path, 'rt') as f:
                    data: Dict[str, Any] = json.load(f)

                # Extract necessary information
                url = data['url']
                title = data['title']
                text_content = data['text_content']
                user_id_str = data['user_id']
                # Extract timestamp from filename and create UUID
                timestamp_ns = int(file.split('.')[0])
                saved_at_uuid = uuid_from_timestamp(timestamp_ns)

                # Ensure the user_id in the filename matches the one in the JSON
                assert user_id == user_id_str, f"User ID mismatch in {file_path}"

                # save to db
                print(f"Processing: {file_path}")
                is_new_content = save_if_new(db, url, title, text_content, user_id_str, saved_at_uuid)
                # Mark processed
                marker_path = f"{file_path}.processed"
                open(marker_path, 'w').close()

                if is_new_content:
                    print(f"\tsaved!")
                    n_saved += 1
                else:
                    print(f"\tduplicate content; skipped")
                    n_duplicates += 1

    print(f"Saved {n_saved} new pages, skipped {n_duplicates} duplicates, and skipped {n_already_processed} already-processed.")

if __name__ == "__main__":
    rehydrate()
