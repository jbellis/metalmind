import sys
import time
import gzip
import json
import os
from uuid import UUID

# Too much work to fight config to get this working
# import scriptutil
# scriptutil.update_sys_path()
# from logic import save_locally
tr_data_dir = 'data/mock'
def save_locally(text, title, url):
    user_id = UUID('9fad21ec-48e0-493d-bff0-008e52d46cee')
    # create a filename based on the current time.  if it already exists, increment it.
    t = time.time_ns()
    while True:
        full_path = f'{tr_data_dir}/{t}.gz'
        if not os.path.exists(full_path):
            break
        t += 1
    os.makedirs(os.path.dirname(full_path), exist_ok=True)
    # json-ify the raw request
    request_json = {
        "url": url,
        "title": title,
        "text_content": text,
        "user_id": str(user_id)
    }
    # write the request json to the file
    with gzip.open(full_path, 'wt') as f:
        json.dump(request_json, f)

# save text from stdin as a mock user
text = sys.stdin.read()
save_locally(text, 'Lorem Ipsum', 'http://example.com')