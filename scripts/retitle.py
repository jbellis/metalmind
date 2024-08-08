# add parent directory of this script to sys.path
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from typing import List
from uuid import UUID
from tqdm import tqdm
from config import db
from ai import token_length, summarize


def update_page_titles():
    # Prepare the select statement
    select_query = db.session.prepare(
        f"""
        SELECT user_id, url_id, title, text_content
        FROM {db.keyspace}.{db.table_pages}
        """
    )

    # Prepare the update statement
    update_query = db.session.prepare(
        f"""
        UPDATE {db.keyspace}.{db.table_pages}
        SET title = ?
        WHERE user_id = ? AND url_id = ?
        """
    )

    # Fetch all rows
    rs = db.session.execute(select_query)

    # Process each row
    n_updated = 0
    for row in tqdm(rs, desc="Updating page titles"):
        print(f"{row.title}")
        text = row.text_content
        if token_length(row.title) < 3 and token_length(text) >= 50:
            new_title = summarize(text)
            print(f"\t-> {new_title}")
            # Update the title in the database
            # db.session.execute(update_query, (new_title, (row.user_id), (row.url_id)))
            n_updated += 1

    print(f"{n_updated} titles updated")

if __name__ == "__main__":
    update_page_titles()
