import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))

import argparse
from uuid import UUID
from cassandra.cluster import Cluster

from config import db

def display_saved_page(user_id: UUID, url_id: UUID):
    # Fetch the saved page data
    url, title, text_content, html_content = db.load_snapshot(user_id, url_id)

    # Display the fetched data
    print(f"URL: {url}")
    print(f"Title: {title}")
    print(f"Text Content: {text_content[:500]}...")  # Display first 500 characters of text content
    print(f"HTML Content: {html_content[:500]}...")  # Display first 500 characters of HTML content


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Display a saved page from the database.")
    parser.add_argument("user_id", type=UUID, help="The user ID (UUID)")
    parser.add_argument("url_id", type=UUID, help="The URL ID (UUID)")

    args = parser.parse_args()

    display_saved_page(args.user_id, args.url_id)
