import gzip
import json
import os
import time
from datetime import datetime, timedelta
from typing import Optional, List
from uuid import uuid4, uuid1, UUID
from types import SimpleNamespace as SN

import nltk
import numpy as np
import re
from sklearn.feature_extraction.text import CountVectorizer

from config import tr_data_dir
from db import DB
from util import humanize_datetime
import fingerprint
from ai import summarize, encode, tokenize, token_length, ai_format


nltk.download('punkt') # needed locally; in heroku this is done in nltk.txt


def _group_sentences_with_overlap(sentences, max_tokens):
    grouped_sentences = []
    current_group = []
    current_token_count = 0
    last_sentence = ""

    # Group sentences in chunks of max_tokens
    for sentence in sentences:
        parts_to_process = [sentence]

        while parts_to_process:
            part = parts_to_process.pop(0)
            token_count = token_length(part)

            # If the part is too long even solo, split it
            if token_count > max_tokens:
                words = part.split()
                if len(words) >= 2:
                    mid = len(words) // 2
                    parts_to_process.insert(0, ' '.join(words[mid:]))
                    part = ' '.join(words[:mid])
                    token_count = token_length(part)
                else:
                    # one huge "word"
                    # we could split it by token but it's unlikely to be useful information
                    # so we'll just skip it
                    continue

            # Check if the previous group's last sentence should be added
            if last_sentence and current_token_count + token_length(last_sentence) <= max_tokens:
                current_group.append(last_sentence)
                current_token_count += token_length(last_sentence)

            # Add the part if it fits, otherwise start a new group
            if current_token_count + token_count <= max_tokens:
                current_group.append(part)
                current_token_count += token_count
            else:
                if current_group:
                    grouped_sentences.append(current_group)
                current_group = [part]
                current_token_count = token_count

            last_sentence = part

    # Add the last group if it's not empty
    if current_group:
        grouped_sentences.append(current_group)

    return grouped_sentences
def _clean_text(text: str) -> str:
    # collapse whitespace runs
    normalized = re.sub(r'\s+', ' ', text)
    # remove non-utf-8 characters, gemini doesn't like them
    return normalized.encode('utf-8', 'ignore').decode('utf-8')
def _save_article(db: DB, text: str, fingerprint: np.array, url: str, title: str, user_id: uuid4, url_id: Optional[uuid1] = None) -> None:
    text = _clean_text(text)
    title = _clean_text(title)
    sentences = [sentence.strip() for sentence in nltk.sent_tokenize(text)]
    sentence_groups = _group_sentences_with_overlap(sentences, 100)
    group_texts = [' '.join(group) for group in sentence_groups]
    if title not in text:
        group_texts.insert(0, title)
    # print(group_texts)
    vectors = encode(group_texts)
    db.upsert_chunks(user_id, url, title, text, fingerprint.tolist(), zip(group_texts, vectors), url_id)


def _is_different(text, last_version):
    """True if text is at least 5% different from last_version"""
    if not last_version:
        return True

    try:
        vectorizer = CountVectorizer().fit_transform([text, last_version])
    except ValueError:
        # something went wrong, err on the side of saving it
        return True
    vectors = vectorizer.toarray()
    normalized = vectors / np.linalg.norm(vectors, axis=1, keepdims=True)
    dot = np.dot(normalized[0], normalized[1])
    print("dot product between this and previous version is " + str(dot))
    return dot < 0.95


def _group_sentences_by_tokens(sentences, max_tokens):
    grouped_sentences = []
    current_group = []
    current_token_count = 0

    # Group sentences in chunks of max_tokens
    for sentence in sentences:
        token_count = token_length(sentence)
        if current_token_count + token_count <= max_tokens:
            current_group.append(sentence)
            current_token_count += token_count
        else:
            grouped_sentences.append(current_group)
            current_group = [sentence]
            current_token_count = token_count

    # Add the last group if it's not empty
    if current_group:
        grouped_sentences.append(current_group)

    return grouped_sentences


def _uuid1_to_datetime(uuid1: UUID) -> datetime:
    # UUID timestamps are in 100-nanosecond units since 15th October 1582
    return datetime(1582, 10, 15) + timedelta(microseconds=uuid1.time // 10)


sites_to_ignore = {
    'totalrecall.click',        # not sure why browser-side isn't filtering this out
    'google.com/search',        # mostly a source of false positives
    'maps.google.com',          # nothing useful
    'calendar.google.com',      # nothing useful
    'docs.google.com/document', # nothing useful
}
def save_if_new(db: DB, url: str, title: str, text: str, user_id: UUID, url_id: Optional[uuid1] = None) -> bool:
    save_locally(text, title, url, user_id)

    for site in sites_to_ignore:
        if site in url:
            return False

    # check if the article is sufficiently different from the last version of the same url
    fp = fingerprint.encode(text)
    if db.similar_page_exists(user_id, fp):
        return False

    if token_length(title) < 1:
        title = "[Untitled]"

    # save the article in the database
    _save_article(db, text, fp, url, title, user_id, url_id)
    return True


def save_locally(text, title, url, user_id):
    user_id_str = str(user_id)
    # create a filename based on the current time.  if it already exists, increment it.
    t = time.time_ns()
    while True:
        full_path = f'{tr_data_dir}/{user_id_str}/{t}.gz'
        if not os.path.exists(full_path):
            break
        t += 1
    os.makedirs(os.path.dirname(full_path), exist_ok=True)
    # json-ify the raw request
    request_json = {
        "url": url,
        "title": title,
        "text_content": text,
        "user_id": user_id_str
    }
    # write the request json to the file
    with gzip.open(full_path, 'wt') as f:
        json.dump(request_json, f)


def recent_urls(db: DB, user_id: UUID, saved_before_str: Optional[str] = None) -> tuple[list[dict[str, Optional[str]]], datetime]:
    saved_before = datetime.fromisoformat(saved_before_str) if saved_before_str else None

    limit = 10
    results = db.recent_urls(user_id, saved_before, limit)
    for result in results:
        result['saved_at'] = _uuid1_to_datetime(result['url_id'])
        result['saved_at_human'] = humanize_datetime(result['saved_at'])
    oldest_saved_at = min(result['saved_at'] for result in results) if results and len(results) == limit else None
    return [SN(**r) for r in results], oldest_saved_at


def search(db: DB, user_id_str: str, search_text: str) -> list:
    vector = encode(['query: ' + search_text])[0]
    results = db.search(UUID(user_id_str), vector)
    for result in results:
        dt = _uuid1_to_datetime(result['url_id'])
        result['saved_at_human'] = humanize_datetime(dt)
        print(result)
    return [SN(**r) for r in results]


def stream_formatted_snapshot(db: DB, user_id: UUID, url_id: UUID) -> tuple[str, str]:
    _, title, text_content, _ = db.load_snapshot(user_id, url_id)

    formatted_pieces = []
    for piece in ai_format(text_content):
        formatted_pieces.append(piece)
        yield piece
    formatted_content = ''.join(formatted_pieces)
    db.save_formatting(user_id, url_id, formatted_content)
