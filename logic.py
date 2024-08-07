import gzip
import json
import os
import time
from datetime import datetime, timedelta
from typing import Optional, List
from urllib.parse import urlparse
from uuid import uuid4, uuid1, UUID
from types import SimpleNamespace as SN

import nltk
import numpy as np
import google.generativeai as gemini
import re
from sklearn.feature_extraction.text import CountVectorizer
import tiktoken

from config import tr_data_dir
from db import DB
from util import humanize_datetime
import fingerprint


nltk.download('punkt') # needed locally; in heroku this is done in nltk.txt

gemini_key=os.environ["GEMINI_KEY"]
if not gemini_key:
    raise Exception('GEMINI_KEY environment variable not set')
gemini.configure(api_key=gemini_key)
# TODO update tiktoken and change this to 4o-mini
_tokenize = lambda st: tiktoken.encoding_for_model('gpt-3.5-turbo').encode(st, disallowed_special=())


# Chunk embedding function using Gemini
def _encode(inputs: list[str]) -> list[list[float]]:
    model = "models/text-embedding-004"
    result = gemini.embed_content(model=model, content=inputs)
    return result['embedding']


def truncate_to(source, max_tokens):
    truncated_tokens = list(_tokenize(source))[:max_tokens]
    truncated_s = tiktoken.encoding_for_model('gpt-3.5-turbo').decode(truncated_tokens)
    return truncated_s


_summarize_prompt = ("You are a helpful assistant who will give the subject of the provided web page content in a single sentence. "
                     "Give the subject in a form appropriate for an article or book title with no extra preamble or context."
                     "Examples of good responses: "
                     "`The significance of German immigrants in early Texas history`, "
                     "`The successes and shortcomings of persistent collections in server-side Java development`, "
                     "`A personal account of the benefits of intermittent fasting`.")
def summarize(text: str) -> str:
    # FIXME
    return text[:100]
    truncated = truncate_to(text, 16000)
    # openai broke this code
    response = openai.ChatCompletion.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": _summarize_prompt},
            {"role": "user", "content": truncated},
        ]
    )
    return response.choices[0].message.content


def _group_sentences_with_overlap(sentences, max_tokens):
    grouped_sentences = []
    current_group = []
    current_token_count = 0
    last_sentence = ""

    def token_length(text):
        return len(list(_tokenize(text)))

    # Group sentences in chunks of max_tokens
    for sentence in sentences:
        parts_to_process = [sentence]

        while parts_to_process:
            part = parts_to_process.pop(0)
            token_count = token_length(part)

            # If the part is too long even solo, split it
            if token_count > max_tokens:
                words = part.split()
                mid = len(words) // 2
                parts_to_process.insert(0, ' '.join(words[mid:]))
                part = ' '.join(words[:mid])
                token_count = token_length(part)

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
def _save_article(db: DB, text: str, fingerprint: np.array, url: str, title: str, user_id: uuid4, url_id: Optional[uuid1] = None) -> None:
    text = re.sub(r'\s+', ' ', text)
    title = re.sub(r'\s+', ' ', title)
    sentences = [sentence.strip() for sentence in nltk.sent_tokenize(text)]
    sentence_groups = _group_sentences_with_overlap(sentences, 100)
    group_texts = [' '.join(group) for group in sentence_groups]
    if title not in text:
        group_texts.insert(0, title)
    # print(group_texts)
    vectors = _encode(group_texts)
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


_format_prompt = ("You are a helpful assistant who will reformat raw text as html. "
                  "Add paragraphing and headings where appropriate. "
                  "Use bootstrap CSS classes.")
def _group_sentences_by_tokens(sentences, max_tokens):
    grouped_sentences = []
    current_group = []
    current_token_count = 0

    # Group sentences in chunks of max_tokens
    for sentence in sentences:
        token_count = len(list(_tokenize(sentence)))
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


def save_if_new(db: DB, url: str, title: str, text: str, user_id_str: str, url_id: Optional[uuid1] = None) -> bool:
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

    # check if the article is sufficiently different from the last version of the same url
    fp = fingerprint.encode(text)
    user_id = UUID(user_id_str)
    if db.similar_page_exists(user_id, fp):
        return False

    # generate a more useful title if necessary
    # FIXME
    # if len(title) < 15:
    #     title = summarize(text)

    # save the article in the database
    _save_article(db, text, fp, url, title, user_id, url_id)
    return True


def recent_urls(db: DB, user_id_str: str, saved_before_str: Optional[str] = None) -> tuple[list[dict[str, Optional[str]]], datetime]:
    user_id = UUID(user_id_str)
    saved_before = datetime.fromisoformat(saved_before_str) if saved_before_str else None

    limit = 10
    results = db.recent_urls(user_id, saved_before, limit)
    for result in results:
        result['saved_at'] = _uuid1_to_datetime(result['url_id'])
        result['saved_at_human'] = humanize_datetime(result['saved_at'])
    oldest_saved_at = min(result['saved_at'] for result in results) if results and len(results) == limit else None
    return [SN(**r) for r in results], oldest_saved_at


def search(db: DB, user_id_str: str, search_text: str) -> list:
    vector = _encode(['query: ' + search_text])[0]
    results = db.search(UUID(user_id_str), vector)
    for result in results:
        dt = _uuid1_to_datetime(result['url_id'])
        result['saved_at_human'] = humanize_datetime(dt)
        print(result)
    return [SN(**r) for r in results]


def load_snapshot(db: DB, user_id_str: str, url_id_str: str) -> tuple[str, str]:
    user_id = UUID(user_id_str)
    url_id = UUID(url_id_str)
    _, _, title, text_content, formatted_content = db.load_snapshot(user_id, url_id)
    return title, text_content, formatted_content
