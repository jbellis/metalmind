import os
from typing import List, Generator

import openai as oai
import nltk
import tiktoken
import google.generativeai as gemini

# OpenAI client setup, used for text generation
openai = oai.OpenAI()

# Gemini client setup, used for embeddings
gemini_key = os.environ["GEMINI_KEY"]
if not gemini_key:
    raise Exception('GEMINI_KEY environment variable not set')
gemini.configure(api_key=gemini_key)

tiktoken_model = tiktoken.encoding_for_model('gpt-4')
def tokenize(text: str) -> List[int]:
    return tiktoken_model.encode(text, disallowed_special=())
def token_length(text: str) -> int:
    return len(list(tokenize(text)))
def truncate_to(text, max_tokens):
    truncated_tokens = list(tokenize(text))[:max_tokens]
    truncated_s = tiktoken_model.decode(truncated_tokens)
    return truncated_s

# Chunk embedding function using Gemini
def encode(inputs: list[str]) -> list[list[float]]:
    model = "models/text-embedding-004"
    result = gemini.embed_content(model=model, content=inputs)
    return result['embedding']

_summarize_prompt = ("You are an assistant who will give the subject of the provided web page content in as few words as possible. "
                     "Give the subject in a form appropriate for an article or book title with no extra preamble or context."
                     "Examples of good responses: "
                     "`Significance of German immigrants in early Texas history`, "
                     "`Successes and shortcomings of persistent collections in server-side Java development`, "
                     "`Personal account of the benefits of intermittent fasting`.")

def summarize(text: str) -> str:
    truncated = truncate_to(text, 10_000)
    response = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": _summarize_prompt},
            {"role": "user", "content": truncated},
        ]
    )
    return response.choices[0].message.content

_format_prompt = ("You are an assistant who reformats raw text as html. "
                  "Add paragraphing, headings, and tables where appropriate. "
                  "Use Pico CSS classes.")
def _group_sentences_by_tokens(sentences: List[str], max_tokens: int) -> List[List[str]]:
    grouped_sentences = []
    current_group = []
    current_token_count = 0

    for sentence in sentences:
        token_count = len(list(tokenize(sentence)))
        if current_token_count + token_count <= max_tokens:
            current_group.append(sentence)
            current_token_count += token_count
        else:
            grouped_sentences.append(current_group)
            current_group = [sentence]
            current_token_count = token_count

    if current_group:
        grouped_sentences.append(current_group)

    return grouped_sentences

def ai_format(text_content: str) -> Generator[str, None, None]:
    sentences = [sentence.strip() for sentence in nltk.sent_tokenize(text_content)]
    sentence_groups = _group_sentences_by_tokens(sentences, 100_000)

    for group in sentence_groups:
        group_text = ' '.join(group)
        response = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": _format_prompt},
                {"role": "user", "content": group_text},
            ],
            stream=True
        )
        for chunk in response:
            if chunk.choices[0].delta.content is not None:
                yield chunk.choices[0].delta.content