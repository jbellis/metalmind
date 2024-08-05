from fasthtml.common import *
import json
import os
from uuid import UUID
import logic
from config import db

app = FastHTML()


@app.get("/")
def index():
    return Titled("Search App", H1("Welcome to the Search App"))


@app.get("/search")
def search(user_id: str | None = None, saved_before: str | None = None):
    if not user_id:
        return Div("Error: user_id not provided", cls="alert alert-danger")

    urls, oldest_saved_at = logic.recent_urls(db, user_id, saved_before)

    search_form = Form(
        Input(type="text", name="search_text", placeholder="Enter search text", cls="form-control"),
        Input(type="hidden", name="user_id_str", value=user_id),
        Button("Search", type="submit", cls="btn btn-primary"),
        action="/results", method="post"
    )

    url_cards = [
        Card(
            H5(A(url.title, href=url.full_url), cls="card-title"),
            P(f"Saved at: {url.saved_at_human}", cls="card-text"),
            P(A("View snapshot", href=f"/snapshot/{user_id}/{url.url_id}"), cls="card-text"),
            cls="mb-3"
        ) for url in urls
    ]

    older_urls_btn = A("Older URLs", href=f"/search?user_id={user_id}&saved_before={oldest_saved_at}",
                       cls="btn btn-primary") if urls and oldest_saved_at else None
    reset_btn = A("Reset to newest", href=f"/search?user_id={user_id}", cls="btn btn-primary") if saved_before else None

    return Titled("Search",
      Container(
          H1("Search", cls="mt-5"),
          search_form,
          H1("Recent URLs", cls="mt-5"),
          *url_cards,
          Div(older_urls_btn, reset_btn, cls="mt-3")
      )
  )


@app.post("/results")
def results(user_id_str: str, search_text: str):
    search_results = logic.search(db, user_id_str, search_text)
    result_items = [Li(f"{result.title} - {result.url}") for result in search_results]

    return Titled("Search Results",
      Container(
          H1("Search Results"),
          Ul(*result_items),
          A("Back to Search", href=f"/search?user_id={user_id_str}", cls="btn btn-primary")
      )
  )


@app.post("/save_if_new")
def save_if_new(url: str, title: str, text_content: str, user_id: str):
    if not all([url, title, text_content, user_id]):
        return JSON({"error": "Missing required fields"}, status_code=400)

    result = logic.save_if_new(db, url, title, text_content, user_id)
    return JSON({"saved": result})


@app.get("/snapshot/{user_id}/{url_id}")
def snapshot(user_id: str, url_id: str):
    title, formatted_content = logic.load_snapshot(db, user_id, url_id)
    saved_at = logic._uuid1_to_datetime(UUID(url_id))

    return Titled(f"Snapshot: {title}",
      Container(
          H1(title),
          P(f"Saved at: {saved_at}"),
          Div(formatted_content, cls="formatted-content")
      )
  )


@app.get("/snapshot/stream/{user_id}/{url_id}")
async def snapshot_stream(user_id: str, url_id: str):
    async def generate():
        async for formatted_content in logic.stream_snapshot(db, user_id, url_id):
            yield f'data: {json.dumps({"formatted_content": formatted_content})}\n\n'
        yield 'event: EOF\ndata: {}\n\n'

    return StreamingResponse(generate(), media_type='text/event-stream')


serve(port=80)