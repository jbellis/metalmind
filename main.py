import gzip
from uuid import UUID

from fasthtml.common import *
from starlette.responses import StreamingResponse

import logic
from config import db
from util import humanize_url, humanize_datetime


app = FastHTML(hdrs=[picolink])


@app.get("/")
def index():
    return Title("Total Recall"), Main(
        Img(src="/static/frontpage.png", alt="Front Page"),
        H1("Welcome to MetalMind, your personal web archive"),
        P("""
          MetalMind (formerlly Total Recall) provides a browser extension that automatically
          saves the articles you read on the web.  You can search the full text of your entire history
          using semantic search powered by """,
          A("DataStax Astra", href="https://astra.datastax.com/"),
          "."
        ),
        P("Get the extension for ",
          A("Firefox", href="https://addons.mozilla.org/en-US/firefox/addon/total-recall-web-companion/?utm_source=addons.mozilla.org"),
          "."
        ),
        cls="container"
    )


@app.get("/search")
def search(session, user_id: UUID | None = None, saved_before: str | None = None):
    if user_id:
        session['user_id'] = str(user_id)
    else:
        try:
            user_id = session['user_id']
        except KeyError:
            return Titled("Search", H2("Missing user ID"))

    urls, oldest_saved_at = logic.recent_urls(db, user_id, saved_before)

    search_form = Search(
        Input(type="text", name="search_text", placeholder="Enter search text"),
        Button("Search", type="submit"),
        action="/results", method="post"
    )

    url_cards = [
        Article(
            H4(A(url.title, href=f"/snapshot/{url.url_id}")),
            Small(
                humanize_url(url.full_url),
                Br(),
                f"Saved: {url.saved_at_human}",
                " • ",
                A("View original", href=url.full_url),
            ),
        ) for url in urls
    ]

    older_urls_btn = A("Older URLs", href=f"/search?saved_before={oldest_saved_at}",
                       role="button", cls="outline") if urls and oldest_saved_at else None
    reset_btn = A("Reset to newest", href=f"/search", role="button", cls="outline") if saved_before else None

    return Titled("Search",
        Main(
            search_form,
            H2("Recent URLs"),
            *url_cards,
            Div(older_urls_btn, reset_btn, cls="grid")
        )
    )


@app.post("/results")
def results(session, search_text: str):
    user_id = session['user_id']
    search_results = logic.search(db, user_id, search_text)

    search_form = Search(
        Input(type="text", name="search_text", placeholder="Enter search text", value=search_text),
        Button("Search", type="submit"),
        action="/results", method="post"
    )

    result_cards = []
    for result in search_results:
        card = Article(H4(A(result.title, href=f"/snapshot/url_id={result.url_id}")),
                       Small(
                           P(f"Saved at: {result.saved_at_human}",
                             A("View original", href=result.full_url)),
                           Ul(*[Li(f"{chunk[0]}") for chunk in result.chunks],
                                            cls="list-group list-group-flush"),
                           cls="card"
                       ))
        result_cards.append(card)

    return Titled("Search Results",
                  Main(search_form,
                       *result_cards,
                       A("Back to Search", href=f"/search", role="button"),
                       cls="container"))


@app.post("/save_if_new")
async def save_if_new(request: Request):
    # check content-type
    if request.headers.get("content-type") != "application/json":
        raise HTTPException(status_code=415, detail="Content-Type must be application/json")
    # Manually deserialize the JSON request body
    data = await request.json()
    url = data.get("url")
    title = data.get("title")
    text_content = data.get("text_content")
    user_id = UUID(data.get("user_id"))

    # Validate the required fields
    if not all([url, title, text_content, user_id]):
        raise HTTPException(status_code=400, detail="Missing required fields")

    # Call the logic function with the extracted data
    # FIXME remove backwards-compatibility logic here
    result = logic.save_if_new(db, url, title, text_content, user_id)
    result['saved'] = result['result'] == 'saved'
    return result


@app.post("/save_html")
async def save_html(request: Request):
    # check content-type
    if request.headers.get("content-type") != "application/octet-stream" or \
            request.headers.get("content-encoding") != "gzip":
        raise HTTPException(status_code=415, detail="Content-Type must be application/octet-stream")

    # Manually deserialize the request body
    user_id = UUID(request.headers.get('X-USER-ID'))
    url_id = UUID(request.headers.get('X-URL-ID'))
    content_gz = await request.body()

    # Validate the required fields
    if not all([user_id, url_id, content_gz]):
        raise HTTPException(status_code=400, detail="Missing required fields")

    # Call the logic function with the extracted data
    return db.save_formatting(user_id, url_id, content_gz)


@app.get("/snapshot/{url_id}")
def snapshot(session, url_id: UUID):
    user_id = UUID(session['user_id'])
    url, title, text_content, content_gz = db.load_snapshot(user_id, url_id)
    saved_at = logic._uuid1_to_datetime(url_id)

    content_div = Iframe(src=f"/snapshot_iframe/{url_id}", width="100%", height="600px", style="border: 1px solid #ccc;")

    return Titled("Snapshot of " + title,
                  Container(
                      P(f"Snapshot of ", A(title, id="title", href=url)),
                      P(f"Taken {humanize_datetime(saved_at)}"),
                      content_div
                  ))

@app.get("/snapshot_iframe/{url_id}")
def snapshot_iframe(session, url_id: UUID):
    user_id = UUID(session['user_id'])
    url, title, text_content, content_gz = db.load_snapshot(user_id, url_id)
    formatted_content = gzip.decompress(content_gz).decode('utf-8') if content_gz else None

    if formatted_content:
        # Wrap the content in a full HTML structure
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>{title}</title>
            <base href="{url}">
        </head>
        <body>
            {formatted_content}
        </body>
        </html>
        """
    else:
        # this is not consistently displayed in an iframe but i guess that's okay since the
        # "rehydrated" html is generated against pico css
        content_div = Div(
            "Please wait, loading...",
            id="formatted_content",
            hx_get=f"/snapshot/stream/{url_id}/",
            hx_trigger="load",
            hx_swap="innerHTML"
        )
        saved_at = logic._uuid1_to_datetime(url_id)
        return Titled("Snapshot of " + title,
                      Container(
                          P(f"Snapshot of ", A(title, id="title", href=url)),
                          P(f"Taken {humanize_datetime(saved_at)}"),
                          content_div
                      ))


@app.get('/snapshot/stream/{url_id}/')
async def snapshot_stream(session, url_id: UUID):
    user_id = UUID(session['user_id'])
    async def generate():
        for chunk in logic.stream_formatted_snapshot(db, user_id, url_id):
            yield chunk

    return StreamingResponse(generate(), media_type='text/html')


if __name__ == "__main__":
    serve()
