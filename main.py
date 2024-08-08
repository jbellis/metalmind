from uuid import UUID

from fasthtml.common import *
from starlette.responses import JSONResponse

import logic
from config import db
from util import humanize_url

import logging
from starlette.requests import Request
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


app = FastHTML(hdrs=[picolink])


@app.get("/")
def index():
    return Titled("MetalMind", H2("Welcome to MetalMind"))


@app.get("/search")
def search(session, user_id: str | None = None, saved_before: str | None = None):
    if user_id:
        session['user_id'] = user_id
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
            H3(A(url.title, href=f"/snapshot/{url.url_id}")),
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

    result_cards = []
    for result in search_results:
        chunks_list = Ul(*[Li(f"{chunk[0]}") for chunk in result.chunks],
                         cls="list-group list-group-flush")
        card = Article(H3(A(result.title, href=f"/snapshot{result.url_id}")),
                       P(f"Saved at: {result.saved_at_human}",
                         A("View original", href=result.full_url)),
                       chunks_list,
                       cls="card")
        result_cards.append(card)

    return Titled("Search Results",
                  Main(*result_cards,
                       A("Back to Search", href=f"/search", role="button"),
                       cls="container"))

@app.post("/save_if_new")
async def save_if_new(request):
    body = await request.body()
    logger.debug(f"Received request body: {body.decode()}")
    logger.debug(f"Request headers: {request.headers}")

    if not all([url, title, text_content, user_id]):
        return JSONResponse({"error": "Missing required fields"}, status_code=400)

    result = logic.save_if_new(db, url, title, text_content, user_id)
    return {"saved": result}


@app.get("/snapshot/{url_id}")
def snapshot(session, url_id: str):
    user_id = session['user_id']
    title, text_content, formatted_content = logic.load_snapshot(db, user_id, url_id)
    saved_at = logic._uuid1_to_datetime(UUID(url_id))

    return Titled(f"Snapshot: {title}",
      Main(
          P(f"Saved at: {saved_at}"),
          Article(text_content)
      )
  )


serve()
