from fasthtml.common import *
from uuid import UUID
import logic
from config import db
from util import humanize_url

app = FastHTML(hdrs=[picolink])


@app.get("/")
def index():
    return Titled("MetalMind", H2("Welcome to MetalMind"))


@app.get("/search")
def search(user_id: str | None = None, saved_before: str | None = None):
    if not user_id:
        return Div("Error: user_id not provided", cls="alert alert-danger")

    urls, oldest_saved_at = logic.recent_urls(db, user_id, saved_before)

    search_form = Form(
        Group(Input(type="text", name="search_text", placeholder="Enter search text"),
              Input(type="hidden", name="user_id_str", value=user_id),
              Button("Search", type="submit")),
        action="/results", method="post"
    )

    url_cards = [
        Article(
            H3(A(url.title, href=f"/snapshot/{user_id}/{url.url_id}")),
            Small(
                humanize_url(url.full_url),
                Br(),
                f"Saved: {url.saved_at_human}",
                " • ",
                A("View original", href=url.full_url),
            ),
        ) for url in urls
    ]

    older_urls_btn = A("Older URLs", href=f"/search?user_id={user_id}&saved_before={oldest_saved_at}",
                       role="button", cls="outline") if urls and oldest_saved_at else None
    reset_btn = A("Reset to newest", href=f"/search?user_id={user_id}", role="button", cls="outline") if saved_before else None

    return Titled("Search",
      Main(
          search_form,
          H2("Recent URLs"),
          *url_cards,
          Div(older_urls_btn, reset_btn, cls="grid")
      )
  )


@app.post("/results")
def results(user_id_str: str, search_text: str):
    search_results = logic.search(db, user_id_str, search_text)

    result_cards = []
    for result in search_results:
        chunks_list = Ul(*[Li(f"{chunk[0]}") for chunk in result.chunks],
                         cls="list-group list-group-flush")
        card = Article(H3(A(result.title, href=f"/snapshot/{user_id_str}/{result.url_id}")),
                       P(f"Saved at: {result.saved_at_human}",
                         A("View original", href=result.full_url)),
                       chunks_list,
                       cls="card")
        result_cards.append(card)

    return Titled("Search Results",
                  Main(*result_cards,
                       A("Back to Search", href=f"/search?user_id={user_id_str}", role="button"),
                       cls="container"))

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
      Main(
          P(f"Saved at: {saved_at}"),
          Article(formatted_content)
      )
  )


serve()
