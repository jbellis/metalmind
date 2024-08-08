import google.generativeai as gemini
import os

# Configure the Gemini API
gemini.configure(api_key=os.environ['GEMINI_API_KEY'])


_summarize_prompt = (
    "You are a helpful assistant who will give the subject of the provided web page content in a single sentence. "
    "Give the subject in a form appropriate for an article or book title with no extra preamble or context."
    "Examples of good responses: "
    "`The significance of German immigrants in early Texas history`, "
    "`The successes and shortcomings of persistent collections in server-side Java development`, "
    "`A personal account of the benefits of intermittent fasting`.")
def summarize(text: str) -> str:
    # setup
    model = gemini.GenerativeModel("gemini-1.5-flash")
    truncated = text[:16000]  # Truncate to 16000 characters
    # summarize
    response = model.generate_content([_summarize_prompt, truncated])
    return response.text.strip()


