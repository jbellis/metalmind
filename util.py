from urllib.parse import urlparse
from datetime import datetime

from dateutil.relativedelta import relativedelta


def humanize_datetime(dt: datetime) -> str:
    now = datetime.now()
    delta = relativedelta(now, dt)

    if delta.days == 0:
        return "Today"
    elif delta.days == 1:
        return "Yesterday"
    elif delta.days < 7:
        return "This Week"
    elif delta.days < 14:
        return "Last Week"
    elif delta.days < 21:
        return "Two Weeks Ago"
    elif delta.days < 28:
        return "Three Weeks Ago"
    elif delta.months == 0:
        return "This Month"
    elif delta.months == 1:
        return "Last Month"
    else:
        return f"{delta.months} Months Ago"


def humanize_url(url: str) -> str:
    """
    https://cloud.oracle.com/networking?region=us-phoenix-1
    -> cloud.oracle.com/networking
    """
    parsed = urlparse(url)
    raw = parsed.hostname + parsed.path
    return raw if len(raw) < 80 else raw[:77] + '...'
