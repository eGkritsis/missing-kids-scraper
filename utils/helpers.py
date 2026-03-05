"""
utils/helpers.py
Shared utilities: rate-limited HTTP client, logging setup, data cleaning.
"""

import time
import logging
import json
import re
from datetime import datetime, date
from typing import Optional
from functools import wraps

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from rich.logging import RichHandler

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True)],
    )
    return logging.getLogger(name)


logger = setup_logger(__name__)


# ---------------------------------------------------------------------------
# HTTP Session with retries & polite rate-limiting
# ---------------------------------------------------------------------------

HEADERS = {
    "User-Agent": (
        "MissingChildrenTracker/1.0 (nonprofit volunteer project; "
        "contact: your@email.com)"
    ),
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "en-US,en;q=0.9",
}


def build_session(retries: int = 3, backoff: float = 1.5) -> requests.Session:
    """Return a requests Session with retry logic and volunteer user-agent."""
    session = requests.Session()
    session.headers.update(HEADERS)

    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def polite_get(session: requests.Session, url: str, delay: float = 1.5, **kwargs):
    """GET with a mandatory pause to avoid hammering servers."""
    time.sleep(delay)
    resp = session.get(url, timeout=30, **kwargs)
    resp.raise_for_status()
    return resp


def polite_post(session: requests.Session, url: str, delay: float = 1.5, **kwargs):
    """POST with a mandatory pause."""
    time.sleep(delay)
    resp = session.post(url, timeout=30, **kwargs)
    resp.raise_for_status()
    return resp


# ---------------------------------------------------------------------------
# Data cleaning helpers
# ---------------------------------------------------------------------------

def clean_text(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    return re.sub(r"\s+", " ", text).strip()


def parse_date(value: Optional[str]) -> Optional[date]:
    """Try several common date formats and return a date object or None."""
    if not value:
        return None
    value = value.strip()
    formats = [
        "%m/%d/%Y", "%Y-%m-%d", "%d-%b-%Y",
        "%B %d, %Y", "%b %d, %Y", "%m-%d-%Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    logger.debug("Could not parse date: %s", value)
    return None


def height_to_cm(feet: Optional[int], inches: Optional[int]) -> Optional[float]:
    if feet is None:
        return None
    total_inches = (feet * 12) + (inches or 0)
    return round(total_inches * 2.54, 1)


def lbs_to_kg(lbs: Optional[float]) -> Optional[float]:
    if lbs is None:
        return None
    return round(lbs * 0.453592, 1)


def extract_names_from_text(text: str) -> list[str]:
    """
    Very simple heuristic: look for sequences of capitalized words that could
    be person names. This is best-effort and will have false positives.
    """
    pattern = r"\b([A-Z][a-z]{1,20})\s+([A-Z][a-z]{1,20})\b"
    matches = re.findall(pattern, text)
    return [f"{f} {l}" for f, l in matches]


def safe_json(obj) -> str:
    """Serialize an object to JSON, falling back gracefully."""
    try:
        return json.dumps(obj, default=str)
    except Exception:
        return "{}"
