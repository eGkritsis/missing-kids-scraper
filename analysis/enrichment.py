"""
analysis/enrichment.py
======================
Phase 2: OSINT Enrichment Pipeline

For every flagged case (from pattern analysis), queries:
  - CourtListener API (federal court cases, 18 USC 1591/1594 trafficking)
  - DOJ Press Releases RSS (arrest/conviction news)
  - Europol Newsroom RSS (operations, victim counts)
  - FBI Kidnappings page (wanted/active cases)
  - Google News targeted per-case search
  - OpenSanctions API (free tier, sanctioned traffickers)

All findings stored in enrichment_findings table.
Results feed into network graph (Phase 3) and dashboard.

Usage:
  python analysis/enrichment.py                  # enrich all flagged cases
  python analysis/enrichment.py --limit 50       # limit to 50 cases
  python analysis/enrichment.py --source interpol # only one source
  python analysis/enrichment.py --case-id 123    # single DB record
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import quote_plus

import requests
import feedparser
from rapidfuzz import fuzz

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.models import init_db, MissingPerson, Base
from sqlalchemy import Column, Integer, String, Text, DateTime, Float, Index, create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from utils.helpers import safe_json, setup_logger

logger = setup_logger("enrichment")

DB_PATH = "missing_children.db"

# ---------------------------------------------------------------------------
# Extended DB model for findings
# ---------------------------------------------------------------------------

def ensure_enrichment_table(engine):
    """Create enrichment_findings table if it doesn't exist."""
    from sqlalchemy import text
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS enrichment_findings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                missing_person_id INTEGER,
                source_type TEXT,
                source_name TEXT,
                title TEXT,
                url TEXT,
                snippet TEXT,
                relevance_score REAL,
                finding_type TEXT,
                raw_data TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (missing_person_id) REFERENCES missing_persons(id)
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_finding_person
            ON enrichment_findings(missing_person_id)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_finding_type
            ON enrichment_findings(finding_type)
        """))
        conn.commit()


def save_finding(db, person_id, source_type, source_name, title,
                 url, snippet, relevance, finding_type, raw=None):
    from sqlalchemy import text
    try:
        db.execute(text("""
            INSERT OR IGNORE INTO enrichment_findings
            (missing_person_id, source_type, source_name, title, url,
             snippet, relevance_score, finding_type, raw_data)
            VALUES (:pid, :stype, :sname, :title, :url,
                    :snippet, :rel, :ftype, :raw)
        """), {
            "pid":     person_id,
            "stype":   source_type,
            "sname":   source_name,
            "title":   title[:500] if title else "",
            "url":     url[:1000] if url else "",
            "snippet": snippet[:2000] if snippet else "",
            "rel":     relevance,
            "ftype":   finding_type,
            "raw":     json.dumps(raw) if raw else None,
        })
        db.commit()
        return True
    except Exception as e:
        logger.debug("save_finding error: %s", e)
        db.rollback()
        return False


# ---------------------------------------------------------------------------
# HTTP helper with retry
# ---------------------------------------------------------------------------

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0"})


def safe_get(url, params=None, timeout=15, delay=1.0):
    time.sleep(delay)
    try:
        r = SESSION.get(url, params=params, timeout=timeout)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 30))
            logger.warning("Rate limited on %s — waiting %ds", url[:60], wait)
            time.sleep(wait)
            r = SESSION.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r
    except requests.RequestException as e:
        logger.debug("HTTP error %s: %s", url[:60], e)
        return None


# ---------------------------------------------------------------------------
# 1. CourtListener — federal court cases
# ---------------------------------------------------------------------------

TRAFFICKING_STATUTES = ["1591", "1594", "2251", "2252", "2422"]

def search_courtlistener(person, db):
    """
    Search CourtListener for federal cases mentioning this person.
    Focuses on trafficking statutes. Free API, 50 req/min.
    """
    name = person.full_name or ""
    if not name or len(name) < 4:
        return 0

    found = 0
    # Search opinions mentioning the name
    r = safe_get(
        "https://www.courtlistener.com/api/rest/v3/search/",
        params={
            "q":          f'"{name}"',
            "type":       "o",
            "stat_Precedential": "on",
            "filed_after": "2000-01-01",
        },
        delay=1.2,
    )
    if not r:
        return 0

    try:
        data = r.json()
    except Exception:
        return 0

    results = data.get("results", [])
    for result in results[:5]:
        text_excerpt = result.get("snippet", "") or result.get("text", "")
        case_name    = result.get("caseName", "")
        citation     = result.get("citation", "")
        url          = f"https://www.courtlistener.com{result.get('absolute_url','')}"

        # Score relevance
        score = fuzz.partial_ratio(name.lower(), text_excerpt.lower()) / 100.0

        # Boost if trafficking statute mentioned
        if any(s in text_excerpt for s in TRAFFICKING_STATUTES):
            score = min(score + 0.3, 1.0)

        if score < 0.4:
            continue

        finding_type = "COURT_TRAFFICKING" if any(
            s in text_excerpt for s in TRAFFICKING_STATUTES
        ) else "COURT_MENTION"

        saved = save_finding(
            db, person.id,
            "courtlistener", case_name or "CourtListener",
            f"{case_name} — {citation}",
            url, text_excerpt[:500], score,
            finding_type,
            {"case_name": case_name, "citation": citation},
        )
        if saved:
            found += 1
            logger.info("CourtListener: %s → %s (%.2f)", name, case_name[:60], score)

    return found


# ---------------------------------------------------------------------------
# 2. DOJ Press Releases
# ---------------------------------------------------------------------------

DOJ_RSS_FEEDS = [
    "https://www.justice.gov/feeds/opa/justice-news.xml",
    "https://www.justice.gov/feeds/opa/press-releases.xml",
]

DOJ_TRAFFICKING_KEYWORDS = [
    "sex trafficking", "child trafficking", "human trafficking",
    "1591", "minor", "juvenile", "child exploitation",
    "child pornography", "child sexual abuse", "CSAM",
    "missing child", "amber alert", "recovered minor",
]

_doj_cache = []
_doj_loaded = False

def _load_doj_feed():
    global _doj_cache, _doj_loaded
    if _doj_loaded:
        return
    for url in DOJ_RSS_FEEDS:
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries:
                _doj_cache.append({
                    "title":   entry.get("title", ""),
                    "summary": entry.get("summary", "") or entry.get("description", ""),
                    "url":     entry.get("link", ""),
                    "date":    entry.get("published", ""),
                })
        except Exception as e:
            logger.debug("DOJ feed error: %s", e)
    _doj_loaded = True
    logger.info("DOJ feed loaded: %d articles", len(_doj_cache))


def search_doj(person, db):
    _load_doj_feed()
    name  = (person.full_name or "").lower()
    parts = name.split()
    if len(parts) < 2:
        return 0

    found = 0
    for article in _doj_cache:
        text = f"{article['title']} {article['summary']}".lower()

        # Name match
        name_score = fuzz.partial_ratio(name, text) / 100.0
        if name_score < 0.5:
            # Try last name only
            last = parts[-1]
            if len(last) > 3 and last in text:
                name_score = 0.5
            else:
                continue

        # Keyword relevance
        kw_hits = sum(1 for k in DOJ_TRAFFICKING_KEYWORDS if k in text)
        if kw_hits == 0:
            continue

        score = min(name_score + kw_hits * 0.05, 1.0)

        finding_type = "DOJ_TRAFFICKING" if any(
            k in text for k in ["sex trafficking", "1591", "child trafficking"]
        ) else "DOJ_MENTION"

        saved = save_finding(
            db, person.id,
            "doj", "DOJ Press Release",
            article["title"],
            article["url"],
            article["summary"][:500],
            score, finding_type,
            {"date": article["date"]},
        )
        if saved:
            found += 1
            logger.info("DOJ: %s → %s (%.2f)", person.full_name, article["title"][:60], score)

    return found


# ---------------------------------------------------------------------------
# 3. Europol Newsroom
# ---------------------------------------------------------------------------

EUROPOL_FEED = "https://www.europol.europa.eu/newsroom/rss.xml"

_europol_cache = []
_europol_loaded = False

def _load_europol():
    global _europol_cache, _europol_loaded
    if _europol_loaded:
        return
    try:
        feed = feedparser.parse(EUROPOL_FEED)
        for entry in feed.entries:
            _europol_cache.append({
                "title":   entry.get("title", ""),
                "summary": entry.get("summary", "") or "",
                "url":     entry.get("link", ""),
            })
        _europol_loaded = True
        logger.info("Europol feed loaded: %d articles", len(_europol_cache))
    except Exception as e:
        logger.debug("Europol feed error: %s", e)
        _europol_loaded = True


EUROPOL_KEYWORDS = [
    "trafficking", "missing", "child", "minor", "exploitation",
    "sexual", "abduction", "kidnapping", "smuggling",
]


def search_europol(person, db):
    _load_europol()
    name     = (person.full_name or "").lower()
    country  = (person.country_last_seen or "").lower()
    nat      = (person.nationality or "").lower()

    found = 0
    for article in _europol_cache:
        text = f"{article['title']} {article['summary']}".lower()

        # For Europol we match on country/nationality + keywords
        # (individual names rarely appear in Europol press releases)
        geo_match = (country and country in text) or (nat and nat in text)
        kw_hits   = sum(1 for k in EUROPOL_KEYWORDS if k in text)

        if not geo_match or kw_hits < 2:
            continue

        score = min(0.4 + kw_hits * 0.08, 1.0)

        save_finding(
            db, person.id,
            "europol", "Europol Newsroom",
            article["title"],
            article["url"],
            article["summary"][:500],
            score, "EUROPOL_OPERATION",
        )
        found += 1

    return found


# ---------------------------------------------------------------------------
# 4. FBI Kidnappings / Wanted
# ---------------------------------------------------------------------------

FBI_WANTED_URL = "https://api.fbi.gov/wanted/v1/list"

_fbi_cache = []
_fbi_loaded = False


def _load_fbi():
    global _fbi_cache, _fbi_loaded
    if _fbi_loaded:
        return
    try:
        # FBI has a public JSON API
        r = safe_get(
            FBI_WANTED_URL,
            params={"page": 1, "pageSize": 50, "status": "na",
                    "program": "kidnappings-missing-persons"},
            delay=1.0,
        )
        if r:
            data = r.json()
            _fbi_cache = data.get("items", [])
            logger.info("FBI wanted loaded: %d items", len(_fbi_cache))
    except Exception as e:
        logger.debug("FBI load error: %s", e)
    _fbi_loaded = True


def search_fbi(person, db):
    _load_fbi()
    name = (person.full_name or "").lower()
    if not name:
        return 0

    found = 0
    for item in _fbi_cache:
        subjects = " ".join([
            item.get("title", ""),
            item.get("description", ""),
            " ".join(item.get("aliases", [])),
        ]).lower()

        score = fuzz.token_sort_ratio(name, subjects) / 100.0
        if score < 0.55:
            continue

        images = item.get("images", [])
        img_url = images[0].get("original", "") if images else ""

        save_finding(
            db, person.id,
            "fbi", "FBI Wanted",
            item.get("title", ""),
            item.get("url", ""),
            item.get("description", "")[:500],
            score, "FBI_WANTED",
            {"image": img_url, "uid": item.get("uid", "")},
        )
        found += 1
        logger.info("FBI: %s → %s (%.2f)", person.full_name, item.get("title","")[:50], score)

    return found


# ---------------------------------------------------------------------------
# 5. Google News targeted per-case search
# ---------------------------------------------------------------------------

def search_google_news(person, db):
    """
    Targeted Google News RSS search for each person's full name.
    Queries: "FIRSTNAME LASTNAME" missing OR found OR arrested OR rescued
    """
    name = person.full_name or ""
    if not name or len(name) < 4:
        return 0

    query = f'"{name}" missing OR found OR arrested OR rescued OR trafficking'
    url   = (
        f"https://news.google.com/rss/search?"
        f"q={quote_plus(query)}&hl=en&gl=US&ceid=US:en"
    )

    try:
        feed = feedparser.parse(url)
    except Exception:
        return 0

    found = 0
    for entry in feed.entries[:5]:
        title   = entry.get("title", "")
        summary = entry.get("summary", "") or ""
        text    = f"{title} {summary}".lower()
        score   = fuzz.partial_ratio(name.lower(), text) / 100.0

        if score < 0.45:
            continue

        is_resolution = any(k in text for k in [
            "found safe", "found alive", "was found", "been found",
            "rescued", "reunited", "returned home", "amber alert cancel",
        ])
        finding_type = "NEWS_RESOLUTION" if is_resolution else "NEWS_MENTION"

        # Auto-resolve in DB if resolution news found
        if is_resolution and not person.is_resolved:
            person.is_resolved      = True
            person.resolution_notes = f"News resolution: {title[:200]} | {entry.get('link','')}"
            db.commit()
            logger.warning("AUTO-RESOLVED: %s — %s", name, title[:60])

        saved = save_finding(
            db, person.id,
            "google_news", "Google News",
            title, entry.get("link", ""),
            summary[:500], score, finding_type,
        )
        if saved:
            found += 1

    time.sleep(0.5)
    return found


# ---------------------------------------------------------------------------
# 6. OpenSanctions (free tier — no auth needed for basic search)
# ---------------------------------------------------------------------------

def search_opensanctions(person, db):
    """
    Search OpenSanctions for traffickers connected to this person's country.
    Uses the free /search endpoint.
    """
    country = person.country_last_seen or person.nationality or ""
    name    = person.full_name or ""
    if not country:
        return 0

    # Search for traffickers from this country
    r = safe_get(
        "https://api.opensanctions.org/search/default",
        params={
            "q":       f"trafficking {country}",
            "schema":  "Person",
            "limit":   10,
        },
        delay=1.5,
    )
    if not r:
        return 0

    try:
        data = r.json()
    except Exception:
        return 0

    found   = 0
    results = data.get("results", [])
    for result in results:
        caption    = result.get("caption", "")
        properties = result.get("properties", {})
        topics     = properties.get("topics", [])
        countries  = properties.get("country", [])

        # Only trafficking/criminal related
        if not any(t in str(topics).lower() for t in
                   ["sanction", "crime", "trafficking", "wanted"]):
            continue

        score = 0.5
        if country.lower() in [c.lower() for c in countries]:
            score = 0.7

        dataset = result.get("datasets", [""])[0]
        url     = f"https://www.opensanctions.org/entities/{result.get('id','')}"

        save_finding(
            db, person.id,
            "opensanctions", f"OpenSanctions/{dataset}",
            caption, url,
            f"Topics: {topics} | Countries: {countries}",
            score, "SANCTIONS_NETWORK",
            {"id": result.get("id"), "topics": topics},
        )
        found += 1

    return found


# ---------------------------------------------------------------------------
# MAIN ENRICHMENT RUNNER
# ---------------------------------------------------------------------------

ENRICHERS = {
    "courtlistener": search_courtlistener,
    "doj":           search_doj,
    "europol":       search_europol,
    "fbi":           search_fbi,
    "google_news":   search_google_news,
    "opensanctions": search_opensanctions,
}


def run_enrichment(db_path=DB_PATH, limit=None, source_filter=None,
                   case_id=None, min_priority=0):
    engine, Session = init_db(db_path)
    ensure_enrichment_table(engine)
    db = Session()

    # Build query
    query = db.query(MissingPerson).filter(MissingPerson.is_resolved == False)
    if case_id:
        query = query.filter(MissingPerson.id == case_id)
    if limit:
        query = query.limit(limit)

    cases = query.all()
    logger.info("Enriching %d cases with %d sources...",
                len(cases), len(ENRICHERS))

    total_findings = 0
    enrichers = {k: v for k, v in ENRICHERS.items()
                 if not source_filter or k == source_filter}

    for i, person in enumerate(cases):
        name = person.full_name or f"ID:{person.id}"
        logger.info("[%d/%d] %s", i+1, len(cases), name)

        case_findings = 0
        for source_name, enricher_fn in enrichers.items():
            try:
                n = enricher_fn(person, db)
                case_findings += n
            except Exception as e:
                logger.error("Enricher %s failed for %s: %s",
                             source_name, name, e)

        total_findings += case_findings
        if case_findings:
            logger.info("  → %d findings", case_findings)

    db.close()
    logger.info("Enrichment complete. Total findings: %d", total_findings)
    return total_findings


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OSINT Enrichment Pipeline")
    parser.add_argument("--db",       default=DB_PATH)
    parser.add_argument("--limit",    type=int, default=None,
                        help="Max cases to enrich (default: all)")
    parser.add_argument("--source",   default=None,
                        choices=list(ENRICHERS.keys()),
                        help="Run only one enricher")
    parser.add_argument("--case-id",  type=int, default=None,
                        help="Enrich a single case by DB id")
    args = parser.parse_args()

    n = run_enrichment(args.db, args.limit, args.source, args.case_id)
    print(f"\nTotal new findings: {n}")
