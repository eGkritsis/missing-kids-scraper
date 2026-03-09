"""
scrapers/missing_people_uk.py

Async scraper for Missing People UK (missingpeople.org.uk/appeal-search)
Uses httpx async client with 10 concurrent detail fetches.

Flow:
  1. POST XHR endpoint with age=child + paged=N  -> 20 cards per page
  2. Concurrently fetch each detail page          -> age, city, date, photo
  3. Skip anyone age > 17
  4. Upsert into MissingPerson table
"""

import asyncio
import re
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from database.models import MissingPerson, init_db
from scrapers.base import BaseScraper
from utils.helpers import clean_text, safe_json

BASE           = "https://www.missingpeople.org.uk"
SEARCH_URL     = f"{BASE}/appeal-search"
HEADERS        = {
    "User-Agent":       "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/120 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
    "Referer":          SEARCH_URL,
    "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
}
MAX_CONCURRENT = 10
CHILD_MAX_AGE  = 17


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _extract(pattern, text):
    m = re.search(pattern, text, re.IGNORECASE)
    return m.group(1).strip() if m else None


def _parse_detail(html):
    soup   = BeautifulSoup(html, "lxml")
    text   = soup.get_text(" ", strip=True)

    age_str       = _extract(r"Age at disappearance\s*([0-9]+)", text)
    missing_since = _extract(r"Missing since\s*([0-9/]+)", text)
    location      = _extract(r"Missing from\s*([A-Za-z ,\-]+?)\s*Missing since", text)
    reference     = _extract(r"Reference No\s*([A-Za-z0-9\-]+)", text)

    og    = soup.find("meta", property="og:image")
    image = og["content"] if og and og.get("content") else None

    city = county = None
    if location and "," in location:
        parts  = [x.strip() for x in location.split(",", 1)]
        city, county = parts[0], parts[1]
    elif location:
        city = location.strip()

    gender  = None
    snippet = text[:600].lower()
    if any(w in snippet for w in ("girl", " she ", " her ")):
        gender = "Female"
    elif any(w in snippet for w in ("boy", " he ", " him ", " his ")):
        gender = "Male"

    return {
        "age":           int(age_str) if age_str else None,
        "missing_since": missing_since,
        "city":          city,
        "county":        county,
        "reference":     reference,
        "image":         image,
        "gender":        gender,
    }


def _parse_date(ds):
    if not ds:
        return None
    for fmt in ("%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(ds, fmt).date()
        except ValueError:
            pass
    return None


# ---------------------------------------------------------------------------
# Async fetch
# ---------------------------------------------------------------------------

async def _fetch_list_page(client, page):
    payload = {
        "action":       "mp_filter_appeals_xhr",
        "search_term":  "",
        "region":       "",
        "date_missing": "",
        "age":          "child",
        "gender":       "",
        "paged":        str(page),
    }
    r    = await client.post(SEARCH_URL, data=payload, timeout=30)
    soup = BeautifulSoup(r.text, "lxml")

    results = []
    for card in soup.select(".card--person"):
        name_el   = card.select_one(".card__title")
        link_el   = card.select_one(".card__link")
        region_el = card.select_one(".post-meta__item")
        img_el    = card.select_one("img")

        if not link_el or not link_el.get("href"):
            continue

        url  = urljoin(BASE, link_el["href"])
        slug = url.rstrip("/").split("/")[-1]
        ref  = re.search(r"(\d{2,4}-\d{3,9})$", slug)

        results.append({
            "name":        clean_text(name_el.get_text()) if name_el else None,
            "url":         url,
            "region":      clean_text(region_el.get_text()) if region_el else None,
            "image_thumb": img_el["src"] if img_el and img_el.get("src") else None,
            "case_id":     ref.group(1) if ref else slug,
        })

    return results


async def _fetch_detail(client, semaphore, record):
    async with semaphore:
        try:
            r       = await client.get(record["url"], timeout=30)
            details = _parse_detail(r.text)
            record.update(details)
            # Drop adults that slipped past the age=child XHR filter
            if record.get("age") is not None and record["age"] > CHILD_MAX_AGE:
                return None
            return record
        except Exception:
            return None


async def _scrape_all():
    semaphore    = asyncio.Semaphore(MAX_CONCURRENT)
    all_children = []

    async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True) as client:
        page = 1
        while True:
            cards = await _fetch_list_page(client, page)
            if not cards:
                break

            results = await asyncio.gather(
                *[_fetch_detail(client, semaphore, card) for card in cards]
            )
            all_children.extend(r for r in results if r is not None)
            page += 1
            if page > 60:
                break

    return all_children


# ---------------------------------------------------------------------------
# BaseScraper integration
# ---------------------------------------------------------------------------

class MissingPeopleUKScraper(BaseScraper):
    name = "missing_people_uk"

    def run(self) -> dict:
        self.logger.info(
            "Starting Missing People UK async scrape (%d concurrent detail fetches)...",
            MAX_CONCURRENT,
        )

        children = asyncio.run(_scrape_all())
        self.logger.info("Fetched %d children from site, saving...", len(children))

        new = updated = skipped = errors = 0
        _, Session = init_db()
        db = Session()

        try:
            for child in children:
                try:
                    name    = child.get("name") or ""
                    parts   = name.split()
                    first   = parts[0] if parts else ""
                    last    = parts[-1] if len(parts) > 1 else ""
                    case_id = child.get("case_id") or child.get("reference") or ""

                    if not case_id or not name:
                        skipped += 1
                        continue

                    photo = child.get("image") or child.get("image_thumb")
                    if photo and not photo.startswith("http"):
                        photo = BASE + photo

                    update_data = {
                        "source_url":           child.get("url"),
                        "full_name":            name,
                        "first_name":           first,
                        "last_name":            last,
                        "age_at_disappearance": child.get("age"),
                        "gender":               child.get("gender"),
                        "date_missing":         _parse_date(child.get("missing_since")),
                        "city_last_seen":       child.get("city"),
                        "state_last_seen":      child.get("county") or child.get("region"),
                        "country_last_seen":    "United Kingdom",
                        "photo_url":            photo,
                        "raw_data":             safe_json(child),
                    }

                    inst = db.query(MissingPerson).filter_by(
                        source=self.name, source_id=case_id
                    ).first()

                    if inst is None:
                        db.add(MissingPerson(
                            source=self.name, source_id=case_id, **update_data
                        ))
                        new += 1
                        self.logger.info("NEW: %s (age=%s, %s)",
                                         name, child.get("age"), child.get("city"))
                    else:
                        for k, v in update_data.items():
                            setattr(inst, k, v)
                        updated += 1

                    db.commit()

                except Exception as exc:
                    db.rollback()
                    self.logger.error("DB [%s]: %s", child.get("name", "?"), exc)
                    errors += 1
        finally:
            db.close()

        self.logger.info(
            "Missing People UK done. found=%d new=%d updated=%d skipped=%d errors=%d",
            len(children), new, updated, skipped, errors,
        )
        return {
            "found":   len(children),
            "new":     new,
            "updated": updated,
            "skipped": skipped,
            "errors":  errors,
        }
