"""
scrapers/missing_people_uk.py

Scraper for Missing People UK (missingpeople.org.uk)
Uses the XHR filter endpoint with age=child filter and paged= pagination.

Endpoints:
  List:   POST https://www.missingpeople.org.uk/appeal-search
          action=mp_filter_appeals_xhr&age=child&paged=N
  Detail: GET  https://www.missingpeople.org.uk/help-us-find/{slug}

Detail page fields available:
  - Age at disappearance
  - Missing from (city/region)
  - Missing since (date)
  - Reference No
  - Photo
"""

import re
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from database.models import MissingPerson
from scrapers.base import BaseScraper
from utils.helpers import clean_text, safe_json

BASE_URL   = "https://www.missingpeople.org.uk"
LIST_URL   = "https://www.missingpeople.org.uk/appeal-search"
HEADERS    = {
    "User-Agent":      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Referer":         "https://www.missingpeople.org.uk/appeal-search",
}
XHR_HEADERS = {
    **HEADERS,
    "Content-Type":    "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
}


def _parse_date(text: str):
    """Parse DD/MM/YYYY date strings from the detail page."""
    if not text:
        return None
    text = text.strip()
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass
    return None


def _parse_age(text: str):
    """Extract integer age from strings like 'Age at disappearance44'."""
    if not text:
        return None
    m = re.search(r'\d+', text)
    return int(m.group()) if m else None


def _fetch_list_page(session: requests.Session, paged: int) -> list[dict]:
    """
    POST the XHR endpoint with age=child and paged=N.
    Returns list of dicts: {name, url, region, photo_url}
    """
    time.sleep(1.5)
    data = {
        "action":      "mp_filter_appeals_xhr",
        "search_term": "",
        "region":      "",
        "date_missing": "",
        "age":         "child",
        "gender":      "",
        "paged":       str(paged),
    }
    resp = session.post(LIST_URL, data=data, headers=XHR_HEADERS, timeout=30)
    resp.raise_for_status()

    soup  = BeautifulSoup(resp.text, "lxml")
    cards = soup.select(".card--person")
    results = []

    for card in cards:
        link_el  = card.select_one("a.card__link")
        title_el = card.select_one(".card__title")
        img_el   = card.select_one("img.card__image")
        region_el = card.select_one(".post-meta__item")

        url   = link_el["href"] if link_el and link_el.get("href") else None
        name  = clean_text(title_el.get_text()) if title_el else None
        photo = img_el["src"] if img_el and img_el.get("src") else None
        region = clean_text(region_el.get_text()) if region_el else None

        if not url or not name:
            continue

        if photo and not photo.startswith("http"):
            photo = BASE_URL + photo

        # Extract slug-based case ID from URL
        # e.g. /help-us-find/adam-ming-25-502420 → adam-ming-25-502420
        slug    = url.rstrip("/").split("/")[-1]
        # Reference number is at end: 25-502420
        ref_match = re.search(r'(\d{2,4}-\d{3,9})$', slug)
        case_id   = ref_match.group(1) if ref_match else slug

        results.append({
            "name":     name,
            "url":      url,
            "region":   region,
            "photo":    photo,
            "case_id":  case_id,
            "slug":     slug,
        })

    return results


def _fetch_detail(session: requests.Session, url: str) -> dict:
    """
    Fetch the individual appeal page and extract structured fields.
    Returns dict with: age, city, date_missing, reference
    """
    time.sleep(1.2)
    try:
        resp = session.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except Exception:
        return {}

    soup = BeautifulSoup(resp.text, "lxml")

    # All detail fields live in elements whose text matches patterns like:
    # "Age at disappearance44"
    # "Missing fromLiverpool, Merseyside"
    # "Missing since15/11/2024"
    # "Reference No25-502420"

    result = {}

    # Find the details container — look for any element containing "Missing since"
    full_text = soup.get_text(" ", strip=True)

    # Age at disappearance
    m = re.search(r'Age at disappearance\s*(\d+)', full_text, re.IGNORECASE)
    if m:
        result["age"] = int(m.group(1))

    # Missing from
    m = re.search(r'Missing from\s*(.+?)(?:Missing since|Reference|$)', full_text, re.IGNORECASE)
    if m:
        result["city"] = clean_text(m.group(1))

    # Missing since
    m = re.search(r'Missing since\s*(\d{1,2}/\d{1,2}/\d{2,4})', full_text, re.IGNORECASE)
    if m:
        result["date_missing"] = _parse_date(m.group(1))

    # Reference No
    m = re.search(r'Reference No\s*([\d\-]+)', full_text, re.IGNORECASE)
    if m:
        result["reference"] = m.group(1).strip()

    # Gender — look for He/She/They pronoun hints or explicit label
    m = re.search(r'\b(male|female|boy|girl|man|woman)\b', full_text[:500], re.IGNORECASE)
    if m:
        word = m.group(1).lower()
        result["gender"] = "Male" if word in ("male", "boy", "man") else "Female"

    # Photo — og:image is most reliable
    og_img = soup.select_one('meta[property="og:image"]')
    if og_img and og_img.get("content"):
        result["photo"] = og_img["content"]

    return result


class MissingPeopleUKScraper(BaseScraper):
    name = "missing_people_uk"

    def run(self) -> dict:
        found = new = updated = skipped = errors = 0
        paged = 1

        self.logger.info("Starting Missing People UK scrape (children only)...")

        while True:
            try:
                cards = _fetch_list_page(self.http, paged)
            except Exception as exc:
                self.logger.error("List page %d failed: %s", paged, exc)
                errors += 1
                break

            if not cards:
                self.logger.info("No more cards at paged=%d — done.", paged)
                break

            self.logger.info("Page %d: %d cards", paged, len(cards))
            found += len(cards)

            for card in cards:
                try:
                    # Fetch detail page for structured fields
                    detail = _fetch_detail(self.http, card["url"])

                    age = detail.get("age") or _parse_age(card.get("name", ""))

                    # Skip adults (age filter=child should handle this, but double-check)
                    if age is not None and age >= 18:
                        skipped += 1
                        continue

                    # Parse city/state from "Liverpool, Merseyside" style
                    city_raw = detail.get("city", "") or card.get("region", "")
                    city_parts = [p.strip() for p in city_raw.split(",")] if city_raw else []
                    city  = city_parts[0] if city_parts else None
                    state = city_parts[1] if len(city_parts) > 1 else card.get("region")

                    # Name split
                    name   = card["name"]
                    parts  = name.split()
                    first  = parts[0] if parts else ""
                    last   = parts[-1] if len(parts) > 1 else ""

                    update_data = {
                        "source_url":           card["url"],
                        "full_name":            name,
                        "first_name":           first,
                        "last_name":            last,
                        "age_at_disappearance": age,
                        "gender":               detail.get("gender"),
                        "date_missing":         detail.get("date_missing"),
                        "city_last_seen":       city,
                        "state_last_seen":      state,
                        "country_last_seen":    "United Kingdom",
                        "photo_url":            detail.get("photo") or card.get("photo"),
                        "raw_data":             safe_json({**card, **detail}),
                    }

                    _, created = self.upsert(
                        MissingPerson,
                        lookup_kwargs={"source": self.name, "source_id": card["case_id"]},
                        update_kwargs=update_data,
                    )

                    if created:
                        new += 1
                        self.logger.info("NEW: %s (age=%s, from=%s)", name, age, city_raw)
                    else:
                        updated += 1

                except Exception as exc:
                    self.logger.error("Card error [%s]: %s", card.get("name", "?"), exc)
                    errors += 1

            paged += 1
            # Safety cap — site has ~27 pages total
            if paged > 50:
                break

        self.logger.info(
            "Missing People UK done. found=%d new=%d updated=%d skipped=%d errors=%d",
            found, new, updated, skipped, errors,
        )
        return {"found": found, "new": new, "updated": updated,
                "skipped": skipped, "errors": errors}
