"""
scrapers/interpol.py

Async scraper for INTERPOL Yellow Notices (missing persons).
Uses the public ws-public.interpol.int REST API — no auth required.

API:
  List:   GET https://ws-public.interpol.int/notices/v1/yellow
          ?page=N&resultPerPage=160&ageMin=0&ageMax=17
  Detail: GET {notice._links.self.href}   ← only for photo URL, best-effort

Strategy:
  - Filter children server-side via ageMax=17 (avoids fetching adults at all)
  - Use resultPerPage=160 (site's documented max) to minimise page count
  - Detail fetches are optional/best-effort — core data lives in list response
  - Retry on 429/5xx with exponential back-off (tenacity)
  - Semaphore limits concurrent detail requests to avoid bans
  - Falls back gracefully if detail endpoint 403s (uses list data only)
"""

import asyncio
import logging
from datetime import date, datetime

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from database.models import MissingPerson, init_db
from scrapers.base import BaseScraper
from utils.helpers import safe_json

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LIST_API    = "https://ws-public.interpol.int/notices/v1/yellow"
BASE_URL    = "https://www.interpol.int"
NOTICE_URL  = BASE_URL + "/How-we-work/Notices/View-Yellow-Notices#{entity_id}"

LIST_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept":     "application/json",
}

DETAIL_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept":     "application/json",
}

RESULTS_PER_PAGE  = 160   # API max
CHILD_MAX_AGE     = 17
MAX_CONCURRENT    = 5     # conservative — detail API rate-limits aggressively
PAGE_DELAY        = 1.0   # seconds between list pages
DETAIL_DELAY      = 0.3   # seconds between detail requests (inside semaphore)

logger = logging.getLogger("interpol")


# ---------------------------------------------------------------------------
# Age helpers
# ---------------------------------------------------------------------------

def _age_from_dob(dob_str: str | None) -> int | None:
    """Calculate current age from 'YYYY/MM/DD' string."""
    if not dob_str:
        return None
    try:
        dob   = datetime.strptime(dob_str, "%Y/%m/%d").date()
        today = date.today()
        return today.year - dob.year - (
            (today.month, today.day) < (dob.month, dob.day)
        )
    except (ValueError, TypeError):
        return None


def _is_minor(notice: dict) -> bool:
    """Return True if this notice is for someone ≤17 years old."""
    api_age = notice.get("age")
    if api_age is not None:
        return int(api_age) <= CHILD_MAX_AGE

    # Fall back to DOB calculation
    calc_age = _age_from_dob(notice.get("date_of_birth"))
    if calc_age is not None:
        return calc_age <= CHILD_MAX_AGE

    # Unknown age — include by default (better safe than sorry)
    return True


# ---------------------------------------------------------------------------
# Async HTTP helpers with retry
# ---------------------------------------------------------------------------

class RateLimitError(Exception):
    pass


@retry(
    retry=retry_if_exception_type(RateLimitError),
    wait=wait_exponential(multiplier=2, min=4, max=60),
    stop=stop_after_attempt(5),
    reraise=True,
)
async def _get_json(client: httpx.AsyncClient, url: str,
                    params: dict = None, headers: dict = None) -> dict | None:
    resp = await client.get(url, params=params,
                            headers=headers or LIST_HEADERS, timeout=30)
    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", 10))
        logger.warning("Rate limited — waiting %ds", retry_after)
        await asyncio.sleep(retry_after)
        raise RateLimitError("429 rate limit")
    if resp.status_code == 403:
        logger.debug("403 on %s — skipping", url)
        return None
    if resp.status_code >= 500:
        raise RateLimitError(f"HTTP {resp.status_code}")
    if resp.status_code != 200:
        logger.warning("HTTP %d on %s", resp.status_code, url)
        return None
    try:
        return resp.json()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Fetch functions
# ---------------------------------------------------------------------------

async def _fetch_list_page(client: httpx.AsyncClient, page: int) -> tuple[list, int]:
    """
    Returns (notices_list, total_count).
    Uses server-side age filter ageMax=17 to only get children.
    """
    params = {
        "page":          page,
        "resultPerPage": RESULTS_PER_PAGE,
        "ageMin":        0,
        "ageMax":        CHILD_MAX_AGE,
    }
    data = await _get_json(client, LIST_API, params=params)
    if not data:
        return [], 0

    notices = data.get("_embedded", {}).get("notices", [])
    total   = data.get("total", 0)
    return notices, total


async def _fetch_detail(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    self_href: str,
) -> dict | None:
    """
    Fetch individual notice detail for photo URL and extra fields.
    Best-effort — returns None on any error.
    """
    async with semaphore:
        await asyncio.sleep(DETAIL_DELAY)
        try:
            data = await _get_json(client, self_href, headers=DETAIL_HEADERS)
            return data
        except Exception:
            return None


async def _scrape_all() -> list[dict]:
    """
    Full async scrape.  Returns list of enriched notice dicts.
    """
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    all_records: list[dict] = []

    async with httpx.AsyncClient(
        headers=LIST_HEADERS,
        http2=False,
        follow_redirects=True,
        timeout=30,
    ) as client:

        # --- page 1 to discover total ---
        page = 1
        notices, total = await _fetch_list_page(client, page)
        if not notices:
            logger.warning("No notices on page 1 — aborting")
            return []

        total_pages = max(1, -(-total // RESULTS_PER_PAGE))  # ceiling div
        logger.info("Total child notices: %d  (%d pages)", total, total_pages)

        async def process_page(pg_notices: list) -> list[dict]:
            """Fetch details for a batch of notices concurrently."""
            tasks = []
            for n in pg_notices:
                href = n.get("_links", {}).get("self", {}).get("href")
                tasks.append(
                    _fetch_detail(client, semaphore, href) if href else
                    asyncio.coroutine(lambda: None)()  # no-op
                )
            details = await asyncio.gather(*tasks, return_exceptions=True)

            records = []
            for n, det in zip(pg_notices, details):
                # Merge detail data on top of list data (detail has more fields)
                merged = dict(n)
                if isinstance(det, dict) and det:
                    merged.update(det)

                # Secondary age check (list already filtered, but be safe)
                if not _is_minor(merged):
                    continue

                # Extract photo from detail thumbnails or images
                photo = None
                if isinstance(det, dict) and det:
                    imgs = det.get("_links", {}).get("images", {})
                    # Prefer href of first thumbnail
                    thumb = imgs.get("href") if imgs else None
                    if not thumb:
                        # Some notices have _embedded.images
                        emb_imgs = det.get("_embedded", {}).get("images", [])
                        if emb_imgs:
                            thumb = emb_imgs[0].get("_links", {}).get(
                                "self", {}).get("href")
                    photo = thumb

                merged["_photo_url"] = photo
                records.append(merged)
            return records

        # Process page 1
        all_records.extend(await process_page(notices))
        await asyncio.sleep(PAGE_DELAY)

        # Remaining pages
        for page in range(2, total_pages + 1):
            logger.info("Fetching page %d / %d", page, total_pages)
            pg_notices, _ = await _fetch_list_page(client, page)
            if not pg_notices:
                logger.info("Empty page %d — stopping", page)
                break
            all_records.extend(await process_page(pg_notices))
            await asyncio.sleep(PAGE_DELAY)

    return all_records


# ---------------------------------------------------------------------------
# Country code → name mapping (ISO 3166-1 alpha-2, common subset)
# ---------------------------------------------------------------------------

_CC = {
    "AF":"Afghanistan","AL":"Albania","DZ":"Algeria","AR":"Argentina",
    "AU":"Australia","AT":"Austria","AZ":"Azerbaijan","BS":"Bahamas",
    "BD":"Bangladesh","BY":"Belarus","BE":"Belgium","BO":"Bolivia",
    "BR":"Brazil","BG":"Bulgaria","KH":"Cambodia","CA":"Canada",
    "CL":"Chile","CN":"China","CO":"Colombia","CG":"Congo",
    "CR":"Costa Rica","HR":"Croatia","CU":"Cuba","CZ":"Czech Republic",
    "DK":"Denmark","DO":"Dominican Republic","EC":"Ecuador","EG":"Egypt",
    "SV":"El Salvador","ET":"Ethiopia","FI":"Finland","FR":"France",
    "DE":"Germany","GH":"Ghana","GR":"Greece","GT":"Guatemala",
    "HN":"Honduras","HU":"Hungary","IN":"India","ID":"Indonesia",
    "IR":"Iran","IQ":"Iraq","IE":"Ireland","IL":"Israel","IT":"Italy",
    "JM":"Jamaica","JP":"Japan","JO":"Jordan","KZ":"Kazakhstan",
    "KE":"Kenya","KW":"Kuwait","LB":"Lebanon","LY":"Libya",
    "LT":"Lithuania","MK":"North Macedonia","MY":"Malaysia","MX":"Mexico",
    "MA":"Morocco","MZ":"Mozambique","MM":"Myanmar","NP":"Nepal",
    "NL":"Netherlands","NZ":"New Zealand","NI":"Nicaragua","NG":"Nigeria",
    "NO":"Norway","PK":"Pakistan","PA":"Panama","PY":"Paraguay",
    "PE":"Peru","PH":"Philippines","PL":"Poland","PT":"Portugal",
    "RO":"Romania","RU":"Russia","SA":"Saudi Arabia","SN":"Senegal",
    "RS":"Serbia","SK":"Slovakia","ZA":"South Africa","KR":"South Korea",
    "ES":"Spain","LK":"Sri Lanka","SE":"Sweden","CH":"Switzerland",
    "SY":"Syria","TW":"Taiwan","TZ":"Tanzania","TH":"Thailand",
    "TN":"Tunisia","TR":"Turkey","UA":"Ukraine","GB":"United Kingdom",
    "US":"United States","UY":"Uruguay","UZ":"Uzbekistan","VE":"Venezuela",
    "VN":"Vietnam","YE":"Yemen","ZM":"Zambia","ZW":"Zimbabwe",
}

def _cc_to_name(codes: list | None) -> str | None:
    if not codes:
        return None
    names = [_CC.get(c, c) for c in codes]
    return ", ".join(names)


# ---------------------------------------------------------------------------
# BaseScraper integration
# ---------------------------------------------------------------------------

class InterpolScraper(BaseScraper):
    name = "interpol"

    def run(self) -> dict:
        self.logger.info("Starting Interpol Yellow Notices async scrape "
                         "(children ≤%d only)...", CHILD_MAX_AGE)

        records = asyncio.run(_scrape_all())
        self.logger.info("Fetched %d child records — saving to DB...", len(records))

        new = updated = skipped = errors = 0
        _, Session = init_db()
        db = Session()

        try:
            for rec in records:
                try:
                    entity_id = rec.get("entity_id")
                    last_name = (rec.get("name") or "").strip()
                    first_name = (rec.get("forename") or "").strip()
                    full_name = f"{first_name} {last_name}".strip()

                    if not entity_id or not full_name:
                        skipped += 1
                        continue

                    dob_str  = rec.get("date_of_birth")
                    calc_age = _age_from_dob(dob_str)
                    api_age  = rec.get("age")
                    age      = api_age if api_age is not None else calc_age

                    dob = None
                    if dob_str:
                        try:
                            dob = datetime.strptime(dob_str, "%Y/%m/%d").date()
                        except ValueError:
                            pass

                    nats     = rec.get("nationalities") or []
                    nat_str  = _cc_to_name(nats)
                    country  = _CC.get(nats[0], nats[0]) if nats else None

                    sex_map  = {"M": "Male", "F": "Female", "U": "Unknown"}
                    gender   = sex_map.get(rec.get("sex_id") or rec.get("sex") or "", None)

                    photo    = rec.get("_photo_url")

                    update_data = {
                        "source_url":           NOTICE_URL.format(entity_id=entity_id),
                        "full_name":            full_name,
                        "first_name":           first_name,
                        "last_name":            last_name,
                        "date_of_birth":        dob,
                        "age_at_disappearance": age,
                        "gender":               gender,
                        "nationality":          nat_str,
                        "country_last_seen":    country,
                        "photo_url":            photo,
                        "raw_data":             safe_json(rec),
                    }

                    inst = db.query(MissingPerson).filter_by(
                        source=self.name, source_id=entity_id
                    ).first()

                    if inst is None:
                        db.add(MissingPerson(
                            source=self.name,
                            source_id=entity_id,
                            **update_data,
                        ))
                        new += 1
                        self.logger.info("NEW: %s (age=%s, nat=%s)",
                                         full_name, age, nat_str)
                    else:
                        for k, v in update_data.items():
                            setattr(inst, k, v)
                        updated += 1

                    db.commit()

                except Exception as exc:
                    db.rollback()
                    self.logger.error("DB [%s]: %s",
                                      rec.get("entity_id", "?"), exc)
                    errors += 1
        finally:
            db.close()

        self.logger.info(
            "Interpol done. found=%d new=%d updated=%d skipped=%d errors=%d",
            len(records), new, updated, skipped, errors,
        )
        return {
            "found":   len(records),
            "new":     new,
            "updated": updated,
            "skipped": skipped,
            "errors":  errors,
        }
