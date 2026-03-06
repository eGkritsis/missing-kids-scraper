"""
scrapers/gmcn.py

Global Missing Children's Network (GMCN) scraper.
API: POST https://gmcngine-api.globalmissingkids.org/api/cases/search

Response structure:
  {"cases": {"total": 5752, "results": [...]}}

Each result has: caseId, fullName, birthDate (ms timestamp), missingSince (ms),
  country, state, city, type, agencyCode, status, portrait, childId
"""

import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from database.models import MissingPerson, init_db
from scrapers.base import BaseScraper
from utils.helpers import clean_text, safe_json

API_URL   = "https://gmcngine-api.globalmissingkids.org/api/cases/search"
HEADERS   = {
    "Origin":       "https://find.globalmissingkids.org",
    "Referer":      "https://find.globalmissingkids.org/",
    "Content-Type": "application/json",
    "Accept":       "application/json",
    "User-Agent":   "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}
PAGE_SIZE = 24
WORKERS   = 6


def ms_to_date(ms):
    """Convert millisecond Unix timestamp to date object."""
    if not ms:
        return None
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).date()
    except Exception:
        return None


class GMCNScraper(BaseScraper):
    name = "gmcn"

    def run(self) -> dict:
        self.logger.info("Starting GMCN scrape...")

        # Step 1: get total
        try:
            first = self._fetch_page(0, 1)
            total = first["cases"]["total"]
            self.logger.info("GMCN total cases: %d", total)
        except Exception as exc:
            self.logger.error("GMCN initial fetch failed: %s", exc)
            return {"found": 0, "new": 0, "updated": 0, "errors": 1}

        pages  = list(range(0, (total // PAGE_SIZE) + 1))
        found  = [0]; new = [0]; updated = [0]; errors = [0]
        lock   = Lock()
        _, Session = init_db()

        def process_page(page_num):
            session = Session()
            try:
                time.sleep(0.5)
                data    = self._fetch_page(page_num, PAGE_SIZE)
                results = data["cases"]["results"]
                b_new   = b_upd = 0

                for case in results:
                    try:
                        case_id = str(case.get("caseId", "")).strip()
                        if not case_id:
                            continue

                        update_data = _build_record(case)
                        inst = session.query(MissingPerson).filter_by(
                            source="gmcn", source_id=case_id
                        ).first()

                        if inst is None:
                            inst = MissingPerson(source="gmcn", source_id=case_id, **update_data)
                            session.add(inst)
                            b_new += 1
                        else:
                            for k, v in update_data.items():
                                setattr(inst, k, v)
                            b_upd += 1
                        session.commit()

                    except Exception as e:
                        session.rollback()
                        self.logger.debug("Row error: %s", e)

                with lock:
                    found[0]   += len(results)
                    new[0]     += b_new
                    updated[0] += b_upd

                self.logger.info("Page %d: %d cases (new=%d updated=%d)",
                                 page_num, len(results), b_new, b_upd)

            except Exception as exc:
                self.logger.error("Page %d failed: %s", page_num, exc)
                with lock:
                    errors[0] += 1
            finally:
                session.close()

        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = [pool.submit(process_page, p) for p in pages]
            for f in as_completed(futures):
                pass

        self.logger.info("GMCN done. found=%d new=%d updated=%d errors=%d",
                         found[0], new[0], updated[0], errors[0])
        return {"found": found[0], "new": new[0], "updated": updated[0], "errors": errors[0]}

    def _fetch_page(self, page: int, size: int) -> dict:
        payload = {"request": {
            "page": page, "size": size,
            "sort": [{"missingSince": "desc"}, "fullName"],
            "search": "", "status": "open",
        }}
        resp = self.http.post(API_URL, json=payload, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        return resp.json()


def _build_record(case: dict) -> dict:
    full  = clean_text(case.get("fullName", ""))
    parts = full.split() if full else []
    first = parts[0] if parts else ""
    last  = parts[-1] if len(parts) > 1 else ""

    status      = (case.get("status") or "").lower()
    is_resolved = status in ("closed", "resolved", "found")

    return {
        "source_url":           f"https://find.globalmissingkids.org/case/{case.get('caseId','')}",
        "first_name":           first,
        "last_name":            last,
        "full_name":            full,
        "date_of_birth":        ms_to_date(case.get("birthDate")),
        "date_missing":         ms_to_date(case.get("missingSince")),
        "city_last_seen":       clean_text(case.get("city")),
        "state_last_seen":      clean_text(case.get("state")),
        "country_last_seen":    clean_text(case.get("country")),
        "case_type":            clean_text(case.get("type")),
        "contact_agency":       clean_text(case.get("agencyCode")),
        "photo_url":            case.get("portrait") or None,
        "is_resolved":          is_resolved,
        "raw_data":             safe_json(case),
    }
