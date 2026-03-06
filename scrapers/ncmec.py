"""
scrapers/ncmec.py

Scraper for the National Center for Missing & Exploited Children (NCMEC).
Public search: https://www.missingkids.org/gethelpnow/isyourchildmissing

NCMEC publishes a public-facing search interface backed by a JSON API.
We call the same endpoint the website uses, with polite rate-limiting.
No authentication is required for the public search.
"""

import json
from typing import Optional

from database.models import MissingPerson
from scrapers.base import BaseScraper
from utils.helpers import (
    clean_text, parse_date, height_to_cm, lbs_to_kg, safe_json, polite_post
)

NCMEC_API = "https://api.missingkids.org/missingkids/servlet/JSONDataServlet"

# Case type codes used by NCMEC
CASE_TYPE_MAP = {
    "EA": "Endangered Adult",
    "EC": "Endangered Child",
    "FA": "Family Abduction",
    "LO": "Lost / Injured",
    "NA": "Non-Family Abduction",
    "OU": "Unknown",
    "RU": "Runaway",
    "UU": "Unknown",
}


class NCMECScraper(BaseScraper):
    name = "ncmec"

    # NCMEC paginates at 25 records per page
    PAGE_SIZE = 25

    def run(self) -> dict:
        found = new = updated = errors = 0
        page = 1

        self.logger.info("Starting NCMEC scrape...")

        while True:
            try:
                records = self._fetch_page(page)
            except Exception as exc:
                self.logger.error("Page %d fetch failed: %s", page, exc)
                errors += 1
                break

            if not records:
                self.logger.info("No more records at page %d — done.", page)
                break

            self.logger.info("Page %d: %d records", page, len(records))
            found += len(records)

            for raw in records:
                try:
                    _, created = self._upsert_record(raw)
                    if created:
                        new += 1
                    else:
                        updated += 1
                except Exception as exc:
                    self.logger.error("Failed to save record %s: %s", raw.get("caseNumber"), exc)
                    errors += 1

            # NCMEC returns fewer than PAGE_SIZE on the last page
            if len(records) < self.PAGE_SIZE:
                break
            page += 1

        self.logger.info("NCMEC done. found=%d new=%d updated=%d errors=%d",
                         found, new, updated, errors)
        return {"found": found, "new": new, "updated": updated, "errors": errors}

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _fetch_page(self, page: int) -> list[dict]:
        """
        Call the NCMEC JSON API for one page of results.
        The endpoint accepts POST with form-encoded parameters.
        """
        params = {
            "action": "publicSearchChild",
            "searchLang": "en_US",
            "missType": "child",           # child cases only
            "rstatus": "1",                # 1 = currently missing
            "ageTo": "17",                 # minors only (under 18)
            "rows": str(self.PAGE_SIZE),
            "start": str((page - 1) * self.PAGE_SIZE),
        }
        resp = polite_post(self.http, NCMEC_API, data=params, delay=2.0)
        payload = resp.json()
        return payload.get("persons", [])

    def _upsert_record(self, raw: dict) -> tuple:
        case_num = str(raw.get("caseNumber", "")).strip()
        if not case_num:
            raise ValueError("Record missing caseNumber")

        # -- Parse physical details --
        height_ft = self._safe_int(raw.get("heightFeet"))
        height_in = self._safe_int(raw.get("heightInches"))
        weight_lbs = self._safe_float(raw.get("weight"))

        first = clean_text(raw.get("firstName") or "")
        last = clean_text(raw.get("lastName") or "")
        full = f"{first} {last}".strip()

        photo_url = None
        if raw.get("hasPoster"):
            photo_url = (
                f"https://www.missingkids.org/poster/NCMC/{case_num}/1"
            )

        case_type_code = raw.get("caseType", "UU")
        case_type_label = CASE_TYPE_MAP.get(case_type_code, case_type_code)

        update_data = {
            "source_url": f"https://www.missingkids.org/case/{case_num}",
            "first_name": first,
            "last_name": last,
            "full_name": full,
            "date_of_birth": parse_date(raw.get("dateOfBirth")),
            "age_at_disappearance": self._safe_int(raw.get("age")),
            "gender": clean_text(raw.get("sex")),
            "race_ethnicity": clean_text(raw.get("race")),
            "height_cm": height_to_cm(height_ft, height_in),
            "weight_kg": lbs_to_kg(weight_lbs),
            "eye_color": clean_text(raw.get("eyeColor")),
            "hair_color": clean_text(raw.get("hairColor")),
            "date_missing": parse_date(raw.get("dateMissing")),
            "city_last_seen": clean_text(raw.get("missingCity")),
            "state_last_seen": clean_text(raw.get("missingState")),
            "country_last_seen": clean_text(raw.get("missingCountry")) or "USA",
            "circumstances": clean_text(raw.get("circumstances")),
            "case_type": case_type_label,
            "ncic_number": clean_text(raw.get("ncmcNumber")),
            "photo_url": photo_url,
            "contact_agency": clean_text(raw.get("orgName")),
            "contact_phone": clean_text(raw.get("orgTelephone")),
            "raw_data": safe_json(raw),
        }

        return self.upsert(
            MissingPerson,
            lookup_kwargs={"source": "ncmec", "source_id": case_num},
            update_kwargs=update_data,
        )

    @staticmethod
    def _safe_int(value) -> Optional[int]:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _safe_float(value) -> Optional[float]:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
