"""
scrapers/international.py

Scrapers for official missing children databases outside the US.

Covered:
  - Interpol Yellow Notices API        (global)
  - Global Missing Children's Network  (global, multilingual)
  - Missing People UK                  (United Kingdom)
  - Child Focus                        (Belgium / EU)
  - RCMP Missing Persons               (Canada)
"""

import re
import time
from bs4 import BeautifulSoup

from database.models import MissingPerson
from scrapers.base import BaseScraper
from utils.helpers import clean_text, parse_date, safe_json

BROWSER_UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


def get(session, url, delay=2.0, ua=None, verify=True, **kwargs):
    time.sleep(delay)
    headers = {"User-Agent": ua or BROWSER_UA}
    resp = session.get(url, headers=headers, timeout=30, verify=verify, **kwargs)
    resp.raise_for_status()
    return resp


class InterpolScraper(BaseScraper):
    name = "interpol"
    SEARCH_URL = "https://ws-public.interpol.int/notices/v1/yellow"

    def run(self) -> dict:
        found = new = updated = errors = 0
        page = 1
        self.logger.info("Starting Interpol Yellow Notices scrape...")

        while True:
            params = {"ageMin": 0, "ageMax": 18, "resultPerPage": 160, "page": page}
            try:
                resp = get(self.http, self.SEARCH_URL, params=params, delay=2.0)
                data = resp.json()
            except Exception as exc:
                self.logger.error("Interpol page %d failed: %s", page, exc)
                errors += 1
                break

            notices = data.get("_embedded", {}).get("notices", [])
            total   = data.get("total", 0)

            if not notices:
                break

            self.logger.info("Interpol page %d: %d/%d", page, len(notices), total)
            found += len(notices)

            for n in notices:
                try:
                    _, created = self._upsert(n)
                    if created: new += 1
                    else: updated += 1
                except Exception as exc:
                    self.logger.error("Interpol save: %s", exc)
                    errors += 1

            if page * 160 >= total:
                break
            page += 1

        self.logger.info("Interpol done. found=%d new=%d updated=%d errors=%d", found, new, updated, errors)
        return {"found": found, "new": new, "updated": updated, "errors": errors}

    def _upsert(self, n):
        notice_id = n.get("entity_id", "").replace("/", "_")
        forename  = clean_text(n.get("forename", ""))
        surname   = clean_text(n.get("name", ""))
        return self.upsert(
            MissingPerson,
            lookup_kwargs={"source": "interpol", "source_id": notice_id},
            update_kwargs={
                "source_url":        f"https://www.interpol.int/en/How-we-work/Notices/Yellow-Notices/View-Yellow-Notices/{notice_id}",
                "first_name":        forename,
                "last_name":         surname,
                "full_name":         " ".join(filter(None, [forename, surname])),
                "date_of_birth":     parse_date(n.get("date_of_birth")),
                "gender":            clean_text(n.get("sex_id")),
                "nationality":       clean_text(n.get("nationality")),
                "country_last_seen": clean_text(n.get("country_of_birth")),
                "photo_url":         n.get("_links", {}).get("thumbnail", {}).get("href"),
                "raw_data":          safe_json(n),
            },
        )


class GlobalMissingKidsScraper(BaseScraper):
    name = "global_missing_kids"
    BASE_URL   = "https://globalmissingkids.org"
    SEARCH_URL = "https://globalmissingkids.org/missing/missing-children-search/"

    def run(self) -> dict:
        found = new = updated = errors = 0
        page = 1
        self.logger.info("Starting Global Missing Children's Network scrape...")

        while True:
            url = f"{self.SEARCH_URL}?page={page}"
            try:
                resp = get(self.http, url, delay=2.0)
                soup = BeautifulSoup(resp.text, "lxml")
            except Exception as exc:
                self.logger.error("GMCN page %d failed: %s", page, exc)
                errors += 1
                break

            cards = (soup.select(".missing-child-item") or soup.select(".child-result") or
                     soup.select(".views-row") or soup.select("article") or soup.select(".case-listing"))

            if not cards:
                self.logger.info("GMCN: no more cards at page %d", page)
                break

            self.logger.info("GMCN page %d: %d records", page, len(cards))
            found += len(cards)

            for card in cards:
                try:
                    name_el    = card.select_one("h2, h3, .name, [class*='name'], .title")
                    link_el    = card.select_one("a[href]")
                    country_el = card.select_one("[class*='country'], .country")
                    name    = clean_text(name_el.get_text()) if name_el else None
                    url     = link_el["href"] if link_el else None
                    country = clean_text(country_el.get_text()) if country_el else None
                    if not name:
                        continue
                    if url and not url.startswith("http"):
                        url = self.BASE_URL + url
                    parts   = name.split()
                    case_id = re.sub(r'[^a-zA-Z0-9]', '_', name)[:64]
                    _, created = self.upsert(
                        MissingPerson,
                        lookup_kwargs={"source": self.name, "source_id": case_id},
                        update_kwargs={"source_url": url, "full_name": name,
                                       "first_name": parts[0] if parts else "",
                                       "last_name": parts[-1] if len(parts) > 1 else "",
                                       "country_last_seen": country,
                                       "raw_data": safe_json({"name": name, "url": url})},
                    )
                    if created: new += 1
                    else: updated += 1
                except Exception as exc:
                    self.logger.error("GMCN card: %s", exc)
                    errors += 1

            page += 1
            if page > 100:
                break

        self.logger.info("GMCN done. found=%d new=%d updated=%d errors=%d", found, new, updated, errors)
        return {"found": found, "new": new, "updated": updated, "errors": errors}


class MissingPeopleUKScraper(BaseScraper):
    name = "missing_people_uk"
    BASE_URL   = "https://www.missingpeople.org.uk"
    SEARCH_URL = "https://www.missingpeople.org.uk/appeal-search"

    def run(self) -> dict:
        found = new = updated = errors = 0
        page = 1
        self.logger.info("Starting Missing People UK scrape...")

        while True:
            url = f"{self.SEARCH_URL}?page={page}"
            try:
                resp = get(self.http, url, delay=2.0)
                soup = BeautifulSoup(resp.text, "lxml")
            except Exception as exc:
                self.logger.error("UK page %d failed: %s", page, exc)
                errors += 1
                break

            cards = (soup.select(".missing-person") or soup.select(".appeal-item") or
                     soup.select(".case-item") or soup.select("article") or
                     soup.select(".person-card") or soup.select(".views-row"))

            if not cards:
                self.logger.info("UK: no more cards at page %d", page)
                break

            self.logger.info("UK page %d: %d records", page, len(cards))
            found += len(cards)

            for card in cards:
                try:
                    name_el = card.select_one("h2, h3, .name, [class*='name'], .title")
                    link_el = card.select_one("a[href]")
                    name    = clean_text(name_el.get_text()) if name_el else None
                    url     = link_el["href"] if link_el else None
                    if not name:
                        continue
                    if url and not url.startswith("http"):
                        url = self.BASE_URL + url
                    age_el  = card.select_one("[class*='age'], .age, .dob")
                    age_str = clean_text(age_el.get_text()) if age_el else None
                    age = None
                    if age_str:
                        m = re.search(r'\d+', age_str)
                        if m: age = int(m.group())
                    parts   = name.split()
                    case_id = re.sub(r'[^a-zA-Z0-9]', '_', name)[:64]
                    _, created = self.upsert(
                        MissingPerson,
                        lookup_kwargs={"source": self.name, "source_id": case_id},
                        update_kwargs={"source_url": url, "full_name": name,
                                       "first_name": parts[0] if parts else "",
                                       "last_name": parts[-1] if len(parts) > 1 else "",
                                       "age_at_disappearance": age,
                                       "country_last_seen": "United Kingdom",
                                       "raw_data": safe_json({"name": name, "url": url})},
                    )
                    if created: new += 1
                    else: updated += 1
                except Exception as exc:
                    self.logger.error("UK card: %s", exc)
                    errors += 1

            page += 1
            if page > 50:
                break

        self.logger.info("Missing People UK done. found=%d new=%d updated=%d errors=%d", found, new, updated, errors)
        return {"found": found, "new": new, "updated": updated, "errors": errors}


class ChildFocusScraper(BaseScraper):
    name = "child_focus"
    BASE_URL   = "https://www.childfocus.be"
    SEARCH_URL = "https://www.childfocus.be/en/missing-children"

    def run(self) -> dict:
        found = new = updated = errors = 0
        self.logger.info("Starting Child Focus (Belgium/EU) scrape...")
        try:
            resp = get(self.http, self.SEARCH_URL, delay=2.0)
            soup = BeautifulSoup(resp.text, "lxml")
            cards = (soup.select(".missing-child") or soup.select(".child-card") or
                     soup.select(".case-item") or soup.select("article") or
                     soup.select(".views-row") or soup.select("[class*='missing']"))
            self.logger.info("Child Focus: %d cards", len(cards))
            found = len(cards)
            for card in cards:
                try:
                    name_el = card.select_one("h2, h3, .name, .title, [class*='name']")
                    link_el = card.select_one("a[href]")
                    name    = clean_text(name_el.get_text()) if name_el else None
                    url     = link_el["href"] if link_el else None
                    if not name: continue
                    if url and not url.startswith("http"):
                        url = self.BASE_URL + url
                    parts   = name.split()
                    case_id = re.sub(r'[^a-zA-Z0-9]', '_', name)[:64]
                    _, created = self.upsert(
                        MissingPerson,
                        lookup_kwargs={"source": self.name, "source_id": case_id},
                        update_kwargs={"source_url": url, "full_name": name,
                                       "first_name": parts[0] if parts else "",
                                       "last_name": parts[-1] if len(parts) > 1 else "",
                                       "country_last_seen": "Belgium",
                                       "raw_data": safe_json({"name": name, "url": url})},
                    )
                    if created: new += 1
                    else: updated += 1
                except Exception as exc:
                    self.logger.error("Child Focus card: %s", exc)
                    errors += 1
        except Exception as exc:
            self.logger.error("Child Focus failed: %s", exc)
            errors += 1

        self.logger.info("Child Focus done. found=%d new=%d updated=%d errors=%d", found, new, updated, errors)
        return {"found": found, "new": new, "updated": updated, "errors": errors}


class RCMPScraper(BaseScraper):
    name = "rcmp_canada"
    SEARCH_URL = "https://www.rcmp-grc.gc.ca/en/missing-persons-search"

    def run(self) -> dict:
        found = new = updated = errors = 0
        self.logger.info("Starting RCMP Canada scrape...")
        try:
            # verify=False: RCMP server has an incomplete SSL cert chain
            import urllib3; urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            resp = get(self.http, self.SEARCH_URL, delay=2.0, verify=False)
            soup = BeautifulSoup(resp.text, "lxml")
            rows = (soup.select("table tbody tr") or soup.select(".views-row") or
                    soup.select("article") or soup.select(".mp-result"))
            self.logger.info("RCMP: %d records", len(rows))
            found = len(rows)
            for row in rows:
                try:
                    name_el = row.select_one("td, h3, h2, .name, a")
                    link_el = row.select_one("a[href]")
                    name    = clean_text(name_el.get_text()) if name_el else None
                    url     = link_el["href"] if link_el else None
                    if not name or len(name) < 3: continue
                    if url and not url.startswith("http"):
                        url = "https://www.rcmp-grc.gc.ca" + url
                    parts   = name.split()
                    case_id = re.sub(r'[^a-zA-Z0-9]', '_', name)[:64]
                    _, created = self.upsert(
                        MissingPerson,
                        lookup_kwargs={"source": self.name, "source_id": case_id},
                        update_kwargs={"source_url": url, "full_name": name,
                                       "first_name": parts[0] if parts else "",
                                       "last_name": parts[-1] if len(parts) > 1 else "",
                                       "country_last_seen": "Canada",
                                       "raw_data": safe_json({"name": name, "url": url})},
                    )
                    if created: new += 1
                    else: updated += 1
                except Exception as exc:
                    self.logger.error("RCMP row: %s", exc)
                    errors += 1
        except Exception as exc:
            self.logger.error("RCMP failed: %s", exc)
            errors += 1

        self.logger.info("RCMP done. found=%d new=%d updated=%d errors=%d", found, new, updated, errors)
        return {"found": found, "new": new, "updated": updated, "errors": errors}


INTERNATIONAL_SCRAPERS = {
    "interpol":            InterpolScraper,
    "global_missing_kids": GlobalMissingKidsScraper,
    "missing_people_uk":   MissingPeopleUKScraper,
    "child_focus":         ChildFocusScraper,
    "rcmp_canada":         RCMPScraper,
}
