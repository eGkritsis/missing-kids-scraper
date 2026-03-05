"""
scrapers/ncmec.py

NCMEC scraper using their public RSS feed.
The old JSON servlet API (JSONDataServlet) is no longer active.

For full database access, apply for official API access at:
https://www.missingkids.org/search  ("Request API access" link)

This scraper uses:
1. NCMEC RSS feed  - recent/new cases (no auth needed)
2. BeautifulSoup HTML scraper against the public search page
"""

import re
from bs4 import BeautifulSoup
import feedparser

from database.models import MissingPerson
from scrapers.base import BaseScraper
from utils.helpers import clean_text, parse_date, safe_json, polite_get

# Public RSS feeds NCMEC still maintains
NCMEC_RSS_FEEDS = [
    "https://www.missingkids.org/missingkids/servlet/XmlServlet?act=rss&missType=child&LanguageCountry=en_US",
    "https://www.missingkids.org/rss/missingkids.rss",
]

# Public HTML search - one page at a time
NCMEC_SEARCH_URL = "https://www.missingkids.org/gethelpnow/search/poster-results"


class NCMECScraper(BaseScraper):
    name = "ncmec"

    def run(self) -> dict:
        found = new = updated = errors = 0
        self.logger.info("Starting NCMEC scrape (RSS + HTML)...")

        # --- Try RSS feeds first ---
        for feed_url in NCMEC_RSS_FEEDS:
            try:
                import time; time.sleep(2)
                feed = feedparser.parse(feed_url)
                entries = feed.get("entries", [])
                self.logger.info("RSS feed %s: %d entries", feed_url, len(entries))
                for entry in entries:
                    try:
                        _, created = self._upsert_from_rss(entry)
                        found += 1
                        if created: new += 1
                        else: updated += 1
                    except Exception as e:
                        self.logger.error("RSS entry error: %s", e)
                        errors += 1
            except Exception as e:
                self.logger.warning("RSS feed failed (%s): %s", feed_url, e)

        # --- HTML scrape fallback ---
        try:
            html_found, html_new, html_updated, html_errors = self._scrape_html()
            found += html_found
            new += html_new
            updated += html_updated
            errors += html_errors
        except Exception as e:
            self.logger.error("HTML scrape failed: %s", e)
            errors += 1

        # --- Suggest official API if little data ---
        if found == 0:
            self.logger.warning(
                "No records retrieved. NCMEC may have changed their site. "
                "Consider applying for official API access at: "
                "https://www.missingkids.org/search (click 'Request API access')"
            )

        self.logger.info("NCMEC done. found=%d new=%d updated=%d errors=%d",
                         found, new, updated, errors)
        return {"found": found, "new": new, "updated": updated, "errors": errors}

    def _upsert_from_rss(self, entry: dict) -> tuple:
        url = entry.get("link", "")
        title = clean_text(entry.get("title", ""))
        summary = clean_text(entry.get("summary", ""))

        # Extract case number from URL if present
        case_id = None
        m = re.search(r'caseNum=([^&]+)', url)
        if m:
            case_id = m.group(1)
        if not case_id:
            case_id = re.sub(r'[^a-zA-Z0-9]', '_', title)[:64]

        # Best-effort name parse from title (e.g. "Missing: Jane Doe")
        name_part = re.sub(r'^(Missing|Endangered|Abducted)[:\s]*', '', title, flags=re.I).strip()
        parts = name_part.split()
        first = parts[0] if parts else ""
        last = parts[-1] if len(parts) > 1 else ""

        update_data = {
            "source_url": url,
            "full_name": name_part or title,
            "first_name": first,
            "last_name": last,
            "circumstances": summary,
            "raw_data": safe_json(dict(entry)),
        }

        return self.upsert(
            MissingPerson,
            lookup_kwargs={"source": "ncmec", "source_id": case_id},
            update_kwargs=update_data,
        )

    def _scrape_html(self) -> tuple:
        """
        Scrape the public NCMEC search results page.
        Returns (found, new, updated, errors).
        """
        found = new = updated = errors = 0

        params = {
            "missType": "child",
            "action": "publicSearchChild",
            "rstatus": "1",
        }

        try:
            resp = polite_get(self.http, NCMEC_SEARCH_URL, params=params, delay=2.0)
            soup = BeautifulSoup(resp.text, "lxml")

            # Each result card typically has class containing 'result' or 'poster'
            cards = (
                soup.select(".missing-child-result") or
                soup.select(".poster-result") or
                soup.select("[class*='result']") or
                soup.select("article")
            )

            self.logger.info("HTML scrape: found %d cards", len(cards))

            for card in cards:
                try:
                    name_el = card.select_one("h2, h3, .name, [class*='name']")
                    link_el = card.select_one("a[href]")
                    name = clean_text(name_el.get_text()) if name_el else None
                    url = link_el["href"] if link_el else None
                    if url and not url.startswith("http"):
                        url = "https://www.missingkids.org" + url

                    if not name:
                        continue

                    case_id = re.sub(r'[^a-zA-Z0-9]', '_', name)[:64]
                    parts = name.split()
                    first = parts[0] if parts else ""
                    last = parts[-1] if len(parts) > 1 else ""

                    _, created = self.upsert(
                        MissingPerson,
                        lookup_kwargs={"source": "ncmec_html", "source_id": case_id},
                        update_kwargs={
                            "full_name": name,
                            "first_name": first,
                            "last_name": last,
                            "source_url": url,
                            "raw_data": safe_json({"html_name": name, "url": url}),
                        },
                    )
                    found += 1
                    if created: new += 1
                    else: updated += 1

                except Exception as e:
                    self.logger.error("Card parse error: %s", e)
                    errors += 1

        except Exception as e:
            self.logger.warning("HTML search page error: %s", e)
            errors += 1

        return found, new, updated, errors
