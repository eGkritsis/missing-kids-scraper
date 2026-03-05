"""
scrapers/social_media.py

Social Media Integration for Missing Children Tracker
=====================================================

IMPORTANT NOTE ON SOCIAL MEDIA SCRAPING
----------------------------------------
Direct HTML scraping of social media platforms (Facebook, Twitter/X, Instagram,
TikTok) is explicitly prohibited by their Terms of Service and in some
jurisdictions is legally risky under the Computer Fraud and Abuse Act (CFAA).

Furthermore, for a child safety use case, unofficial scrapers would:
  1. Produce unreliable / unverified data (hoaxes spread fast on social media)
  2. Risk exposing sensitive location data of at-risk children
  3. Create legal liability for the nonprofit organization

RECOMMENDED APPROACH: Official APIs
------------------------------------
The platforms below offer official API programs for nonprofits and safety
organizations. These are the right path for a legitimate volunteer project.

Twitter / X
  - Product:   X API v2 (Free tier allows read access)
  - Apply at:  https://developer.twitter.com/en/portal/dashboard
  - Relevant:  Search for #AmberAlert, #MissingChild, accounts like @MissingKids
  - Tier:      Basic (free) or Pro for higher volume
  - Note:      X offers Data for Good program for nonprofits

Meta (Facebook & Instagram)
  - Product:   Meta Content Library API
  - Apply at:  https://developers.facebook.com/programs/content-library-api/
  - Note:      Requires nonprofit verification; access is granted per-project
  - Relevant:  Public Facebook pages of missing children orgs, NCMEC page

TikTok
  - Product:   TikTok Research API
  - Apply at:  https://developers.tiktok.com/products/research-api/
  - Note:      Academic/nonprofit use supported

Once you have API credentials, drop them in config.py and implement the
client classes below.
"""

import os
from scrapers.base import BaseScraper
from utils.helpers import clean_text


# ============================================================
#  Twitter / X  (requires TWITTER_BEARER_TOKEN in env)
# ============================================================

TWITTER_SEARCH_URL = "https://api.twitter.com/2/tweets/search/recent"
TWITTER_QUERIES = [
    "#AmberAlert",
    "#MissingChild",
    "missing child -is:retweet lang:en",
    "from:MissingKids",
]


class TwitterScraper(BaseScraper):
    """
    Searches Twitter/X for recent missing children posts using the v2 API.
    Requires: TWITTER_BEARER_TOKEN environment variable.
    """
    name = "twitter"

    def run(self) -> dict:
        token = os.environ.get("TWITTER_BEARER_TOKEN")
        if not token:
            self.logger.warning(
                "TWITTER_BEARER_TOKEN not set. "
                "Get one at https://developer.twitter.com and set it in your .env file. "
                "Skipping Twitter scraper."
            )
            return {"found": 0, "new": 0, "updated": 0, "errors": 0}

        headers = {"Authorization": f"Bearer {token}"}
        found = new = errors = 0

        for query in TWITTER_QUERIES:
            try:
                params = {
                    "query": query,
                    "max_results": 100,
                    "tweet.fields": "created_at,author_id,text,geo",
                    "expansions": "author_id",
                }
                from utils.helpers import polite_get
                resp = polite_get(
                    self.http, TWITTER_SEARCH_URL,
                    params=params, delay=2.0,
                    headers=headers,
                )
                tweets = resp.json().get("data", [])
                found += len(tweets)
                self.logger.info("Twitter query '%s': %d tweets", query, len(tweets))

                for tweet in tweets:
                    try:
                        self._process_tweet(tweet)
                        new += 1
                    except Exception as exc:
                        self.logger.error("Tweet processing failed: %s", exc)
                        errors += 1

            except Exception as exc:
                self.logger.error("Twitter query '%s' failed: %s", query, exc)
                errors += 1

        return {"found": found, "new": new, "updated": 0, "errors": errors}

    def _process_tweet(self, tweet: dict):
        """
        Extract names from tweet text and cross-reference against DB.
        Tweets are not stored verbatim (respects Twitter ToS on data storage).
        We only log potential matches for investigator follow-up.
        """
        from utils.helpers import extract_names_from_text
        from database.models import MissingPerson

        text = tweet.get("text", "")
        names = extract_names_from_text(text)

        for full_name in names:
            parts = full_name.split()
            if len(parts) < 2:
                continue
            first, last = parts[0], parts[-1]
            match = self.db.query(MissingPerson).filter(
                MissingPerson.first_name.ilike(first),
                MissingPerson.last_name.ilike(last),
                MissingPerson.is_resolved == False,
            ).first()
            if match:
                self.logger.warning(
                    "🐦 TWITTER MATCH: '%s' in tweet matches DB record %s/%s",
                    full_name, match.source, match.source_id,
                )
                self.logger.warning("Tweet: %s", text[:200])


# ============================================================
#  Placeholder for Meta / TikTok
#  (implement once API access is approved)
# ============================================================

class MetaScraper(BaseScraper):
    name = "meta"

    def run(self) -> dict:
        self.logger.warning(
            "Meta Content Library API not yet configured. "
            "Apply at https://developers.facebook.com/programs/content-library-api/"
        )
        return {"found": 0, "new": 0, "updated": 0, "errors": 0}
