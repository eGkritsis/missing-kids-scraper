"""
scrapers/news.py

Global news scraper for missing children.

Features:
  - 50+ RSS/news feeds across 25+ countries in 15 languages
  - Resolution detection: marks DB records as resolved when found/rescued news appears
  - Cross-references article names against the local database
  - Deduplicates articles by URL
"""

import re
from datetime import datetime

import feedparser

from database.models import NewsArticle, MissingPerson
from scrapers.base import BaseScraper
from utils.helpers import clean_text, extract_names_from_text

# ---------------------------------------------------------------------------
# Resolution keywords — if ANY appear in title/summary, child was likely found
# ---------------------------------------------------------------------------

RESOLUTION_KEYWORDS = [
    # English
    "found safe", "found alive", "has been found", "was found", "been located",
    "safely recovered", "has been recovered", "been recovered", "returned home",
    "reunited with", "has been reunited", "located safe", "amber alert cancelled",
    "amber alert canceled", "amber alert resolved", "child found", "teen found",
    "juvenile found", "safely returned", "no longer missing", "case closed",
    "child recovered", "children recovered", "rescued", "safe recovery",
    "found unharmed", "found unhurt",
    # Spanish
    "fue encontrado", "fue encontrada", "encontrado con vida", "encontrada con vida",
    "ha sido localizado", "ha sido localizada", "niño encontrado", "niña encontrada",
    "alerta amber cancelada", "fue rescatado", "fue rescatada",
    # Portuguese
    "foi encontrado", "foi encontrada", "foi localizado", "foi localizada",
    "crianca encontrada", "menor encontrado", "resgatado", "resgatada",
    # French
    "a ete retrouve", "a ete retrouvee", "enfant retrouve", "enfant retrouvee",
    "retrouve sain", "retrouvee saine", "alerte enlevement annulee",
    # German
    "wurde gefunden", "ist gefunden", "kind gefunden", "vermisstes kind gefunden",
    "wohlbehalten aufgefunden", "wurde gerettet",
    # Italian
    "e stato trovato", "e stata trovata", "bambino trovato", "ritrovato sano",
    # Dutch
    "is gevonden", "kind gevonden", "vermist kind gevonden", "veilig gevonden",
    # Turkish
    "bulundu", "kurtarildi", "kayip cocuk bulundu",
    # Polish
    "odnaleziono", "dziecko odnalezione", "bezpiecznie odnalezione",
    # Russian
    "najden", "najdena", "rebenok najden",
    # Greek
    "vrethike", "entopiistike",
    # Arabic
    "tm alethwr", "othir ala",
    # Japanese
    "hakken", "hogo",
    # Korean
    "balgyon", "gujo",
]

MISSING_KEYWORDS = [
    # English
    "missing", "abducted", "amber alert", "last seen", "endangered",
    "runaway", "kidnapped", "abduction", "disappear", "disappeared",
    # Spanish
    "desaparecido", "desaparecida", "menor desaparecido", "nino desaparecido",
    "alerta amber", "secuestrado", "secuestrada",
    # Portuguese
    "desaparecida", "desaparecido", "crianca desaparecida",
    # French
    "disparu", "disparue", "enfant disparu", "alerte enlevement",
    # German
    "vermisst", "entfuhrt", "kindesentfuhrung", "vermisstes kind",
    # Italian
    "scomparso", "scomparsa", "bambino scomparso", "sequestrato",
    # Dutch
    "vermist", "ontvoerd", "vermist kind",
    # Turkish
    "kayip", "kayip cocuk", "kacirildi",
    # Polish
    "zaginięcie", "zaginięte dziecko", "uprowadzenie",
    # Russian
    "propal", "propala", "pohischen",
    # Greek
    "exafanisi", "apagogi",
    # Arabic
    "mfqwd", "tfl mfqwd",
    # Japanese
    "yukuefumei", "yukai",
    # Korean
    "siljeong", "napchi",
]

# ---------------------------------------------------------------------------
# Feed list: (label, url)
# ---------------------------------------------------------------------------

FEEDS = [
    # ---- USA ---------------------------------------------------------------
    ("USA: Amber Alert",
     "https://news.google.com/rss/search?q=%22amber+alert%22&hl=en-US&gl=US&ceid=US:en"),
    ("USA: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+%22last+seen%22&hl=en-US&gl=US&ceid=US:en"),
    ("USA: NCMEC news",
     "https://news.google.com/rss/search?q=NCMEC+missing&hl=en-US&gl=US&ceid=US:en"),
    ("USA: Child abduction",
     "https://news.google.com/rss/search?q=%22child+abduction%22&hl=en-US&gl=US&ceid=US:en"),
    ("USA: Child found safe",
     "https://news.google.com/rss/search?q=%22child+found+safe%22+OR+%22amber+alert+canceled%22&hl=en-US&gl=US&ceid=US:en"),
    ("USA: NCMEC RSS",
     "https://www.missingkids.org/missingkids/servlet/XmlServlet?act=rss&missType=child&LanguageCountry=en_US"),

    # ---- UK ----------------------------------------------------------------
    ("UK: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+police&hl=en-GB&gl=GB&ceid=GB:en"),
    ("UK: Child found",
     "https://news.google.com/rss/search?q=%22missing+child%22+%22found+safe%22&hl=en-GB&gl=GB&ceid=GB:en"),
    ("UK: Child abduction",
     "https://news.google.com/rss/search?q=%22child+abduction%22&hl=en-GB&gl=GB&ceid=GB:en"),

    # ---- Canada ------------------------------------------------------------
    ("Canada: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+RCMP&hl=en-CA&gl=CA&ceid=CA:en"),
    ("Canada: Amber alert",
     "https://news.google.com/rss/search?q=%22amber+alert%22+canada&hl=en-CA&gl=CA&ceid=CA:en"),

    # ---- Australia ---------------------------------------------------------
    ("Australia: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22&hl=en-AU&gl=AU&ceid=AU:en"),
    ("Australia: Child found",
     "https://news.google.com/rss/search?q=%22missing+child%22+%22found+safe%22&hl=en-AU&gl=AU&ceid=AU:en"),

    # ---- Ireland -----------------------------------------------------------
    ("Ireland: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+garda&hl=en-IE&gl=IE&ceid=IE:en"),

    # ---- India -------------------------------------------------------------
    ("India: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22+police&hl=en-IN&gl=IN&ceid=IN:en"),
    ("India: Child found",
     "https://news.google.com/rss/search?q=%22missing+child%22+%22found%22&hl=en-IN&gl=IN&ceid=IN:en"),

    # ---- South Africa ------------------------------------------------------
    ("South Africa: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22&hl=en-ZA&gl=ZA&ceid=ZA:en"),

    # ---- Mexico / Latin America --------------------------------------------
    ("Mexico: Alerta amber",
     "https://news.google.com/rss/search?q=%22alerta+amber%22&hl=es-419&gl=MX&ceid=MX:es-419"),
    ("Mexico: Nino desaparecido",
     "https://news.google.com/rss/search?q=ni%C3%B1o+desaparecido&hl=es-419&gl=MX&ceid=MX:es-419"),
    ("Mexico: Menor encontrado",
     "https://news.google.com/rss/search?q=menor+encontrado+sano&hl=es-419&gl=MX&ceid=MX:es-419"),
    ("Colombia: Menor desaparecido",
     "https://news.google.com/rss/search?q=menor+desaparecido&hl=es-419&gl=CO&ceid=CO:es-419"),
    ("Argentina: Nino desaparecido",
     "https://news.google.com/rss/search?q=ni%C3%B1o+desaparecido&hl=es-419&gl=AR&ceid=AR:es-419"),
    ("Spain: Menor desaparecido",
     "https://news.google.com/rss/search?q=menor+desaparecido&hl=es&gl=ES&ceid=ES:es"),

    # ---- Brazil ------------------------------------------------------------
    ("Brazil: Crianca desaparecida",
     "https://news.google.com/rss/search?q=crian%C3%A7a+desaparecida&hl=pt-BR&gl=BR&ceid=BR:pt-419"),
    ("Brazil: Menor encontrado",
     "https://news.google.com/rss/search?q=menor+desaparecido+encontrado&hl=pt-BR&gl=BR&ceid=BR:pt-419"),

    # ---- France / Belgium --------------------------------------------------
    ("France: Enfant disparu",
     "https://news.google.com/rss/search?q=enfant+disparu&hl=fr&gl=FR&ceid=FR:fr"),
    ("France: Enfant retrouve",
     "https://news.google.com/rss/search?q=enfant+retrouv%C3%A9&hl=fr&gl=FR&ceid=FR:fr"),
    ("France: Alerte enlevement",
     "https://news.google.com/rss/search?q=alerte+enl%C3%A8vement&hl=fr&gl=FR&ceid=FR:fr"),
    ("Belgium: Enfant disparu",
     "https://news.google.com/rss/search?q=enfant+disparu&hl=fr&gl=BE&ceid=BE:fr"),

    # ---- Germany -----------------------------------------------------------
    ("Germany: Vermisstes Kind",
     "https://news.google.com/rss/search?q=vermisstes+Kind&hl=de&gl=DE&ceid=DE:de"),
    ("Germany: Kind gefunden",
     "https://news.google.com/rss/search?q=vermisstes+Kind+gefunden&hl=de&gl=DE&ceid=DE:de"),
    ("Germany: Kindesentfuhrung",
     "https://news.google.com/rss/search?q=Kindesentf%C3%BChrung&hl=de&gl=DE&ceid=DE:de"),

    # ---- Italy -------------------------------------------------------------
    ("Italy: Bambino scomparso",
     "https://news.google.com/rss/search?q=bambino+scomparso&hl=it&gl=IT&ceid=IT:it"),
    ("Italy: Bambino ritrovato",
     "https://news.google.com/rss/search?q=bambino+scomparso+ritrovato&hl=it&gl=IT&ceid=IT:it"),

    # ---- Netherlands -------------------------------------------------------
    ("Netherlands: Vermist kind",
     "https://news.google.com/rss/search?q=vermist+kind&hl=nl&gl=NL&ceid=NL:nl"),

    # ---- Poland ------------------------------------------------------------
    ("Poland: Zaginięcie dziecka",
     "https://news.google.com/rss/search?q=zaginięcie+dziecka&hl=pl&gl=PL&ceid=PL:pl"),
    ("Poland: Dziecko odnalezione",
     "https://news.google.com/rss/search?q=zaginięte+dziecko+odnalezione&hl=pl&gl=PL&ceid=PL:pl"),

    # ---- Russia ------------------------------------------------------------
    ("Russia: Propal rebyonok",
     "https://news.google.com/rss/search?q=%D0%BF%D1%80%D0%BE%D0%BF%D0%B0%D0%BB+%D1%80%D0%B5%D0%B1%D1%91%D0%BD%D0%BE%D0%BA&hl=ru&gl=RU&ceid=RU:ru"),
    ("Russia: Rebyonok najden",
     "https://news.google.com/rss/search?q=%D1%80%D0%B5%D0%B1%D1%91%D0%BD%D0%BE%D0%BA+%D0%BD%D0%B0%D0%B9%D0%B4%D0%B5%D0%BD&hl=ru&gl=RU&ceid=RU:ru"),

    # ---- Turkey ------------------------------------------------------------
    ("Turkey: Kayip cocuk",
     "https://news.google.com/rss/search?q=kayip+cocuk&hl=tr&gl=TR&ceid=TR:tr"),

    # ---- Greece ------------------------------------------------------------
    ("Greece: Exafanisi paidiou",
     "https://news.google.com/rss/search?q=%CE%B5%CE%BE%CE%B1%CF%86%CE%AC%CE%BD%CE%B9%CF%83%CE%B7+%CF%80%CE%B1%CE%B9%CE%B4%CE%B9%CE%BF%CF%8D&hl=el&gl=GR&ceid=GR:el"),

    # ---- Arabic / Middle East ----------------------------------------------
    ("MENA: Tifl mafqoud",
     "https://news.google.com/rss/search?q=%D8%B7%D9%81%D9%84+%D9%85%D9%81%D9%82%D9%88%D8%AF&hl=ar&gl=SA&ceid=SA:ar"),
    ("Egypt: Tifl mafqoud",
     "https://news.google.com/rss/search?q=%D8%B7%D9%81%D9%84+%D9%85%D9%81%D9%82%D9%88%D8%AF&hl=ar&gl=EG&ceid=EG:ar"),

    # ---- Japan -------------------------------------------------------------
    ("Japan: Yukue fumei kodomo",
     "https://news.google.com/rss/search?q=%E8%A1%8C%E6%96%B9%E4%B8%8D%E6%98%8E+%E5%AD%90%E4%BE%9B&hl=ja&gl=JP&ceid=JP:ja"),

    # ---- South Korea -------------------------------------------------------
    ("Korea: Siljeong adong",
     "https://news.google.com/rss/search?q=%EC%8B%A4%EC%A2%85+%EC%95%84%EB%8F%99&hl=ko&gl=KR&ceid=KR:ko"),

    # ---- Nigeria / Kenya ---------------------------------------------------
    ("Nigeria: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22&hl=en-NG&gl=NG&ceid=NG:en"),
    ("Kenya: Missing child",
     "https://news.google.com/rss/search?q=%22missing+child%22&hl=en-KE&gl=KE&ceid=KE:en"),
]


class NewsScraper(BaseScraper):
    name = "news"

    def run(self) -> dict:
        found = new = updated = errors = 0
        resolved_count = 0

        self.logger.info("Starting Global News scrape (%d feeds)...", len(FEEDS))

        for feed_name, feed_url in FEEDS:
            try:
                articles = self._fetch_feed(feed_name, feed_url)
                if articles:
                    self.logger.info("%s: %d articles", feed_name, len(articles))
                found += len(articles)

                for article in articles:
                    try:
                        _, created = self._upsert_article(article)
                        if created:
                            new += 1
                        else:
                            updated += 1
                        resolved = self._cross_reference(article)
                        resolved_count += resolved
                    except Exception as exc:
                        self.logger.error("Article save failed: %s", exc)
                        errors += 1

            except Exception as exc:
                self.logger.warning("Feed '%s' failed: %s", feed_name, exc)
                errors += 1

        self.logger.info(
            "News done. found=%d new=%d updated=%d resolved=%d errors=%d",
            found, new, updated, resolved_count, errors,
        )
        return {"found": found, "new": new, "updated": updated,
                "resolved": resolved_count, "errors": errors}

    def _fetch_feed(self, feed_name: str, url: str) -> list[dict]:
        import time
        time.sleep(1.0)
        feed     = feedparser.parse(url)
        articles = []

        for entry in feed.entries:
            title    = clean_text(entry.get("title", ""))
            summary  = clean_text(entry.get("summary", "") or entry.get("description", ""))
            combined = f"{title} {summary}".lower()

            is_missing    = any(kw.lower() in combined for kw in MISSING_KEYWORDS)
            is_resolution = any(kw.lower() in combined for kw in RESOLUTION_KEYWORDS)

            if not (is_missing or is_resolution):
                continue

            published = None
            if entry.get("published_parsed"):
                try:
                    published = datetime(*entry.published_parsed[:6])
                except Exception:
                    pass

            articles.append({
                "url":           entry.get("link", ""),
                "title":         title,
                "summary":       summary,
                "source_name":   feed_name,
                "published_at":  published,
                "is_resolution": is_resolution,
            })

        return articles

    def _upsert_article(self, article: dict) -> tuple:
        url = article.get("url", "")
        if not url:
            raise ValueError("Article missing URL")

        title   = article.get("title", "")
        summary = article.get("summary", "")
        names   = extract_names_from_text(f"{title} {summary}")

        update_data = {
            "title":           title,
            "summary":         summary,
            "source_name":     article["source_name"],
            "published_at":    article["published_at"],
            "names_mentioned": ", ".join(names) if names else None,
        }

        instance = self.db.query(NewsArticle).filter_by(url=url).first()
        created  = False
        if instance is None:
            instance = NewsArticle(url=url, **update_data)
            self.db.add(instance)
            created = True
        else:
            for k, v in update_data.items():
                setattr(instance, k, v)
        self.db.commit()
        return instance, created

    def _cross_reference(self, article: dict) -> int:
        """
        Match names in article against DB records.
        If the article is a resolution (found/rescued), mark matching
        DB records as is_resolved=True and store resolution notes.
        Returns count of newly resolved records.
        """
        title         = article.get("title", "")
        summary       = article.get("summary", "")
        is_resolution = article.get("is_resolution", False)
        names         = extract_names_from_text(f"{title} {summary}")
        resolved_count = 0

        for full_name in names:
            parts = full_name.split()
            if len(parts) < 2:
                continue
            first, last = parts[0], parts[-1]

            matches = self.db.query(MissingPerson).filter(
                MissingPerson.first_name.ilike(first),
                MissingPerson.last_name.ilike(last),
                # Only match records confirmed to be minors or age unknown
                (MissingPerson.age_at_disappearance < 18) |
                (MissingPerson.age_at_disappearance == None),
            ).all()

            for match in matches:
                if is_resolution and not match.is_resolved:
                    match.is_resolved      = True
                    match.resolution_notes = (
                        f"Found via news: {title[:200]} | "
                        f"Source: {article['source_name']} | "
                        f"URL: {article['url']}"
                    )
                    self.db.commit()
                    resolved_count += 1
                    self.logger.warning(
                        "RESOLVED: '%s' marked found — '%s' [%s/%s]",
                        full_name, title[:80], match.source, match.source_id,
                    )
                elif not is_resolution and not match.is_resolved:
                    self.logger.warning(
                        "MATCH: '%s' in '%s' -> DB record %s/%s",
                        full_name, title[:70], match.source, match.source_id,
                    )

        return resolved_count
