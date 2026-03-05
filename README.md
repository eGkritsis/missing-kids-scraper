# Missing Children Tracker 🔍

A Python-based scraper and database for aggregating **verified, public missing children data** from official sources. Built for nonprofit/volunteer use.

---

## Data Sources

| Source | Type | Status | Notes |
|--------|------|--------|-------|
| [NCMEC](https://www.missingkids.org) | Official DB | ✅ Ready | National Center for Missing & Exploited Children — public JSON API |
| [NamUs](https://www.namus.gov) | Official DB | ✅ Ready | DOJ-funded national database — open REST API |
| Google News RSS | News feeds | ✅ Ready | No API key needed; filtered for relevance |
| Twitter / X | Social | ⚙️ Needs key | Requires free developer account — see setup |
| Facebook / Meta | Social | ⚙️ Needs approval | Requires Meta Content Library access |

---

## Project Structure

```
missing_children_tracker/
├── main.py                  # CLI entry point
├── requirements.txt
├── .env.example             # Config template
├── database/
│   └── models.py            # SQLAlchemy models (SQLite)
├── scrapers/
│   ├── base.py              # Abstract base class
│   ├── ncmec.py             # NCMEC scraper
│   ├── namus.py             # NamUs scraper
│   ├── news.py              # Google News RSS scraper
│   └── social_media.py      # Twitter + Meta (API-based)
└── utils/
    └── helpers.py           # HTTP client, date parsing, cleaning
```

---

## Setup

### 1. Install dependencies

```bash
cd missing_children_tracker
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env with your contact email and any API keys
```

### 3. Run

```bash
# Run all scrapers once
python main.py run

# Run a specific scraper
python main.py run ncmec
python main.py run namus
python main.py run news

# Run on a schedule (daemon mode)
# NCMEC/NamUs refresh every 12h, News/Social every 2h
python main.py schedule

# Print a summary report
python main.py report

# Export all active cases to CSV
python main.py export
python main.py export --out /path/to/output.csv
```

---

## Social Media Setup

### Twitter / X (Free tier available)

1. Go to [developer.twitter.com](https://developer.twitter.com/en/portal/dashboard)
2. Create a new App → get your **Bearer Token**
3. Add to `.env`: `TWITTER_BEARER_TOKEN=your_token_here`
4. Run: `python main.py run twitter`

The scraper searches `#AmberAlert`, `#MissingChild`, recent missing child posts, and the official `@MissingKids` account. It **does not store tweets verbatim** (per ToS) — only cross-references names against the database.

### Meta (Facebook/Instagram)

Meta requires nonprofit verification. Apply at the [Content Library API program](https://developers.facebook.com/programs/content-library-api/). This typically takes 2–4 weeks.

---

## Database Schema

All data is stored in `missing_children.db` (SQLite). Key tables:

- **`missing_persons`** — one row per case, with deduplication by `(source, source_id)`
- **`news_articles`** — news articles referencing missing children, with name cross-referencing
- **`scraper_runs`** — full audit log of every scraper execution

---

## Key Design Decisions

### Why not scrape "the entire web"?
- **Legal risk**: CFAA and ToS prohibit unauthorized scraping of most sites
- **Data quality**: Social media is full of hoaxes and unverified reports — bad data can harm real cases
- **Scope**: "Entire clearweb" requires Google-scale infrastructure
- **Official databases already aggregate**: NCMEC alone has 25,000+ active cases

### Why SQLite?
Simple, zero-config, and sufficient for hundreds of thousands of records. Easy to hand off to another volunteer or migrate to Postgres later.

### Rate limiting
Every HTTP request includes a 1.5–2 second delay. The user-agent string identifies the tool and includes a contact email. This is best practice for responsible scraping and is often required by robots.txt compliance.

---

## Legal & Ethical Notes

- All data sources scraped are **publicly accessible** and intended to be shared
- NCMEC and NamUs both publish their data specifically for distribution
- No authentication is bypassed
- Children's personal data is handled with care — only store what the official sources publish
- Review each source's robots.txt before scraping
- If you receive a cease-and-desist from any source, stop immediately

---

## Contributing

Pull requests welcome. Priority areas:
- Additional state-level missing children databases
- Interpol Yellow Notices scraper
- Age progression photo support
- Web UI (Flask/FastAPI dashboard)

---

## Disclaimer

This tool is for volunteer and nonprofit use only. Data accuracy depends on source databases. Always verify information through official channels before taking action. In an emergency, call **1-800-THE-LOST (1-800-843-5678)** (NCMEC hotline) or **911**.
