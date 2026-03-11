<div align="center">

# 🌍 MCID — Missing Children Intelligence Dashboard

**A full-stack OSINT platform for aggregating, analysing, and visualising global missing children data.**  
Built for researchers and law enforcement professionals.

[![Python](https://img.shields.io/badge/Python-3.12-blue?style=flat-square&logo=python)](https://python.org)
[![License](https://img.shields.io/badge/License-Research%20Use-red?style=flat-square)](LICENSE)
[![Sources](https://img.shields.io/badge/Sources-6%20databases-orange?style=flat-square)](#data-sources)
[![Countries](https://img.shields.io/badge/News%20Coverage-60%2B%20countries-green?style=flat-square)](#news-coverage)

</div>

---

## What is MCID?

MCID is an automated intelligence platform that scrapes, aggregates, and analyses data from official missing children databases worldwide. It detects patterns consistent with organised trafficking activity, generates LE-grade PDF briefs, and presents everything in a self-contained interactive HTML dashboard.

**This tool is intended for use by researchers, investigators, and law enforcement professionals.**

---

## Features

| Module | Description |
|--------|-------------|
| 🕷️ **Scrapers** | NCMEC, NamUs, Interpol Yellow Notices, GMCN, Missing People UK, Global News (120+ feeds) |
| 🔍 **Pattern Analysis** | Surname clustering, spatio-temporal burst detection, corridor mapping, demographic targeting, timeline anomalies |
| 🌐 **OSINT Enrichment** | CourtListener, DOJ press releases, Europol newsroom, FBI wanted, Google News per-case search, OpenSanctions |
| 🕸️ **Network Graph** | D3 force-directed graph linking children → clusters → locations → court cases → corridors |
| 🚨 **Alert Monitor** | Real-time spike detection, burst zone monitoring, Telegram/email notifications |
| 📄 **Document Intelligence** | PDF ingestion with spaCy NER, fuzzy DB matching for court docs and NGO reports |
| 📊 **Dashboard** | Self-contained HTML with map, charts, case explorer, patterns, corridors, network graph, LE reports |
| 🗂️ **LE Export** | PDF briefs per cluster, CSV/XML/JSON-LD exports (I/BASE and Analyst's Notebook compatible) |

---

## Pipeline

```
python main.py pipeline
```

Runs the full sequence automatically:

```
Scrape all sources
    → Auto-cleanup adults
    → Pattern analysis
    → OSINT enrichment
    → Network graph
    → Alert monitor
    → HTML dashboard
    → CSV export
```

---

## Screenshots

### Full Pipeline Running

The pipeline ingests from all sources in parallel. Here, NamUs returns 10,000 records, Interpol fetches 2,400 child notices across 15 pages, GMCN processes 5,746 cases, and Missing People UK fetches 75 children concurrently via async.

![Pipeline — NamUs + Interpol scraping](screenshots/1773189654820_Screenshot_from_2026-03-11_01-40-21.png)

---

![Pipeline — Interpol done, GMCN running](1773189654820_Screenshot_from_2026-03-11_01-40-39.png)

---

![Pipeline — GMCN done, Missing People UK async](1773189654821_Screenshot_from_2026-03-11_01-40-49.png)

---

### News Cross-Reference & Auto-Resolution

The news scraper runs 120+ feeds across 60+ countries. It cross-references article names against the DB in real time. When a resolution article is detected (`"Amber Alert canceled"`, `"found safe"`, etc.), the DB record is automatically marked as resolved.

![News cross-reference and auto-resolution](1773189654821_Screenshot_from_2026-03-11_01-40-59.png)

---

### Pattern Analysis & OSINT Enrichment

After scraping, pattern analysis runs on 13,930 cases — finding 2,417 surname clusters (1,493 sibling units, 523 family groups, 401 cross-border), 239 spatio-temporal bursts (47 active), and 20 flagged countries. OSINT enrichment then queries CourtListener, DOJ, Europol, FBI, and Google News per case.

![Pattern analysis results and OSINT enrichment starting](1773189654821_Screenshot_from_2026-03-11_01-41-16.png)

---

### Interpol — New Records Logging

Interpol Yellow Notices are fetched via the public `ws-public.interpol.int` API with `ageMax=17` server-side filtering. Each new child record is logged with age, nationality, and source.

![Interpol new records with nationality](1773189654822_Screenshot_from_2026-03-11_01-56-46.png)

---

## Dashboard

### Overview — KPIs & World Map

13,933 total cases. 47 active burst clusters. 1 active trafficking corridor. 20 countries with demographic flags. The map supports three modes: **Cases Count**, **Active Bursts**, and **Corridors**.

![Dashboard overview — KPIs and world map](1773189654821_Screenshot_from_2026-03-11_01-43-02.png)

---

### Overview — Analytics Charts

Cases by source, gender breakdown, age at disappearance, cases per year trend, missing duration distribution, and top countries — all rendered with Chart.js.

![Dashboard analytics charts](1773189654821_Screenshot_from_2026-03-11_01-43-10.png)

---

### Case Explorer

Filterable table with photo thumbnails, name, age, gender, city, country, date missing, and source. Filters by country, source, and age range. Paginated at 25 per page with direct links to original records.

![Case explorer with photos and filters](1773189654821_Screenshot_from_2026-03-11_01-43-29.png)

---

### Pattern Analysis — Surname Clusters

2,417 clusters detected. Each card shows surname, countries, member count, age range, and date span. Click any card to expand and see individual children with photos and source links. Filterable by type (Sibling Unit / Family Group / Cross-Border).

![Surname clusters — patterns tab](1773189654822_Screenshot_from_2026-03-11_01-44-12.png)

---

### Pattern Analysis — Timeline Anomalies & Demographic Targeting

Statistical spike detection (z-score) per country per month highlights organised activity. Demographic targeting flags countries with abnormal age/gender distributions — Guatemala shows `PREDOMINANTLY_FEMALE + HIGH_PROPORTION_YOUNG_CHILDREN + VERY_YOUNG_MEAN_AGE (8.8)`, consistent with documented trafficking profiles.

![Timeline anomalies](1773189654822_Screenshot_from_2026-03-11_01-44-19.png)  
![Demographic targeting by country](1773189654822_Screenshot_from_2026-03-11_02-11-40.png)

---

### Trafficking Corridors

Corridor matches computed from Interpol nationality data vs country of disappearance, scored against UNODC/IOM/Europol documented trafficking routes. Flow matrix shows raw origin → destination pairs.

![Trafficking corridors](1773189654822_Screenshot_from_2026-03-11_01-44-12.png)

---

### System — Scraper Status

Per-source breakdown with total, active, resolved, and photo counts. Scraper status cards show last run time, status, records found, new records, and errors.

![System tab — scraper status](1773189654822_Screenshot_from_2026-03-11_01-44-19.png)

---

### Network Graph — All Nodes

The embedded D3 force-directed network graph links 4,891 nodes and 3,844 edges. Nodes represent children, family clusters, burst locations, court cases, DOJ findings, and trafficking corridors. Filterable by type, searchable, zoomable, draggable.

![Network graph — all nodes](1773189654823_Screenshot_from_2026-03-11_02-35-27.png)

---

### Network Graph — Burst Filter

Filtering to **Bursts** shows only spatio-temporal burst locations and the children connected to them. Here, Stronie Śląskie, Poland is shown as an active burst node with hovering over a child node showing age, country, date missing, and a direct source link.

![Network graph — burst filter with tooltip](1773189654823_Screenshot_from_2026-03-11_02-36-02.png)

---

### Reports Tab — LE PDF Briefs

PDF briefs generated per cluster are embedded as base64 in the dashboard for direct download without a server. Each brief contains: case summary, member table, analytical assessment, recommended investigative actions, and source links.

![Reports tab — PDF briefs](1773189654823_Screenshot_from_2026-03-11_02-37-46.png)

---

## Installation

```bash
git clone https://github.com/YOUR_USERNAME/missing-kids-scraper
cd missing-kids-scraper

pip install -r requirements.txt
pip install spacy pdfplumber reportlab Pillow rapidfuzz --break-system-packages
python -m spacy download en_core_web_sm
```

---

## Usage

```bash
# Full pipeline (recommended first run)
python main.py pipeline

# Individual commands
python main.py status                          # DB overview + module check
python main.py run all                         # Scrape all sources
python main.py run interpol gmcn               # Specific scrapers
python main.py patterns                        # Pattern analysis
python main.py enrich --limit 500             # OSINT enrichment
python main.py network                         # Build network graph
python main.py monitor --watch                 # Alert daemon
python main.py docs /path/to/report.pdf       # PDF document intelligence
python main.py report                          # Regenerate dashboard
python main.py export --format all            # CSV/XML/JSON export
python main.py le-report --all                # Generate all LE PDF briefs
python main.py le-report --cluster GARCIA     # Specific cluster brief
python main.py cleanup                         # Remove adult records
```

---

## Data Sources

| Source | Records | Coverage | Age Filter |
|--------|---------|----------|------------|
| **GMCN** (Global Missing Children's Network) | ~5,750 | 22+ countries | Post-fetch |
| **NamUs** (National Missing and Unidentified Persons) | ~10,000 | United States | API `ageRanges 0-17` |
| **Interpol Yellow Notices** | ~2,400 | 196 countries | API `ageMax=17` |
| **Missing People UK** | ~75 | United Kingdom | XHR `age=child` |
| **NCMEC** | Pending API | United States | API `ageTo=17` |
| **News** | 120+ feeds | 60+ countries | Keyword filter |

---

## News Coverage

120+ RSS feeds across 60+ countries and 20+ languages including English, Spanish, Portuguese, French, German, Italian, Dutch, Swedish, Norwegian, Danish, Finnish, Polish, Romanian, Ukrainian, Russian, Bulgarian, Czech, Hungarian, Serbian, Croatian, Turkish, Arabic, Hebrew, Hindi, Tagalog, Indonesian, Malay, Thai, Vietnamese, Japanese, Korean, Chinese, Swahili, and more.

---

## Pattern Analysis

Five analytical engines run automatically after every scrape:

1. **Surname Clusters** — same last name + country + date proximity → sibling units, family groups, cross-border clusters
2. **Spatio-Temporal Bursts** — 3+ children disappearing from the same city within 30 days → organised operation signature
3. **Corridor Detection** — nationality vs country of disappearance scored against UNODC/IOM/Europol documented trafficking routes
4. **Demographic Targeting** — abnormal age/gender concentration per country flagged with `PREDOMINANTLY_FEMALE`, `HIGH_PROPORTION_YOUNG_CHILDREN`, `VERY_YOUNG_MEAN_AGE`
5. **Timeline Anomalies** — z-score spike detection per country per month

---

## OSINT Enrichment Sources

| Source | Type | Rate Limit |
|--------|------|-----------|
| **CourtListener** | Federal court cases (18 USC 1591/1594) | 50 req/min, free |
| **DOJ Press Releases** | Trafficking arrests/convictions | RSS, no limit |
| **Europol Newsroom** | Operations, victim counts | RSS, no limit |
| **FBI Wanted API** | Kidnappings/missing persons | Public JSON API |
| **Google News** | Per-case targeted search | RSS, free |
| **OpenSanctions** | Sanctioned traffickers/networks | Free tier |

---

## LE Export Formats

- **PDF Briefs** — per cluster, includes: case summary, member table with photos, analytical assessment, recommended investigative actions, source links
- **CSV** — all active cases with cluster tags and enrichment findings (I/BASE compatible)
- **XML** — i2 Analyst's Notebook compatible
- **JSON-LD** — linked data format with schema.org vocabulary

---

## Alert System

The monitor checks automatically after every scrape run and can run as a daemon:

```bash
python main.py monitor --watch --interval 30
```

Alert types:
- `NEW_BURST_CASE` — new case added to an active burst zone
- `FLAGGED_SURNAME_MATCH` — new case matches a known high-priority cluster
- `STATISTICAL_SPIKE` — z-score crossing threshold in real time
- `HIGH_VALUE_FINDING` — court/DOJ/FBI match with relevance ≥ 0.6

Notifications via **Telegram** and/or **email** (configure via environment variables).

```bash
export TELEGRAM_BOT_TOKEN="your_token"
export TELEGRAM_CHAT_ID="your_chat_id"
export SMTP_USER="you@gmail.com"
export SMTP_PASS="app_password"
export ALERT_EMAIL="recipient@domain.com"
```

---

## Project Structure

```
missing-kids-scraper/
├── main.py                    # Single entry point — all commands
├── report.py                  # HTML dashboard generator
├── scrapers/
│   ├── base.py
│   ├── ncmec.py
│   ├── namus.py
│   ├── interpol.py
│   ├── gmcn.py
│   ├── missing_people_uk.py
│   └── news.py
├── analysis/
│   ├── patterns.py            # Pattern analysis engine
│   ├── enrichment.py          # OSINT enrichment pipeline
│   ├── network.py             # Network graph builder
│   ├── documents.py           # PDF document intelligence
│   └── output/                # Generated reports (JSON, TXT)
├── alerts/
│   └── monitor.py             # Alert monitor daemon
├── export/
│   ├── le_report.py           # LE PDF brief generator
│   ├── ibase.py               # I/BASE compatible export
│   └── output/                # Generated exports
├── database/
│   └── models.py              # SQLAlchemy models
├── utils/
│   └── helpers.py
└── output/
    └── dashboard.html         # Self-contained HTML dashboard
```

---

## Requirements

```
python >= 3.12
feedparser
requests
httpx
beautifulsoup4
sqlalchemy
schedule
rich
tenacity
rapidfuzz
reportlab
Pillow
pdfplumber
spacy (+ en_core_web_sm)
playwright (optional, for JS-rendered sites)
```

---

## Notes

- All data sourced from **publicly available official databases only**
- Adult records (age ≥ 18) are automatically purged after every scrape run
- No dark web sources are used
- Dashboard is a fully self-contained static HTML file — no server required
- PDF reports are embedded as base64 in the dashboard for direct download

---

## Disclaimer

This tool is built for **research and law enforcement use only**. All data displayed is sourced from public official databases. The platform does not access, store, or transmit any non-public data. Users are responsible for ensuring compliance with applicable laws and regulations in their jurisdiction.

---

<div align="center">
<sub>Built for the protection of missing children worldwide.</sub>
</div>
