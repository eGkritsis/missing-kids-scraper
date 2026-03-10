"""
report.py
=========
Unified interactive HTML dashboard.
Integrates core DB stats (from original report.py) with
pattern analysis (patterns.py) into a single self-contained HTML file.

Usage:
  python report.py                      # uses missing_children.db
  python report.py --db custom.db
  python report.py --out output/my_dashboard.html
  python report.py --skip-patterns      # fast mode, skip pattern analysis
"""

import argparse
import json
import statistics
import sys
import os
from collections import defaultdict, Counter
from datetime import datetime, date, timedelta
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import func
from database.models import init_db, MissingPerson, NewsArticle, ScraperRun

DB_PATH = "missing_children.db"

COUNTRY_COORDS = {
    "United States":[37.09,-95.71],"USA":[37.09,-95.71],
    "Canada":[56.13,-106.35],"United Kingdom":[55.38,-3.44],
    "France":[46.23,2.21],"Germany":[51.17,10.45],"Spain":[40.46,-3.75],
    "Italy":[41.87,12.57],"Turkey":[38.96,35.24],"India":[20.59,78.96],
    "Pakistan":[30.38,69.35],"Nigeria":[9.08,8.68],"South Africa":[-30.56,22.94],
    "Australia":[-25.27,133.78],"Brazil":[-14.24,-51.93],"Mexico":[23.63,-102.55],
    "Argentina":[-38.42,-63.62],"Japan":[36.20,138.25],"China":[35.86,104.20],
    "Russia":[61.52,105.32],"Russian Federation":[61.52,105.32],
    "Jamaica":[18.11,-77.30],"Ecuador":[-1.83,-78.18],"Guatemala":[15.78,-90.23],
    "Honduras":[15.20,-86.24],"Colombia":[4.57,-74.30],"Peru":[-9.19,-75.02],
    "Bolivia":[-16.29,-63.59],"Chile":[-35.68,-71.54],"Venezuela":[6.42,-66.59],
    "Greece":[39.07,21.82],"Poland":[51.92,19.15],"Romania":[45.94,24.97],
    "Ukraine":[48.38,31.17],"Belarus":[53.71,27.95],"Ireland":[53.41,-8.24],
    "Netherlands":[52.13,5.29],"Belgium":[50.50,4.47],"Portugal":[39.40,-8.22],
    "Sweden":[60.13,18.64],"Norway":[60.47,8.47],"Finland":[61.92,25.75],
    "South Korea":[35.91,127.77],"Philippines":[12.88,121.77],
    "Thailand":[15.87,100.99],"Vietnam":[14.06,108.28],"Indonesia":[-0.79,113.92],
    "Malaysia":[4.21,101.98],"Egypt":[26.82,30.80],"Morocco":[31.79,-7.09],
    "Kenya":[-0.02,37.91],"Ethiopia":[9.15,40.49],"Ghana":[7.95,-1.02],
    "Cameroon":[3.85,11.50],"Uganda":[1.37,32.29],"Tanzania":[-6.37,34.89],
    "Bangladesh":[23.68,90.36],"Nepal":[28.39,84.12],"Sri Lanka":[7.87,80.77],
    "Myanmar":[16.87,96.08],"Cambodia":[12.57,104.99],"Dominican Republic":[18.74,-70.16],
    "Haiti":[18.97,-72.29],"Cuba":[21.52,-77.78],"Panama":[8.54,-80.78],
    "Costa Rica":[9.75,-83.75],"El Salvador":[13.79,-88.90],"Nicaragua":[12.87,-85.21],
    "Belize":[17.19,-88.50],"Trinidad and Tobago":[10.69,-61.22],
    "United Arab Emirates":[23.42,53.85],"Saudi Arabia":[23.89,45.08],
    "Israel":[31.05,34.85],"Jordan":[30.59,36.24],"Lebanon":[33.85,35.86],
    "Iraq":[33.22,43.68],"Iran":[32.43,53.69],"Afghanistan":[33.94,67.71],
    "Kazakhstan":[48.02,66.92],"Uzbekistan":[41.38,64.59],
    "Czech Republic":[49.82,15.47],"Slovakia":[48.67,19.70],
    "Hungary":[47.16,19.50],"Austria":[47.52,14.55],"Switzerland":[46.82,8.23],
    "Serbia":[44.02,21.01],"Croatia":[45.10,15.20],"Bulgaria":[42.73,25.49],
    "Lithuania":[55.17,23.88],"Latvia":[56.88,24.60],"Estonia":[58.60,25.01],
    "Moldova":[47.41,28.37],"North Macedonia":[41.61,21.75],
    "Albania":[41.15,20.17],"Kosovo":[42.60,20.90],
}

COUNTRY_NORM = {
    "USA":"United States","US":"United States","UK":"United Kingdom",
    "Russia":"Russian Federation","Korea":"South Korea",
    "Republic of Korea":"South Korea","BZ":"Belize",
}

def nc(c):
    if not c: return None
    return COUNTRY_NORM.get(c.strip(), c.strip())

def effective_age(p):
    if p.age_at_disappearance and p.age_at_disappearance >= 0:
        return p.age_at_disappearance
    if p.date_of_birth and p.date_missing:
        try:
            a = p.date_missing.year - p.date_of_birth.year - (
                (p.date_missing.month, p.date_missing.day) <
                (p.date_of_birth.month, p.date_of_birth.day))
            return a if a >= 0 else None
        except: pass
    return None


# -----------------------------------------------------------------------
# DATA COLLECTION
# -----------------------------------------------------------------------

def collect_core(db):
    total    = db.query(MissingPerson).count()
    active   = db.query(MissingPerson).filter(MissingPerson.is_resolved==False).count()
    resolved = db.query(MissingPerson).filter(MissingPerson.is_resolved==True).count()

    thirty_days_ago = date.today() - timedelta(days=30)
    recent = db.query(MissingPerson).filter(
        MissingPerson.created_at >= datetime.combine(thirty_days_ago, datetime.min.time())
    ).count()

    countries_raw = db.query(MissingPerson.country_last_seen,
                             func.count(MissingPerson.id))\
                      .group_by(MissingPerson.country_last_seen).all()

    sources_raw = db.query(MissingPerson.source,
                           func.count(MissingPerson.id))\
                    .group_by(MissingPerson.source).all()

    gender_raw = db.query(MissingPerson.gender,
                          func.count(MissingPerson.id))\
                   .group_by(MissingPerson.gender).all()

    cases = db.query(MissingPerson).filter(MissingPerson.is_resolved==False).all()

    age_groups = defaultdict(int)
    yearly     = defaultdict(int)
    monthly    = defaultdict(int)
    durations  = defaultdict(int)
    ages       = []
    now        = date.today()
    photo_count= 0

    # Country consolidation
    country_counts = defaultdict(int)
    for c, n in countries_raw:
        country_counts[nc(c) or "Unknown"] += n

    for case in cases:
        if case.photo_url: photo_count += 1
        a = effective_age(case)
        if a is not None:
            ages.append(a)
            if a <= 5:    age_groups["0–5"] += 1
            elif a <= 10: age_groups["6–10"] += 1
            elif a <= 13: age_groups["11–13"] += 1
            elif a <= 15: age_groups["14–15"] += 1
            else:         age_groups["16–17"] += 1

        if case.date_missing:
            yearly[str(case.date_missing.year)] += 1
            monthly[case.date_missing.strftime("%Y-%m")] += 1
            days = (now - case.date_missing).days
            if days < 30:        durations["<1 month"] += 1
            elif days < 180:     durations["1–6 months"] += 1
            elif days < 365:     durations["6–12 months"] += 1
            elif days < 1095:    durations["1–3 years"] += 1
            else:                durations["3+ years"] += 1

    age_stats = {}
    if ages:
        age_stats = {
            "mean":   round(statistics.mean(ages),1),
            "median": statistics.median(ages),
            "min":    min(ages), "max": max(ages),
        }

    # Recent cases for table
    recent_cases = sorted(
        [c for c in cases if c.date_missing],
        key=lambda x: x.date_missing, reverse=True
    )[:50]

    # Source details
    source_details = []
    for src, cnt in sources_raw:
        src_active = db.query(MissingPerson).filter(
            MissingPerson.source==src,
            MissingPerson.is_resolved==False
        ).count()
        src_photo = db.query(MissingPerson).filter(
            MissingPerson.source==src,
            MissingPerson.photo_url!=None
        ).count()
        source_details.append({
            "source": src or "unknown",
            "total": cnt, "active": src_active,
            "resolved": cnt - src_active, "photos": src_photo,
        })

    # Last scraper run per source
    scraper_runs = []
    try:
        runs = db.query(ScraperRun).order_by(ScraperRun.started_at.desc()).limit(50).all()
        seen = set()
        for r in runs:
            if r.scraper_name not in seen:
                seen.add(r.scraper_name)
                scraper_runs.append({
                    "name": r.scraper_name,
                    "started": str(r.started_at)[:16] if r.started_at else "—",
                    "status": r.status or "—",
                    "new": r.records_new or 0,
                    "found": r.records_found or 0,
                    "errors": r.errors or 0,
                })
    except: pass

    return {
        "summary": {
            "total": total, "active": active, "resolved": resolved,
            "resolution_rate": round(resolved/total*100,1) if total else 0,
            "with_photo": photo_count, "added_recently": recent,
            "age_stats": age_stats,
        },
        "countries": sorted(
            [{"country": c, "count": n} for c,n in country_counts.items()],
            key=lambda x: x["count"], reverse=True
        ),
        "sources":  source_details,
        "gender":   [{"gender": g or "Unknown","count": n} for g,n in gender_raw],
        "age_groups":dict(age_groups),
        "yearly":   dict(sorted(yearly.items())),
        "monthly":  dict(sorted(monthly.items())),
        "durations":dict(durations),
        "recent_cases": [
            {
                "name":    c.full_name,
                "age":     effective_age(c),
                "gender":  c.gender,
                "country": nc(c.country_last_seen),
                "city":    c.city_last_seen,
                "date":    str(c.date_missing),
                "source":  c.source,
                "url":     c.source_url,
                "photo":   c.photo_url,
            } for c in recent_cases
        ],
        "scraper_runs": scraper_runs,
    }


# -----------------------------------------------------------------------
# HTML DASHBOARD
# -----------------------------------------------------------------------

def build_dashboard(core, patterns):
    country_coords_js = json.dumps(COUNTRY_COORDS)
    core_js           = json.dumps(core, default=str)
    patterns_js       = json.dumps(patterns, default=str) if patterns else "null"
    generated         = datetime.now().strftime("%Y-%m-%d %H:%M")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Missing Children Intelligence Dashboard</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;600;800&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
:root {{
  --bg:      #07090f;
  --bg2:     #0e1220;
  --bg3:     #141926;
  --border:  #1e2a3a;
  --accent:  #e63946;
  --accent2: #f4a261;
  --accent3: #2a9d8f;
  --text:    #e8edf5;
  --muted:   #6b7a99;
  --mono:    'Space Mono', monospace;
  --sans:    'Syne', sans-serif;
}}
*,*::before,*::after {{ box-sizing:border-box; margin:0; padding:0; }}
html {{ scroll-behavior:smooth; }}
body {{
  background:var(--bg);
  color:var(--text);
  font-family:var(--sans);
  min-height:100vh;
  overflow-x:hidden;
}}

/* NOISE TEXTURE */
body::before {{
  content:'';
  position:fixed; inset:0; pointer-events:none; z-index:0;
  background-image:url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='noise'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23noise)' opacity='0.035'/%3E%3C/svg%3E");
  opacity:.4;
}}

/* HEADER */
header {{
  position:sticky; top:0; z-index:1000;
  background:rgba(7,9,15,.92);
  backdrop-filter:blur(12px);
  border-bottom:1px solid var(--border);
  padding:0 32px;
  display:flex; align-items:center; justify-content:space-between;
  height:56px;
}}
.logo {{
  font-family:var(--mono);
  font-size:13px; font-weight:700;
  letter-spacing:3px;
  color:var(--accent);
  text-transform:uppercase;
}}
.logo span {{ color:var(--muted); }}
.nav {{ display:flex; gap:4px; }}
.nav-btn {{
  background:none; border:none; cursor:pointer;
  font-family:var(--mono); font-size:11px; letter-spacing:1px;
  color:var(--muted); padding:6px 14px; border-radius:4px;
  transition:.2s; text-transform:uppercase;
}}
.nav-btn:hover,.nav-btn.active {{ color:var(--text); background:var(--bg3); }}
.gen-time {{
  font-family:var(--mono); font-size:10px;
  color:var(--muted); letter-spacing:1px;
}}

/* SECTIONS */
.section {{
  padding:40px 32px;
  border-bottom:1px solid var(--border);
}}
.section-title {{
  font-family:var(--mono); font-size:11px; letter-spacing:3px;
  color:var(--accent); text-transform:uppercase;
  margin-bottom:24px; display:flex; align-items:center; gap:12px;
}}
.section-title::after {{
  content:''; flex:1; height:1px; background:var(--border);
}}

/* KPI GRID */
.kpi-grid {{
  display:grid;
  grid-template-columns:repeat(auto-fit,minmax(160px,1fr));
  gap:16px; margin-bottom:40px;
}}
.kpi {{
  background:var(--bg2);
  border:1px solid var(--border);
  border-radius:8px;
  padding:20px 24px;
  position:relative; overflow:hidden;
  transition:border-color .2s;
}}
.kpi:hover {{ border-color:var(--accent); }}
.kpi::before {{
  content:''; position:absolute;
  top:0; left:0; right:0; height:2px;
  background:var(--accent);
  opacity:.5;
}}
.kpi.green::before {{ background:var(--accent3); }}
.kpi.orange::before {{ background:var(--accent2); }}
.kpi-val {{
  font-family:var(--mono);
  font-size:32px; font-weight:700;
  color:var(--text); line-height:1;
}}
.kpi-label {{
  font-size:11px; letter-spacing:1px;
  color:var(--muted); text-transform:uppercase;
  margin-top:8px;
}}

/* MAP */
#map {{
  height:520px; border-radius:8px;
  border:1px solid var(--border);
  background:var(--bg2);
}}
.map-controls {{
  display:flex; gap:8px; margin-bottom:12px; flex-wrap:wrap;
}}
.map-btn {{
  font-family:var(--mono); font-size:11px; letter-spacing:1px;
  padding:6px 16px; border-radius:4px; cursor:pointer;
  border:1px solid var(--border); background:var(--bg2);
  color:var(--muted); transition:.2s; text-transform:uppercase;
}}
.map-btn:hover,.map-btn.active {{
  background:var(--accent); border-color:var(--accent);
  color:#fff;
}}

/* CHARTS GRID */
.charts-grid {{
  display:grid;
  grid-template-columns:repeat(auto-fit,minmax(320px,1fr));
  gap:20px;
}}
.chart-box {{
  background:var(--bg2);
  border:1px solid var(--border);
  border-radius:8px;
  padding:20px;
}}
.chart-title {{
  font-family:var(--mono); font-size:10px; letter-spacing:2px;
  color:var(--muted); text-transform:uppercase; margin-bottom:16px;
}}
.chart-box canvas {{ max-height:240px; }}

/* FILTER BAR */
.filter-bar {{
  display:flex; gap:12px; margin-bottom:20px; flex-wrap:wrap;
  align-items:center;
}}
.filter-bar input, .filter-bar select {{
  font-family:var(--mono); font-size:12px;
  background:var(--bg2); border:1px solid var(--border);
  color:var(--text); border-radius:6px; padding:8px 14px;
  outline:none; transition:.2s;
}}
.filter-bar input:focus, .filter-bar select:focus {{
  border-color:var(--accent);
}}
.filter-bar input {{ width:260px; }}
.badge {{
  font-family:var(--mono); font-size:10px; letter-spacing:1px;
  padding:3px 10px; border-radius:20px;
  background:var(--bg3); border:1px solid var(--border);
  color:var(--muted);
}}
.badge.red {{ background:rgba(230,57,70,.15); border-color:var(--accent); color:var(--accent); }}
.badge.orange {{ background:rgba(244,162,97,.15); border-color:var(--accent2); color:var(--accent2); }}
.badge.green {{ background:rgba(42,157,143,.15); border-color:var(--accent3); color:var(--accent3); }}
.badge.active {{ background:rgba(230,57,70,.25); border-color:var(--accent); color:var(--accent); animation:pulse 2s infinite; }}

@keyframes pulse {{
  0%,100% {{ opacity:1; }} 50% {{ opacity:.5; }}
}}

/* TABLE */
.table-wrap {{ overflow-x:auto; }}
table {{
  width:100%; border-collapse:collapse;
  font-size:13px;
}}
th {{
  font-family:var(--mono); font-size:10px; letter-spacing:2px;
  color:var(--muted); text-transform:uppercase;
  padding:10px 14px; text-align:left;
  border-bottom:1px solid var(--border);
  position:sticky; top:56px; background:var(--bg);
}}
td {{
  padding:10px 14px;
  border-bottom:1px solid rgba(30,42,58,.5);
  vertical-align:middle;
}}
tr:hover td {{ background:var(--bg2); }}
.photo-thumb {{
  width:36px; height:36px; border-radius:4px;
  object-fit:cover; background:var(--bg3);
  border:1px solid var(--border);
}}
.no-photo {{
  width:36px; height:36px; border-radius:4px;
  background:var(--bg3); border:1px solid var(--border);
  display:flex; align-items:center; justify-content:center;
  font-size:14px; color:var(--muted);
}}
.name-link {{ color:var(--text); text-decoration:none; }}
.name-link:hover {{ color:var(--accent); }}

/* PATTERN CARDS */
.pattern-grid {{
  display:grid;
  grid-template-columns:repeat(auto-fill,minmax(380px,1fr));
  gap:16px;
}}
.p-card {{
  background:var(--bg2);
  border:1px solid var(--border);
  border-radius:8px; padding:18px;
  cursor:pointer; transition:.2s;
  position:relative;
}}
.p-card:hover {{ border-color:var(--accent2); }}
.p-card.active-flag {{ border-left:3px solid var(--accent); }}
.p-card-header {{
  display:flex; align-items:flex-start;
  justify-content:space-between; gap:12px;
  margin-bottom:12px;
}}
.p-card-title {{
  font-weight:600; font-size:14px;
  line-height:1.3;
}}
.p-card-meta {{
  font-family:var(--mono); font-size:11px;
  color:var(--muted); margin-top:4px;
}}
.p-card-members {{
  display:none; margin-top:12px;
  padding-top:12px; border-top:1px solid var(--border);
}}
.p-card-members.open {{ display:block; }}
.member-row {{
  display:flex; gap:10px; align-items:center;
  padding:6px 0; border-bottom:1px solid rgba(30,42,58,.5);
  font-size:12px;
}}
.member-row:last-child {{ border-bottom:none; }}
.member-photo {{
  width:28px; height:28px; border-radius:3px;
  object-fit:cover; flex-shrink:0;
  background:var(--bg3); border:1px solid var(--border);
}}
.member-info {{ flex:1; }}
.member-name {{ font-weight:600; font-size:12px; }}
.member-detail {{ color:var(--muted); font-size:11px; font-family:var(--mono); }}

/* CORRIDOR */
.corridor-card {{
  background:var(--bg2);
  border:1px solid var(--border);
  border-left:3px solid var(--accent2);
  border-radius:8px; padding:18px;
  margin-bottom:12px;
}}
.corridor-label {{
  font-weight:600; font-size:14px; margin-bottom:6px;
}}
.corridor-count {{
  font-family:var(--mono); font-size:24px; font-weight:700;
  color:var(--accent2); float:right; margin-top:-4px;
}}

/* TARGETING */
.targeting-row {{
  display:flex; align-items:center; gap:12px;
  padding:14px 0; border-bottom:1px solid var(--border);
}}
.targeting-flags {{ display:flex; gap:6px; flex-wrap:wrap; }}
.flag-chip {{
  font-family:var(--mono); font-size:9px; letter-spacing:.5px;
  padding:2px 8px; border-radius:3px;
  background:rgba(230,57,70,.12); border:1px solid rgba(230,57,70,.3);
  color:var(--accent); text-transform:uppercase;
}}
.flag-chip.yellow {{
  background:rgba(244,162,97,.12); border-color:rgba(244,162,97,.3);
  color:var(--accent2);
}}
.bar-container {{ flex:1; }}
.bar-track {{
  height:6px; background:var(--bg3); border-radius:3px; overflow:hidden;
}}
.bar-fill {{
  height:100%; background:var(--accent); border-radius:3px;
  transition:width .6s cubic-bezier(.4,0,.2,1);
}}

/* TIMELINE SPIKES */
.spike-item {{
  display:flex; align-items:center; gap:16px;
  padding:10px 0; border-bottom:1px solid var(--border);
  font-size:13px;
}}
.spike-month {{
  font-family:var(--mono); font-size:12px; color:var(--muted);
  min-width:80px;
}}
.spike-z {{
  font-family:var(--mono); font-size:13px; font-weight:700;
}}
.spike-bar {{
  flex:1; height:4px; background:var(--bg3); border-radius:2px; overflow:hidden;
}}
.spike-fill {{
  height:100%; border-radius:2px;
  background:linear-gradient(90deg, var(--accent2), var(--accent));
}}

/* SCRAPER STATUS */
.scraper-grid {{
  display:grid; grid-template-columns:repeat(auto-fill,minmax(200px,1fr));
  gap:12px;
}}
.scraper-card {{
  background:var(--bg2); border:1px solid var(--border);
  border-radius:8px; padding:14px 16px;
}}
.scraper-name {{
  font-family:var(--mono); font-size:11px; letter-spacing:1px;
  color:var(--muted); text-transform:uppercase; margin-bottom:8px;
}}
.scraper-stat {{
  display:flex; justify-content:space-between;
  font-size:12px; padding:2px 0;
}}

/* PAGINATION */
.pagination {{
  display:flex; gap:6px; align-items:center;
  margin-top:16px; justify-content:flex-end;
}}
.page-btn {{
  font-family:var(--mono); font-size:11px;
  padding:5px 12px; border-radius:4px; cursor:pointer;
  border:1px solid var(--border); background:var(--bg2);
  color:var(--muted); transition:.2s;
}}
.page-btn:hover,.page-btn.active {{
  background:var(--accent); border-color:var(--accent); color:#fff;
}}
.page-info {{
  font-family:var(--mono); font-size:11px; color:var(--muted);
}}

/* EMPTY STATE */
.empty {{
  text-align:center; padding:60px; color:var(--muted);
  font-family:var(--mono); font-size:13px;
}}

/* TOOLTIP */
.leaflet-popup-content-wrapper {{
  background:var(--bg2) !important;
  border:1px solid var(--border) !important;
  border-radius:8px !important;
  color:var(--text) !important;
  box-shadow:0 8px 32px rgba(0,0,0,.6) !important;
}}
.leaflet-popup-tip {{ background:var(--bg2) !important; }}

/* RESPONSIVE */
@media(max-width:768px) {{
  .section {{ padding:24px 16px; }}
  header {{ padding:0 16px; }}
  .nav {{ display:none; }}
  .kpi-grid {{ grid-template-columns:repeat(2,1fr); }}
}}

/* PAGE VISIBILITY */
.page {{ display:none; }}
.page.active {{ display:block; }}
</style>
</head>
<body>

<header>
  <div class="logo">MCID <span>// Missing Children Intelligence Dashboard</span></div>
  <nav class="nav">
    <button class="nav-btn active" onclick="showPage('overview')">Overview</button>
    <button class="nav-btn" onclick="showPage('cases')">Cases</button>
    <button class="nav-btn" onclick="showPage('patterns')">Patterns</button>
    <button class="nav-btn" onclick="showPage('corridors')">Corridors</button>
    <button class="nav-btn" onclick="showPage('system')">System</button>
  </nav>
  <div class="gen-time">Generated: {generated}</div>
</header>

<!-- ===== OVERVIEW PAGE ===== -->
<div id="page-overview" class="page active">

  <div class="section">
    <div class="section-title">Summary Statistics</div>
    <div class="kpi-grid" id="kpi-grid"></div>
    <div class="section-title">Global Distribution</div>
    <div class="map-controls">
      <button class="map-btn active" onclick="setMapMode('cases')">Cases Count</button>
      <button class="map-btn" onclick="setMapMode('bursts')">Active Bursts</button>
      <button class="map-btn" onclick="setMapMode('corridors')">Corridors</button>
    </div>
    <div id="map"></div>
  </div>

  <div class="section">
    <div class="section-title">Analytics</div>
    <div class="charts-grid">
      <div class="chart-box">
        <div class="chart-title">Cases by Source</div>
        <canvas id="chart-sources"></canvas>
      </div>
      <div class="chart-box">
        <div class="chart-title">Gender Breakdown</div>
        <canvas id="chart-gender"></canvas>
      </div>
      <div class="chart-box">
        <div class="chart-title">Age at Disappearance</div>
        <canvas id="chart-ages"></canvas>
      </div>
      <div class="chart-box">
        <div class="chart-title">Cases per Year</div>
        <canvas id="chart-years"></canvas>
      </div>
      <div class="chart-box">
        <div class="chart-title">Missing Duration</div>
        <canvas id="chart-durations"></canvas>
      </div>
      <div class="chart-box">
        <div class="chart-title">Top Countries</div>
        <canvas id="chart-countries"></canvas>
      </div>
    </div>
  </div>

</div>

<!-- ===== CASES PAGE ===== -->
<div id="page-cases" class="page">
  <div class="section">
    <div class="section-title">Case Explorer</div>
    <div class="filter-bar">
      <input type="text" id="case-search" placeholder="Search name, city, country..." oninput="filterCases()">
      <select id="case-country" onchange="filterCases()">
        <option value="">All Countries</option>
      </select>
      <select id="case-source" onchange="filterCases()">
        <option value="">All Sources</option>
      </select>
      <select id="case-age" onchange="filterCases()">
        <option value="">All Ages</option>
        <option value="0-5">0–5</option>
        <option value="6-10">6–10</option>
        <option value="11-13">11–13</option>
        <option value="14-15">14–15</option>
        <option value="16-17">16–17</option>
      </select>
      <span id="case-count" class="badge"></span>
    </div>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th></th>
            <th>Name</th>
            <th>Age</th>
            <th>Gender</th>
            <th>Location</th>
            <th>Country</th>
            <th>Date Missing</th>
            <th>Source</th>
          </tr>
        </thead>
        <tbody id="cases-tbody"></tbody>
      </table>
    </div>
    <div class="pagination" id="cases-pagination"></div>
  </div>
</div>

<!-- ===== PATTERNS PAGE ===== -->
<div id="page-patterns" class="page">
  <div class="section">
    <div class="section-title">Surname Clusters</div>
    <div class="filter-bar">
      <select id="cluster-type" onchange="renderClusters()">
        <option value="">All Types</option>
        <option value="SIBLING_UNIT">Sibling Units</option>
        <option value="FAMILY_GROUP">Family Groups</option>
        <option value="CROSS_BORDER">Cross-Border</option>
      </select>
      <input type="text" id="cluster-search" placeholder="Search surname or country..." oninput="renderClusters()" style="width:220px">
      <span id="cluster-count" class="badge"></span>
    </div>
    <div class="pattern-grid" id="cluster-grid"></div>
  </div>

  <div class="section">
    <div class="section-title">Spatio-Temporal Bursts</div>
    <div class="filter-bar">
      <label style="font-family:var(--mono);font-size:12px;color:var(--muted);display:flex;align-items:center;gap:8px;">
        <input type="checkbox" id="burst-active-only" onchange="renderBursts()"> Active only (last 90d)
      </label>
      <span id="burst-count" class="badge"></span>
    </div>
    <div class="pattern-grid" id="burst-grid"></div>
  </div>

  <div class="section">
    <div class="section-title">Timeline Anomalies</div>
    <div id="spikes-list"></div>
  </div>

  <div class="section">
    <div class="section-title">Demographic Targeting by Country</div>
    <div id="targeting-list"></div>
  </div>
</div>

<!-- ===== CORRIDORS PAGE ===== -->
<div id="page-corridors" class="page">
  <div class="section">
    <div class="section-title">Trafficking Corridor Matches</div>
    <p style="color:var(--muted);font-size:13px;margin-bottom:24px;line-height:1.7;">
      Matches are computed by comparing nationality (origin) against country of disappearance (destination)
      and scoring against UNODC/IOM/Europol documented trafficking routes.
      Coverage depends on nationality data availability (currently ~2% of records — Interpol source only).
    </p>
    <div id="corridor-cards"></div>
  </div>
  <div class="section">
    <div class="section-title">Origin → Destination Flow Matrix</div>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Origin</th><th>Destination</th><th>Cases</th></tr></thead>
        <tbody id="flow-tbody"></tbody>
      </table>
    </div>
  </div>
</div>

<!-- ===== SYSTEM PAGE ===== -->
<div id="page-system" class="page">
  <div class="section">
    <div class="section-title">Data Sources</div>
    <div class="table-wrap">
      <table>
        <thead>
          <tr><th>Source</th><th>Total</th><th>Active</th><th>Resolved</th><th>With Photo</th></tr>
        </thead>
        <tbody id="sources-tbody"></tbody>
      </table>
    </div>
  </div>
  <div class="section">
    <div class="section-title">Scraper Status</div>
    <div class="scraper-grid" id="scraper-grid"></div>
  </div>
</div>

<script>
const CORE     = {core_js};
const PATTERNS = {patterns_js};
const COORDS   = {country_coords_js};

// -----------------------------------------------------------------------
// NAVIGATION
// -----------------------------------------------------------------------
function showPage(name) {{
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
  document.getElementById('page-'+name).classList.add('active');
  event.target.classList.add('active');
  if(name==='overview') setTimeout(()=>map.invalidateSize(),100);
}}

// -----------------------------------------------------------------------
// CHART DEFAULTS
// -----------------------------------------------------------------------
Chart.defaults.color = '#6b7a99';
Chart.defaults.font.family = "'Space Mono', monospace";
Chart.defaults.font.size = 11;

const PALETTE = [
  '#e63946','#f4a261','#2a9d8f','#457b9d','#a8dadc',
  '#e9c46a','#264653','#f77f00','#023e8a','#7b2d8b'
];

function makeChart(id, type, labels, data, label, opts={{}}) {{
  const ctx = document.getElementById(id);
  if(!ctx) return;
  new Chart(ctx, {{
    type, data: {{
      labels,
      datasets: [{{
        label,
        data,
        backgroundColor: type==='line' ? 'rgba(230,57,70,.15)' : PALETTE,
        borderColor: type==='line' ? '#e63946' : PALETTE,
        borderWidth: type==='line' ? 2 : 1,
        tension: .4,
        fill: type==='line',
        pointBackgroundColor: '#e63946',
        pointRadius: type==='line' ? 3 : 0,
      }}]
    }},
    options: {{
      responsive:true, maintainAspectRatio:true,
      plugins: {{
        legend: {{ display: type==='pie'||type==='doughnut' }},
        tooltip: {{ backgroundColor:'#0e1220', borderColor:'#1e2a3a', borderWidth:1 }},
      }},
      scales: type==='pie'||type==='doughnut' ? {{}} : {{
        x: {{ grid:{{ color:'rgba(30,42,58,.5)' }}, ticks:{{ maxRotation:45 }} }},
        y: {{ grid:{{ color:'rgba(30,42,58,.5)' }} }},
      }},
      ...opts
    }}
  }});
}}

// -----------------------------------------------------------------------
// KPIs
// -----------------------------------------------------------------------
function renderKPIs() {{
  const s = CORE.summary;
  const p = PATTERNS ? PATTERNS.summary : null;
  const items = [
    {{ val: s.total.toLocaleString(), label:'Total Cases', cls:'' }},
    {{ val: s.active.toLocaleString(), label:'Active Cases', cls:'red' }},
    {{ val: s.resolved.toLocaleString(), label:'Resolved', cls:'green' }},
    {{ val: s.resolution_rate+'%', label:'Resolution Rate', cls:'green' }},
    {{ val: s.with_photo.toLocaleString(), label:'With Photo', cls:'' }},
    {{ val: s.added_recently.toLocaleString(), label:'Added (30d)', cls:'orange' }},
    ...(p ? [
      {{ val: p.spatiotemporal_bursts, label:'Burst Clusters', cls:'' }},
      {{ val: p.active_bursts, label:'Active Bursts', cls:'red' }},
      {{ val: p.active_corridors, label:'Corridors Active', cls:'orange' }},
      {{ val: p.flagged_countries, label:'Countries Flagged', cls:'' }},
    ] : []),
  ];
  document.getElementById('kpi-grid').innerHTML = items.map(i=>
    `<div class="kpi ${{i.cls}}">
      <div class="kpi-val">${{i.val}}</div>
      <div class="kpi-label">${{i.label}}</div>
    </div>`
  ).join('');
}}

// -----------------------------------------------------------------------
// MAP
// -----------------------------------------------------------------------
let map, markersLayer, burstLayer, corridorLayer;
let mapMode = 'cases';

function initMap() {{
  map = L.map('map', {{ zoomControl:true }}).setView([20,0],2);
  L.tileLayer('https://{{s}}.basemaps.cartocdn.com/dark_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
    attribution:'© CartoDB', maxZoom:19
  }}).addTo(map);

  markersLayer  = L.layerGroup().addTo(map);
  burstLayer    = L.layerGroup();
  corridorLayer = L.layerGroup();

  renderMapCases();
}}

function setMapMode(mode) {{
  mapMode = mode;
  document.querySelectorAll('.map-btn').forEach(b=>b.classList.remove('active'));
  event.target.classList.add('active');
  markersLayer.clearLayers();
  burstLayer.clearLayers();
  corridorLayer.clearLayers();
  map.removeLayer(burstLayer);
  map.removeLayer(corridorLayer);
  if(mode==='cases')    {{ markersLayer.addTo(map); renderMapCases(); }}
  if(mode==='bursts')   {{ burstLayer.addTo(map); renderMapBursts(); }}
  if(mode==='corridors'){{ corridorLayer.addTo(map); renderMapCorridors(); }}
}}

function circleMarker(latlng, count, color, popup) {{
  const r = Math.min(6 + Math.log(count+1)*5, 40);
  return L.circleMarker(latlng, {{
    radius:r, fillColor:color, color:'#fff',
    weight:1, opacity:.8, fillOpacity:.6
  }}).bindPopup(popup);
}}

function renderMapCases() {{
  markersLayer.clearLayers();
  CORE.countries.forEach(c => {{
    const coords = COORDS[c.country];
    if(!coords) return;
    const m = circleMarker(coords, c.count, '#e63946',
      `<b>${{c.country}}</b><br>${{c.count.toLocaleString()}} active cases`);
    markersLayer.addLayer(m);
  }});
}}

function renderMapBursts() {{
  burstLayer.clearLayers();
  if(!PATTERNS) return;
  PATTERNS.spatiotemporal.forEach(b => {{
    const coords = COORDS[b.country];
    if(!coords) return;
    const color = b.is_active ? '#e63946' : '#f4a261';
    const m = circleMarker(coords, b.count, color,
      `<b>${{b.city}}, ${{b.country}}</b><br>
       ${{b.count}} children in ${{b.date_range.span_days}} days<br>
       ${{b.date_range.from}} → ${{b.date_range.to}}<br>
       ${{b.is_active ? '<span style="color:#e63946">⚠ ACTIVE (last 90d)</span>' : ''}}`);
    burstLayer.addLayer(m);
  }});
}}

function renderMapCorridors() {{
  corridorLayer.clearLayers();
  if(!PATTERNS) return;
  PATTERNS.corridors.flow_matrix.slice(0,20).forEach(f => {{
    const from = COORDS[f.from], to = COORDS[f.to];
    if(!from||!to) return;
    const line = L.polyline([from,to], {{
      color:'#f4a261', weight:Math.min(1+f.count,6), opacity:.6,
      dashArray:'6,4'
    }}).bindPopup(`${{f.from}} → ${{f.to}}<br>${{f.count}} cases`);
    corridorLayer.addLayer(line);
    const mid = [(from[0]+to[0])/2,(from[1]+to[1])/2];
    L.circleMarker(mid, {{
      radius:4+f.count, fillColor:'#f4a261',
      color:'#fff', weight:1, fillOpacity:.8
    }}).addTo(corridorLayer);
  }});
}}

// -----------------------------------------------------------------------
// CASES TABLE
// -----------------------------------------------------------------------
let filteredCases = [];
let casePage = 1;
const PAGE_SIZE = 25;

function initCases() {{
  // Populate filter dropdowns
  const countries = [...new Set(CORE.recent_cases.map(c=>c.country).filter(Boolean))].sort();
  const sources   = [...new Set(CORE.recent_cases.map(c=>c.source).filter(Boolean))].sort();
  const cSel = document.getElementById('case-country');
  const sSel = document.getElementById('case-source');
  countries.forEach(c=>{{ const o=document.createElement('option'); o.value=c; o.text=c; cSel.appendChild(o); }});
  sources.forEach(s=>{{ const o=document.createElement('option'); o.value=s; o.text=s; sSel.appendChild(o); }});
  filterCases();
}}

function filterCases() {{
  const q       = document.getElementById('case-search').value.toLowerCase();
  const country = document.getElementById('case-country').value;
  const source  = document.getElementById('case-source').value;
  const ageRange= document.getElementById('case-age').value;

  filteredCases = CORE.recent_cases.filter(c => {{
    if(q && !`${{c.name}} ${{c.city}} ${{c.country}}`.toLowerCase().includes(q)) return false;
    if(country && c.country !== country) return false;
    if(source && c.source !== source) return false;
    if(ageRange && c.age !== null) {{
      const [lo,hi] = ageRange.split('-').map(Number);
      if(c.age < lo || c.age > hi) return false;
    }}
    return true;
  }});

  casePage = 1;
  document.getElementById('case-count').textContent = filteredCases.length+' cases';
  renderCasesPage();
}}

function renderCasesPage() {{
  const start  = (casePage-1)*PAGE_SIZE;
  const slice  = filteredCases.slice(start, start+PAGE_SIZE);
  const tbody  = document.getElementById('cases-tbody');

  tbody.innerHTML = slice.map(c => `
    <tr>
      <td>${{c.photo
        ? `<img class="photo-thumb" src="${{c.photo}}" onerror="this.style.display='none'" loading="lazy">`
        : `<div class="no-photo">👤</div>`
      }}</td>
      <td><a class="name-link" href="${{c.url||'#'}}" target="_blank">${{c.name||'—'}}</a></td>
      <td>${{c.age??'—'}}</td>
      <td>${{c.gender||'—'}}</td>
      <td>${{c.city||'—'}}</td>
      <td>${{c.country||'—'}}</td>
      <td style="font-family:var(--mono);font-size:12px">${{c.date||'—'}}</td>
      <td><span class="badge">${{c.source||'—'}}</span></td>
    </tr>`).join('');

  renderPagination();
}}

function renderPagination() {{
  const totalPages = Math.ceil(filteredCases.length/PAGE_SIZE);
  const pg = document.getElementById('cases-pagination');
  if(totalPages<=1){{ pg.innerHTML=''; return; }}
  let html = `<span class="page-info">Page ${{casePage}}/${{totalPages}}</span>`;
  if(casePage>1) html+=`<button class="page-btn" onclick="goPage(${{casePage-1}})">←</button>`;
  const start=Math.max(1,casePage-2), end=Math.min(totalPages,casePage+2);
  for(let i=start;i<=end;i++)
    html+=`<button class="page-btn ${{i===casePage?'active':''}}" onclick="goPage(${{i}})">${{i}}</button>`;
  if(casePage<totalPages) html+=`<button class="page-btn" onclick="goPage(${{casePage+1}})">→</button>`;
  pg.innerHTML=html;
}}

function goPage(n) {{ casePage=n; renderCasesPage(); window.scrollTo(0,200); }}

// -----------------------------------------------------------------------
// PATTERN CARDS
// -----------------------------------------------------------------------
function renderClusters() {{
  if(!PATTERNS) {{ document.getElementById('cluster-grid').innerHTML='<div class="empty">No pattern data available.</div>'; return; }}
  const type = document.getElementById('cluster-type').value;
  const q    = document.getElementById('cluster-search').value.toLowerCase();

  let data = PATTERNS.surname_clusters.filter(c => {{
    if(type && c.type !== type) return false;
    if(q && !`${{c.surname}} ${{c.country}}`.toLowerCase().includes(q)) return false;
    return true;
  }});

  document.getElementById('cluster-count').textContent = data.length+' clusters';

  document.getElementById('cluster-grid').innerHTML = data.slice(0,60).map((c,i) => {{
    const ar    = c.age_range||{{}};
    const dr    = c.date_range||{{}};
    const badge = c.type==='SIBLING_UNIT' ? 'orange' : c.type==='FAMILY_GROUP' ? 'red' : 'green';
    const members = c.members.slice(0,8).map(m=>
      `<div class="member-row">
        ${{m.photo ? `<img class="member-photo" src="${{m.photo}}" onerror="this.style.display='none'" loading="lazy">` : '<div class="member-photo" style="font-size:10px;display:flex;align-items:center;justify-content:center;color:var(--muted)">👤</div>'}}
        <div class="member-info">
          <div class="member-name">${{m.name||'—'}}</div>
          <div class="member-detail">age=${{m.age??'?'}} · ${{m.date_missing||'date unknown'}} · ${{m.city||'?'}}</div>
        </div>
        ${{m.source_url ? `<a href="${{m.source_url}}" target="_blank" style="color:var(--accent);font-size:18px;text-decoration:none">↗</a>` : ''}}
      </div>`
    ).join('');
    return `
    <div class="p-card ${{c.type==='SIBLING_UNIT'||c.type==='FAMILY_GROUP'?'':''}}" onclick="toggleCard(this)">
      <div class="p-card-header">
        <div>
          <div class="p-card-title">${{c.surname}}</div>
          <div class="p-card-meta">${{c.country}}</div>
        </div>
        <div style="text-align:right">
          <span class="badge ${{badge}}">${{c.type.replace('_',' ')}}</span><br>
          <span style="font-family:var(--mono);font-size:20px;font-weight:700;color:var(--text)">${{c.count}}</span>
        </div>
      </div>
      ${{ar.min!=null ? `<div style="font-size:12px;color:var(--muted)">Ages ${{ar.min}}–${{ar.max}}</div>` : ''}}
      ${{dr.from ? `<div style="font-family:var(--mono);font-size:11px;color:var(--muted);margin-top:4px">${{dr.from}} → ${{dr.to}} (${{dr.span_days}}d)</div>` : ''}}
      <div class="p-card-members" id="card-members-${{i}}">
        ${{members}}
        ${{c.count>8 ? `<div style="font-family:var(--mono);font-size:11px;color:var(--muted);padding:8px 0">+${{c.count-8}} more</div>` : ''}}
      </div>
    </div>`;
  }}).join('') || '<div class="empty">No clusters match the current filter.</div>';
}}

function toggleCard(card) {{
  card.querySelector('.p-card-members').classList.toggle('open');
}}

function renderBursts() {{
  if(!PATTERNS) return;
  const activeOnly = document.getElementById('burst-active-only').checked;
  let data = PATTERNS.spatiotemporal;
  if(activeOnly) data = data.filter(b=>b.is_active);
  document.getElementById('burst-count').textContent = data.length+' bursts';

  document.getElementById('burst-grid').innerHTML = data.slice(0,60).map((b,i) => {{
    const dr = b.date_range;
    const ar = b.age_range||{{}};
    const members = b.members.slice(0,6).map(m=>
      `<div class="member-row">
        ${{m.photo ? `<img class="member-photo" src="${{m.photo}}" onerror="this.style.display='none'" loading="lazy">` : '<div class="member-photo" style="font-size:10px;display:flex;align-items:center;justify-content:center;color:var(--muted)">👤</div>'}}
        <div class="member-info">
          <div class="member-name">${{m.name||'—'}}</div>
          <div class="member-detail">age=${{m.age??'?'}} · ${{m.date_missing}}</div>
        </div>
        ${{m.source_url ? `<a href="${{m.source_url}}" target="_blank" style="color:var(--accent);font-size:18px;text-decoration:none">↗</a>` : ''}}
      </div>`
    ).join('');
    return `
    <div class="p-card ${{b.is_active?'active-flag':''}}" onclick="toggleCard(this)">
      <div class="p-card-header">
        <div>
          <div class="p-card-title">${{b.city}}, ${{b.country}}</div>
          <div class="p-card-meta">${{dr.from}} → ${{dr.to}} (${{dr.span_days}}d)</div>
        </div>
        <div style="text-align:right">
          ${{b.is_active ? '<span class="badge active">ACTIVE</span>' : '<span class="badge">HISTORICAL</span>'}}<br>
          <span style="font-family:var(--mono);font-size:20px;font-weight:700;color:var(--text)">${{b.count}}</span>
        </div>
      </div>
      ${{ar.mean ? `<div style="font-size:12px;color:var(--muted)">Ages ${{ar.min}}–${{ar.max}} · mean ${{ar.mean}}</div>` : ''}}
      ${{Object.keys(b.gender_breakdown||{{}}).length ? `<div style="font-size:12px;color:var(--muted);margin-top:2px">Gender: ${{JSON.stringify(b.gender_breakdown)}}</div>` : ''}}
      <div class="p-card-members">
        ${{members}}
        ${{b.count>6 ? `<div style="font-family:var(--mono);font-size:11px;color:var(--muted);padding:8px 0">+${{b.count-6}} more</div>` : ''}}
      </div>
    </div>`;
  }}).join('') || '<div class="empty">No bursts match the current filter.</div>';
}}

function renderSpikes() {{
  if(!PATTERNS) return;
  const spikes  = PATTERNS.timeline.spikes;
  const maxZ    = Math.max(...spikes.map(s=>s.z_score));
  document.getElementById('spikes-list').innerHTML = spikes.map(s=>
    `<div class="spike-item">
      <span class="spike-month">${{s.month}}</span>
      <span style="flex:2;font-size:13px">${{s.country}}</span>
      <span style="font-family:var(--mono);font-size:13px;min-width:50px">${{s.count}} cases</span>
      <div class="spike-bar" style="flex:3">
        <div class="spike-fill" style="width:${{Math.round(s.z_score/maxZ*100)}}%"></div>
      </div>
      <span class="spike-z" style="min-width:60px;color:${{s.z_score>5?'var(--accent)':'var(--accent2)'}}">z=${{s.z_score}}</span>
      ${{s.is_active ? '<span class="badge active">ACTIVE</span>' : ''}}
    </div>`
  ).join('');
}}

function renderTargeting() {{
  if(!PATTERNS) return;
  const tgt    = PATTERNS.targeting.filter(t=>t.flags.length>0);
  const maxN   = Math.max(...tgt.map(t=>t.total_cases));
  document.getElementById('targeting-list').innerHTML = tgt.map(t=>
    `<div class="targeting-row">
      <div style="min-width:180px;font-weight:600;font-size:13px">${{t.country}}</div>
      <div class="bar-container">
        <div class="bar-track">
          <div class="bar-fill" style="width:${{Math.round(t.total_cases/maxN*100)}}%"></div>
        </div>
        <div style="font-size:11px;color:var(--muted);font-family:var(--mono);margin-top:3px">
          ${{t.total_cases}} cases · mean age ${{t.age_mean??'?'}} · under-12: ${{t.age_under_12}} · teen: ${{t.age_teen}}
        </div>
      </div>
      <div class="targeting-flags">
        ${{t.flags.map(f=>`<span class="flag-chip ${{f.includes('FEMALE')||f.includes('YOUNG')?'yellow':''}}">${{f.replace(/_/g,' ')}}</span>`).join('')}}
      </div>
    </div>`
  ).join('');
}}

// -----------------------------------------------------------------------
// CORRIDORS
// -----------------------------------------------------------------------
function renderCorridors() {{
  if(!PATTERNS) {{
    document.getElementById('corridor-cards').innerHTML = '<div class="empty">No pattern data.</div>';
    return;
  }}
  document.getElementById('corridor-cards').innerHTML =
    PATTERNS.corridors.corridor_hits.map(c => {{
      const ar = c.age_range||{{}};
      return `
      <div class="corridor-card" onclick="this.querySelector('.p-card-members').classList.toggle('open')">
        <span class="corridor-count">${{c.count}}</span>
        <div class="corridor-label">${{c.label}}</div>
        ${{ar.min!=null ? `<div style="font-size:12px;color:var(--muted)">Ages ${{ar.min}}–${{ar.max}}</div>` : ''}}
        <div class="p-card-members" style="margin-top:12px;padding-top:12px;border-top:1px solid var(--border)">
          ${{c.members.slice(0,8).map(m=>`
            <div class="member-row">
              ${{m.photo ? `<img class="member-photo" src="${{m.photo}}" onerror="this.style.display='none'" loading="lazy">` : '<div class="member-photo" style="font-size:10px;display:flex;align-items:center;justify-content:center;color:var(--muted)">👤</div>'}}
              <div class="member-info">
                <div class="member-name">${{m.name||'—'}}</div>
                <div class="member-detail">${{m.nationality}} → ${{m.country}} · age=${{m.age??'?'}}</div>
              </div>
              ${{m.source_url ? `<a href="${{m.source_url}}" target="_blank" style="color:var(--accent);font-size:18px;text-decoration:none">↗</a>` : ''}}
            </div>`).join('')}}
          ${{c.count>8?`<div style="font-family:var(--mono);font-size:11px;color:var(--muted);padding-top:8px">+${{c.count-8}} more</div>`:''}}
        </div>
      </div>`;
    }}).join('') || '<div class="empty">No corridor matches found. Requires nationality data (Interpol source).</div>';

  document.getElementById('flow-tbody').innerHTML =
    PATTERNS.corridors.flow_matrix.map(f=>
      `<tr><td>${{f.from}}</td><td>${{f.to}}</td>
       <td><span style="font-family:var(--mono)">${{f.count}}</span></td></tr>`
    ).join('');
}}

// -----------------------------------------------------------------------
// SYSTEM
// -----------------------------------------------------------------------
function renderSystem() {{
  document.getElementById('sources-tbody').innerHTML =
    CORE.sources.map(s=>
      `<tr>
        <td><span class="badge">${{s.source}}</span></td>
        <td style="font-family:var(--mono)">${{s.total.toLocaleString()}}</td>
        <td style="font-family:var(--mono)">${{s.active.toLocaleString()}}</td>
        <td style="font-family:var(--mono)">${{s.resolved.toLocaleString()}}</td>
        <td style="font-family:var(--mono)">${{s.photos.toLocaleString()}}</td>
      </tr>`
    ).join('');

  document.getElementById('scraper-grid').innerHTML =
    CORE.scraper_runs.map(r=>
      `<div class="scraper-card">
        <div class="scraper-name">${{r.name}}</div>
        <div class="scraper-stat"><span>Last run</span><span style="font-family:var(--mono);font-size:11px">${{r.started}}</span></div>
        <div class="scraper-stat"><span>Status</span><span class="badge ${{r.status==='success'?'green':r.status==='failed'?'red':''}}">${{r.status}}</span></div>
        <div class="scraper-stat"><span>Found</span><span style="font-family:var(--mono)">${{r.found}}</span></div>
        <div class="scraper-stat"><span>New</span><span style="font-family:var(--mono);color:var(--accent3)">${{r.new}}</span></div>
        <div class="scraper-stat"><span>Errors</span><span style="font-family:var(--mono);color:${{r.errors?'var(--accent)':'var(--muted)'}}">${{r.errors}}</span></div>
      </div>`
    ).join('') || '<div class="empty">No scraper run history found.</div>';
}}

// -----------------------------------------------------------------------
// CHARTS
// -----------------------------------------------------------------------
function renderCharts() {{
  const s = CORE.sources.sort((a,b)=>b.total-a.total);
  makeChart('chart-sources','bar',s.map(x=>x.source),s.map(x=>x.total),'Cases');

  const g = CORE.gender.filter(x=>x.gender&&x.count);
  makeChart('chart-gender','doughnut',g.map(x=>x.gender),g.map(x=>x.count),'Gender');

  const ag = CORE.age_groups;
  makeChart('chart-ages','bar',Object.keys(ag),Object.values(ag),'Children');

  const yr = CORE.yearly;
  makeChart('chart-years','line',Object.keys(yr),Object.values(yr),'Cases');

  const dur = CORE.durations;
  makeChart('chart-durations','bar',Object.keys(dur),Object.values(dur),'Cases');

  const top = CORE.countries.slice(0,12);
  makeChart('chart-countries','bar',top.map(x=>x.country),top.map(x=>x.count),'Cases');
}}

// -----------------------------------------------------------------------
// BOOT
// -----------------------------------------------------------------------
document.addEventListener('DOMContentLoaded', () => {{
  renderKPIs();
  initMap();
  renderCharts();
  initCases();
  renderClusters();
  renderBursts();
  renderSpikes();
  renderTargeting();
  renderCorridors();
  renderSystem();
}});
</script>
</body>
</html>"""
    return html


# -----------------------------------------------------------------------
# ENTRY POINT
# -----------------------------------------------------------------------

def run_report(db_path=DB_PATH, skip_patterns=False, out_path=None):
    engine, Session = init_db(db_path)
    db = Session()
    print("Collecting core statistics...")
    core = collect_core(db)
    db.close()

    patterns = None
    if not skip_patterns:
        try:
            from analysis.patterns import run_analysis
            print("Running pattern analysis (this may take 20-30s)...")
            patterns = run_analysis(db_path)
        except ImportError:
            try:
                sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'analysis'))
                from patterns import run_analysis
                print("Running pattern analysis...")
                patterns = run_analysis(db_path)
            except Exception as e:
                print(f"Pattern analysis unavailable: {e}")
        except Exception as e:
            print(f"Pattern analysis error: {e}")

    print("Building dashboard HTML...")
    html = build_dashboard(core, patterns)

    out = out_path or "output/dashboard.html"
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Dashboard → {out}  ({round(os.path.getsize(out)/1024)}KB)")
    return out


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Missing Children Dashboard")
    parser.add_argument("--db",            default=DB_PATH)
    parser.add_argument("--out",           default="output/dashboard.html")
    parser.add_argument("--skip-patterns", action="store_true")
    args = parser.parse_args()
    run_report(args.db, args.skip_patterns, args.out)
