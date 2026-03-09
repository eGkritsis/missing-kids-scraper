import json
import statistics
from pathlib import Path
from collections import defaultdict
from datetime import datetime

from sqlalchemy import func
from database.models import init_db, MissingPerson

DB_PATH = "missing_children.db"


COUNTRY_COORDS = {
"United States":[37.0902,-95.7129],
"Canada":[56.1304,-106.3468],
"United Kingdom":[55.3781,-3.4360],
"France":[46.2276,2.2137],
"Germany":[51.1657,10.4515],
"Spain":[40.4637,-3.7492],
"Italy":[41.8719,12.5674],
"Turkey":[38.9637,35.2433],
"India":[20.5937,78.9629],
"Pakistan":[30.3753,69.3451],
"Nigeria":[9.0820,8.6753],
"South Africa":[-30.5595,22.9375],
"Australia":[-25.2744,133.7751],
"Brazil":[-14.2350,-51.9253],
"Mexico":[23.6345,-102.5528],
"Argentina":[-38.4161,-63.6167],
"Japan":[36.2048,138.2529],
"China":[35.8617,104.1954],
"Russia":[61.5240,105.3188]
}


# ------------------------------------------------
# DATA COLLECTION
# ------------------------------------------------

def collect_data(db):

    total = db.query(MissingPerson).count()

    active = db.query(MissingPerson).filter(
        MissingPerson.is_resolved == False
    ).count()

    resolved = db.query(MissingPerson).filter(
        MissingPerson.is_resolved == True
    ).count()

    countries_raw = db.query(
        MissingPerson.country_last_seen,
        func.count(MissingPerson.id)
    ).group_by(
        MissingPerson.country_last_seen
    ).all()

    sources_raw = db.query(
        MissingPerson.source,
        func.count(MissingPerson.id)
    ).group_by(
        MissingPerson.source
    ).all()

    gender_raw = db.query(
        MissingPerson.gender,
        func.count(MissingPerson.id)
    ).group_by(
        MissingPerson.gender
    ).all()

    cases = db.query(MissingPerson).all()

    age_groups = defaultdict(int)
    yearly = defaultdict(int)
    monthly = defaultdict(int)
    duration_groups = defaultdict(int)

    ages = []

    now = datetime.utcnow().date()

    photo_count = 0
    age_count = 0
    gender_count = 0
    date_count = 0

    for c in cases:

        if c.photo_url:
            photo_count += 1

        if c.age_at_disappearance:
            ages.append(c.age_at_disappearance)
            age_count += 1

        if c.gender:
            gender_count += 1

        if c.date_missing:
            date_count += 1

        if c.age_at_disappearance:

            a = c.age_at_disappearance

            if a <=5:
                age_groups["0-5"]+=1
            elif a<=10:
                age_groups["6-10"]+=1
            elif a<=13:
                age_groups["11-13"]+=1
            elif a<=15:
                age_groups["14-15"]+=1
            elif a<=17:
                age_groups["16-17"]+=1

        if c.date_missing:

            y = str(c.date_missing.year)
            m = f"{c.date_missing.year}-{c.date_missing.month:02d}"

            yearly[y]+=1
            monthly[m]+=1

            days = (now - c.date_missing).days

            if days < 30:
                duration_groups["<1 month"]+=1
            elif days < 180:
                duration_groups["1-6 months"]+=1
            elif days < 365:
                duration_groups["6-12 months"]+=1
            elif days < 1095:
                duration_groups["1-3 years"]+=1
            else:
                duration_groups["3+ years"]+=1


    age_stats = {}

    if ages:
        age_stats = {
            "mean": round(statistics.mean(ages),1),
            "median": statistics.median(ages),
            "min": min(ages),
            "max": max(ages)
        }


    return {

        "summary":{
            "total":total,
            "active":active,
            "resolved":resolved,
            "resolution_rate": round(resolved/total*100,2) if total else 0
        },

        "countries":[
            {"country":c or "Unknown","count":n}
            for c,n in countries_raw
        ],

        "sources":[
            {"source":s or "Unknown","count":n}
            for s,n in sources_raw
        ],

        "gender":[
            {"gender":g or "Unknown","count":n}
            for g,n in gender_raw
        ],

        "ages":age_groups,
        "years":yearly,
        "months":monthly,
        "durations":duration_groups,
        "age_stats":age_stats,

        "cases":[
            {
                "name":c.full_name,
                "age":c.age_at_disappearance,
                "gender":c.gender,
                "country":c.country_last_seen,
                "date":str(c.date_missing),
                "source":c.source,
                "photo":c.photo_url
            }
            for c in cases
        ]

    }


# ------------------------------------------------
# HTML DASHBOARD
# ------------------------------------------------

def build_html(data):

    return f"""
<!DOCTYPE html>
<html>

<head>

<meta charset="utf-8">

<title>Missing Children Dashboard</title>

<link rel="stylesheet"
href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>

<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>

<style>

body {{
background:#0f172a;
color:white;
font-family:Arial;
margin:0;
}}

header {{
padding:20px;
font-size:28px;
font-weight:bold;
}}

.stats {{
display:flex;
gap:20px;
padding:20px;
}}

.stat {{
background:#1e293b;
padding:15px;
border-radius:10px;
flex:1;
text-align:center;
}}

#map {{
height:450px;
margin:20px;
border-radius:10px;
}}

.grid {{
display:grid;
grid-template-columns:repeat(3,1fr);
gap:20px;
padding:20px;
}}

.chartBox {{
background:#1e293b;
padding:10px;
border-radius:10px;
}}

table {{
width:100%;
border-collapse:collapse;
}}

td,th {{
padding:8px;
border-bottom:1px solid #333;
}}

.countryMarker {{
text-align:center;
color:white;
font-weight:bold;
}}

.countryMarker .badge {{
background:#ef4444;
border-radius:20px;
padding:6px 10px;
display:inline-block;
font-size:14px;
margin-bottom:3px;
box-shadow:0 2px 6px rgba(0,0,0,0.6);
}}

.countryMarker span{{
font-size:11px;
}}

</style>

</head>

<body>

<header>🌍 Missing Children Intelligence Dashboard</header>

<div class="stats">

<div class="stat">
<h2>{data["summary"]["total"]}</h2>Total
</div>

<div class="stat">
<h2>{data["summary"]["active"]}</h2>Active
</div>

<div class="stat">
<h2>{data["summary"]["resolved"]}</h2>Resolved
</div>

<div class="stat">
<h2>{data["summary"]["resolution_rate"]}%</h2>Resolution Rate
</div>

</div>

<div id="map"></div>

<div class="grid">

<div class="chartBox"><canvas id="sources"></canvas></div>
<div class="chartBox"><canvas id="gender"></canvas></div>
<div class="chartBox"><canvas id="ages"></canvas></div>
<div class="chartBox"><canvas id="years"></canvas></div>
<div class="chartBox"><canvas id="months"></canvas></div>
<div class="chartBox"><canvas id="durations"></canvas></div>

</div>

<script>

const DATA = {json.dumps(data)}
const COUNTRY_COORDS = {json.dumps(COUNTRY_COORDS)}

var map = L.map("map").setView([20,0],2)

L.tileLayer("https://tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png").addTo(map)

let markers = L.layerGroup().addTo(map)

function updateMap(){{

markers.clearLayers()

DATA.countries.forEach(c=>{{

let coords = COUNTRY_COORDS[c.country]

if(!coords) return

let count = c.count

let icon = L.divIcon({{
className: "countryMarker",
html: `<div class="badge">${{count}}</div><span>${{c.country}}</span>`,
iconSize:[60,40]
}})

let m = L.marker(coords,{{icon}})
.bindPopup(`<b>${{c.country}}</b><br>${{count}} cases`)

markers.addLayer(m)

}})

}}

updateMap()


function makeChart(id,type,labels,data,label){{

new Chart(document.getElementById(id),{{
type:type,
data:{{labels:labels,datasets:[{{label:label,data:data}}]}}
}})

}}

makeChart(
"sources",
"bar",
DATA.sources.map(x=>x.source),
DATA.sources.map(x=>x.count),
"Cases by Source"
)

makeChart(
"gender",
"pie",
DATA.gender.map(x=>x.gender),
DATA.gender.map(x=>x.count),
"Gender"
)

makeChart(
"ages",
"bar",
Object.keys(DATA.ages),
Object.values(DATA.ages),
"Age Distribution"
)

makeChart(
"years",
"line",
Object.keys(DATA.years),
Object.values(DATA.years),
"Cases per Year"
)

makeChart(
"months",
"line",
Object.keys(DATA.months),
Object.values(DATA.months),
"Cases per Month"
)

makeChart(
"durations",
"bar",
Object.keys(DATA.durations),
Object.values(DATA.durations),
"Missing Duration"
)

</script>

</body>
</html>
"""


# ------------------------------------------------
# RUN REPORT
# ------------------------------------------------

def run_report():

    engine,Session = init_db(DB_PATH)
    db = Session()

    data = collect_data(db)

    html = build_html(data)

    Path("output").mkdir(exist_ok=True)

    out="output/dashboard.html"

    with open(out,"w",encoding="utf-8") as f:
        f.write(html)

    print("Dashboard generated:",out)