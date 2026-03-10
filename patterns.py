"""
analysis/patterns.py
====================
Pattern analysis engine for the Missing Children Tracker.

Analyses:
  1. SURNAME CLUSTERS      — siblings / family units / cross-border
  2. SPATIO-TEMPORAL       — disappearance bursts by location + date
  3. CORRIDOR DETECTION    — nationality → destination vs known trafficking routes
  4. DEMOGRAPHIC TARGETING — age/gender concentration by country
  5. TIMELINE ANOMALIES    — statistical spikes in disappearances

Noise filters applied:
  - Burst clusters with span_days==0 and count>10 → data import artifact
  - Surname cross-border clusters where >80% members have missing=None → no signal
  - Common Anglo-Caribbean surnames (Williams, Smith, etc.) only flagged
    if they have real date proximity OR young children (<8) present
  - Age=-1 (calculation artifact from future DOB) treated as unknown

Usage:
  python analysis/patterns.py
  python analysis/patterns.py --db path/to/db --min-cluster 2 --date-window 180
"""

import argparse
import json
import sys
import os
from collections import defaultdict, Counter
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.models import init_db, MissingPerson

# ---------------------------------------------------------------------------
# Known trafficking corridors (UNODC / IOM / Europol sourced)
# ---------------------------------------------------------------------------
KNOWN_CORRIDORS = [
    ({"Guatemala","Honduras","El Salvador","Nicaragua","Mexico"},
     {"United States","USA"},
     "Central America → US (documented smuggling/trafficking route)"),

    ({"Guatemala","Honduras","El Salvador"},
     {"Mexico"},
     "Central America → Mexico (transit route)"),

    ({"Ecuador","Colombia","Peru","Bolivia"},
     {"United States","USA","Spain","Chile"},
     "Andean → North America / Europe"),

    ({"Ukraine","Moldova","Belarus","Romania","Bulgaria"},
     {"Germany","Poland","Netherlands","Italy","France","Spain","United Kingdom"},
     "Eastern Europe → Western Europe (conflict/economic exploitation)"),

    ({"Nigeria","Ghana","Cameroon"},
     {"Italy","France","Spain","United Kingdom","Libya"},
     "West Africa → Europe (Mediterranean route)"),

    ({"Philippines","Thailand","Vietnam","Cambodia","Myanmar"},
     {"China","Malaysia","Japan","South Korea","Australia"},
     "Southeast Asia → East/Pacific Asia"),

    ({"India","Nepal","Bangladesh"},
     {"United Arab Emirates","Saudi Arabia","Kuwait","Qatar"},
     "South Asia → Gulf States"),

    ({"South Africa"},
     {"South Africa"},
     "South Africa internal trafficking (high-volume domestic)"),

    ({"Jamaica"},
     {"United Kingdom","United States","Canada"},
     "Jamaica → Anglo diaspora destinations"),

    ({"Russian Federation","Russia"},
     {"Germany","Finland","Norway","Sweden","Estonia"},
     "Russia → Northern/Western Europe"),
]

# Very common Anglo-Caribbean surnames that produce false cross-border clusters
COMMON_SURNAMES = {
    "WILLIAMS","SMITH","JOHNSON","JONES","BROWN","DAVIS","WILSON",
    "TAYLOR","THOMAS","MOORE","JACKSON","MARTIN","LEWIS","LEE",
    "THOMPSON","WHITE","HARRIS","CLARK","ROBINSON","WALKER","HALL",
    "CAMPBELL","ALLEN","YOUNG","KING","SCOTT","BAKER","NELSON",
}

COUNTRY_NORM = {
    "USA":"United States","US":"United States","U.S.":"United States",
    "United States of America":"United States","UK":"United Kingdom",
    "Great Britain":"United Kingdom","Russia":"Russian Federation",
    "Korea":"South Korea","Republic of Korea":"South Korea",
}

def norm_country(c):
    if not c: return None
    return COUNTRY_NORM.get(c.strip(), c.strip())

def effective_age(p):
    """Age at disappearance; None if unknown; skips negative (DOB artifact)."""
    if p.age_at_disappearance and p.age_at_disappearance >= 0:
        return p.age_at_disappearance
    if p.date_of_birth and p.date_missing:
        try:
            a = p.date_missing.year - p.date_of_birth.year - (
                (p.date_missing.month, p.date_missing.day) <
                (p.date_of_birth.month, p.date_of_birth.day)
            )
            return a if a >= 0 else None
        except Exception:
            pass
    if p.date_of_birth:
        try:
            today = date.today()
            a = today.year - p.date_of_birth.year - (
                (today.month, today.day) <
                (p.date_of_birth.month, p.date_of_birth.day)
            )
            return a if 0 <= a <= 17 else None
        except Exception:
            pass
    return None

def member_dict(m, extra=None):
    d = {
        "id":          m.id,
        "name":        m.full_name,
        "age":         effective_age(m),
        "gender":      m.gender,
        "dob":         str(m.date_of_birth) if m.date_of_birth else None,
        "date_missing":str(m.date_missing) if m.date_missing else None,
        "city":        m.city_last_seen,
        "state":       m.state_last_seen,
        "country":     norm_country(m.country_last_seen),
        "nationality": m.nationality,
        "source":      m.source,
        "source_url":  m.source_url,
        "photo":       m.photo_url,
    }
    if extra:
        d.update(extra)
    return d

def priority_score(flag, count, ages):
    score = count * 10
    if flag == "FAMILY_GROUP":  score += 30
    if flag == "CROSS_BORDER":  score += 20
    if flag == "SIBLING_UNIT":  score += 15
    if ages:
        score += sum(5 for a in ages if a < 12)
        score += sum(3 for a in ages if a < 6)
    return score


# ---------------------------------------------------------------------------
# 1. SURNAME CLUSTERS
# ---------------------------------------------------------------------------

def analyse_surname_clusters(cases, min_cluster=2, date_window_days=180):
    by_surname = defaultdict(list)
    for p in cases:
        if not p.last_name:
            continue
        by_surname[p.last_name.strip().upper()].append(p)

    clusters = []

    for surname, members in by_surname.items():
        if len(members) < min_cluster:
            continue

        is_common = surname in COMMON_SURNAMES

        # --- Within-country clusters ---
        by_country = defaultdict(list)
        for m in members:
            c = norm_country(m.country_last_seen) or "Unknown"
            by_country[c].append(m)

        for country, group in by_country.items():
            if len(group) < min_cluster:
                continue

            dated   = sorted([m for m in group if m.date_missing], key=lambda x: x.date_missing)
            undated = [m for m in group if not m.date_missing]

            # For common surnames, only flag if there's real date proximity
            if is_common:
                if not dated:
                    continue

            # Sliding date window
            i = 0
            while i < len(dated):
                window = [dated[i]]
                j = i + 1
                while j < len(dated):
                    if (dated[j].date_missing - dated[i].date_missing).days <= date_window_days:
                        window.append(dated[j])
                        j += 1
                    else:
                        break

                all_m = window + (undated if not is_common else [])
                if len(all_m) >= min_cluster:
                    ages = [a for a in (effective_age(m) for m in all_m) if a is not None]
                    flag = "SIBLING_UNIT" if len(all_m) <= 5 else "FAMILY_GROUP"
                    dr = dated[i:j]
                    clusters.append({
                        "type":       flag,
                        "surname":    surname,
                        "country":    country,
                        "count":      len(all_m),
                        "members":    [member_dict(m) for m in all_m],
                        "date_range": {
                            "from":      str(dr[0].date_missing),
                            "to":        str(dr[-1].date_missing),
                            "span_days": (dr[-1].date_missing - dr[0].date_missing).days
                                         if len(dr) >= 2 else 0,
                        } if dr else None,
                        "age_range": {
                            "min": min(ages) if ages else None,
                            "max": max(ages) if ages else None,
                        },
                        "priority": priority_score(flag, len(all_m), ages),
                    })
                i += 1

        # --- Cross-border clusters ---
        countries_with_members = {
            c: g for c, g in by_country.items() if len(g) >= 1
        }
        if len(countries_with_members) >= 2:
            all_cross = [m for g in countries_with_members.values() for m in g]

            # Noise filter: skip if >80% have no date AND surname is common
            no_date_pct = sum(1 for m in all_cross if not m.date_missing) / len(all_cross)
            if is_common and no_date_pct > 0.80:
                continue

            ages = [a for a in (effective_age(m) for m in all_cross) if a is not None]

            clusters.append({
                "type":    "CROSS_BORDER",
                "surname": surname,
                "country": " / ".join(sorted(countries_with_members.keys())),
                "count":   len(all_cross),
                "members": [member_dict(m) for m in all_cross],
                "age_range": {
                    "min": min(ages) if ages else None,
                    "max": max(ages) if ages else None,
                },
                "priority": priority_score("CROSS_BORDER", len(all_cross), ages),
            })

    clusters.sort(key=lambda x: x["priority"], reverse=True)
    return clusters


# ---------------------------------------------------------------------------
# 2. SPATIO-TEMPORAL CLUSTERS
# ---------------------------------------------------------------------------

def analyse_spatiotemporal(cases, date_window_days=30, min_cluster=3):
    by_location = defaultdict(list)
    for p in cases:
        if not p.date_missing:
            continue
        city    = (p.city_last_seen or "").strip().upper()
        country = norm_country(p.country_last_seen) or "Unknown"
        by_location[(city, country)].append(p)

    bursts = []
    seen_keys = set()

    for (city, country), group in by_location.items():
        if len(group) < min_cluster:
            continue

        group.sort(key=lambda x: x.date_missing)

        i = 0
        while i < len(group):
            window = [group[i]]
            j = i + 1
            while j < len(group):
                if (group[j].date_missing - group[i].date_missing).days <= date_window_days:
                    window.append(group[j])
                    j += 1
                else:
                    break

            if len(window) >= min_cluster:
                span = (window[-1].date_missing - window[0].date_missing).days

                # NOISE FILTER: span==0 with large count = bulk import artifact
                if span == 0 and len(window) > 8:
                    i += 1
                    continue

                # Deduplicate: keep best window per location
                loc_key = (city, country)
                if loc_key in seen_keys:
                    i += 1
                    continue
                seen_keys.add(loc_key)

                ages    = [a for a in (effective_age(m) for m in window) if a is not None]
                genders = Counter(m.gender for m in window if m.gender)

                bursts.append({
                    "type":    "SPATIO_TEMPORAL_BURST",
                    "city":    city.title() if city else "Unknown",
                    "country": country,
                    "count":   len(window),
                    "date_range": {
                        "from":      str(window[0].date_missing),
                        "to":        str(window[-1].date_missing),
                        "span_days": span,
                    },
                    "age_range": {
                        "min":  min(ages) if ages else None,
                        "max":  max(ages) if ages else None,
                        "mean": round(sum(ages)/len(ages), 1) if ages else None,
                    },
                    "gender_breakdown": dict(genders),
                    "members":  [member_dict(m) for m in window],
                    "priority": len(window) * 8 + (30 if ages and min(ages) < 12 else 0)
                                + (20 if span <= 14 else 0),
                    "is_active": window[-1].date_missing >= (date.today() - timedelta(days=90)),
                })

            i += 1

    bursts.sort(key=lambda x: x["priority"], reverse=True)
    return bursts


# ---------------------------------------------------------------------------
# 3. CORRIDOR DETECTION
# ---------------------------------------------------------------------------

def analyse_corridors(cases):
    flow_matrix   = defaultdict(lambda: defaultdict(int))
    corridor_hits = []

    nat_cases = [p for p in cases if p.nationality and p.country_last_seen]

    for p in nat_cases:
        nats = [norm_country(n.strip()) or n.strip() for n in p.nationality.split(",")]
        dest = norm_country(p.country_last_seen)
        for nat in nats:
            if nat != dest:
                flow_matrix[nat][dest] += 1

    for origins, dests, label in KNOWN_CORRIDORS:
        matching = []
        for p in nat_cases:
            nats = {norm_country(n.strip()) for n in p.nationality.split(",")}
            dest = norm_country(p.country_last_seen)
            if nats & origins and dest in dests:
                matching.append(p)

        if matching:
            ages = [a for a in (effective_age(m) for m in matching) if a is not None]
            corridor_hits.append({
                "label":    label,
                "count":    len(matching),
                "age_range":{"min": min(ages) if ages else None,
                              "max": max(ages) if ages else None},
                "members":  [member_dict(m) for m in matching],
            })

    corridor_hits.sort(key=lambda x: x["count"], reverse=True)

    flows = [
        {"from": o, "to": d, "count": c}
        for o, dests in flow_matrix.items()
        for d, c in dests.items()
    ]
    flows.sort(key=lambda x: x["count"], reverse=True)

    return {"corridor_hits": corridor_hits, "flow_matrix": flows[:40]}


# ---------------------------------------------------------------------------
# 4. DEMOGRAPHIC TARGETING
# ---------------------------------------------------------------------------

def analyse_targeting(cases):
    by_country = defaultdict(list)
    for p in cases:
        c = norm_country(p.country_last_seen)
        if c:
            by_country[c].append(p)

    targeting = []
    for country, group in by_country.items():
        if len(group) < 5:
            continue

        ages        = [a for a in (effective_age(m) for m in group) if a is not None]
        genders     = Counter(m.gender for m in group if m.gender)
        age_mean    = sum(ages)/len(ages) if ages else None
        age_under12 = sum(1 for a in ages if a < 12)
        age_teen    = sum(1 for a in ages if 12 <= a <= 17)

        flags = []
        if ages:
            if len(ages) >= 5 and age_under12 / len(ages) > 0.4:
                flags.append("HIGH_PROPORTION_YOUNG_CHILDREN")
            if len(ages) >= 5 and age_teen / len(ages) > 0.70:
                flags.append("PREDOMINANTLY_TEEN")
            if age_mean and age_mean < 10:
                flags.append("VERY_YOUNG_MEAN_AGE")

        female = genders.get("Female",0) + genders.get("F",0)
        male   = genders.get("Male",0) + genders.get("M",0)
        total_g = female + male
        if total_g >= 5:
            if female/total_g > 0.75:
                flags.append("PREDOMINANTLY_FEMALE")
            elif male/total_g > 0.75:
                flags.append("PREDOMINANTLY_MALE")

        targeting.append({
            "country":         country,
            "total_cases":     len(group),
            "age_mean":        round(age_mean,1) if age_mean else None,
            "age_under_12":    age_under12,
            "age_teen":        age_teen,
            "age_unknown":     len(group) - len(ages),
            "gender_breakdown":dict(genders),
            "flags":           flags,
        })

    targeting.sort(key=lambda x: len(x["flags"])*100 + x["total_cases"], reverse=True)
    return targeting


# ---------------------------------------------------------------------------
# 5. TIMELINE ANOMALIES
# ---------------------------------------------------------------------------

def analyse_timeline(cases):
    monthly  = defaultdict(lambda: defaultdict(int))
    overall  = defaultdict(int)

    for p in cases:
        if not p.date_missing:
            continue
        month   = p.date_missing.strftime("%Y-%m")
        country = norm_country(p.country_last_seen) or "Unknown"
        monthly[country][month] += 1
        overall[month] += 1

    anomalies = []
    for country, months in monthly.items():
        if len(months) < 4:
            continue
        counts = list(months.values())
        mean   = sum(counts)/len(counts)
        if len(counts) < 2:
            continue
        stdev  = (sum((c-mean)**2 for c in counts)/len(counts))**0.5
        if stdev == 0:
            continue
        threshold = mean + 2*stdev
        for month, count in months.items():
            if count > threshold and count >= 3:
                # Skip known import artifacts (Jamaica Nov 2018)
                if count > 100 and stdev > 20:
                    continue
                anomalies.append({
                    "country": country,
                    "month":   month,
                    "count":   count,
                    "mean":    round(mean,1),
                    "stdev":   round(stdev,1),
                    "z_score": round((count-mean)/stdev, 2),
                    "is_active": month >= (date.today() - timedelta(days=90)).strftime("%Y-%m"),
                })

    anomalies.sort(key=lambda x: x["z_score"], reverse=True)

    return {
        "spikes":            anomalies[:30],
        "monthly_overall":   dict(sorted(overall.items())),
        "monthly_by_country":{
            c: dict(sorted(m.items()))
            for c,m in monthly.items()
        },
    }


# ---------------------------------------------------------------------------
# TEXT REPORT
# ---------------------------------------------------------------------------

def build_text_report(results, args):
    lines = []
    w = lines.append
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    w("="*72)
    w("  MISSING CHILDREN — PATTERN ANALYSIS REPORT")
    w(f"  Generated : {now}")
    w(f"  Database  : {args.db}")
    w(f"  Parameters: min_cluster={args.min_cluster}  date_window={args.date_window}d")
    w("="*72)
    w("")

    # 1. Surname
    sc = results["surname_clusters"]
    sib   = [c for c in sc if c["type"]=="SIBLING_UNIT"]
    fam   = [c for c in sc if c["type"]=="FAMILY_GROUP"]
    cross = [c for c in sc if c["type"]=="CROSS_BORDER"]
    w(f"1. SURNAME CLUSTERS  ({len(sc)} total | {len(sib)} sibling-units | {len(fam)} family-groups | {len(cross)} cross-border)")
    w("-"*72)
    for c in sc[:25]:
        ar = c.get("age_range",{})
        dr = c.get("date_range") or {}
        w(f"  [{c['type']}]  {c['surname']}  ×{c['count']}  —  {c['country']}")
        if ar.get("min") is not None:
            w(f"    Ages: {ar['min']}–{ar['max']}")
        if dr.get("from"):
            w(f"    Dates: {dr['from']} → {dr['to']}  ({dr.get('span_days',0)}d span)")
        for m in c["members"][:5]:
            w(f"    • {m['name']}  age={m.get('age','?')}  missing={m.get('date_missing','?')}  {m.get('city','?')}")
        w("")

    # 2. Spatio-temporal
    st = results["spatiotemporal"]
    active_bursts = [b for b in st if b.get("is_active")]
    w(f"2. SPATIO-TEMPORAL BURSTS  ({len(st)} total | {len(active_bursts)} ACTIVE last 90d)")
    w("-"*72)
    for b in st[:20]:
        dr  = b["date_range"]
        ar  = b.get("age_range",{})
        tag = "*** ACTIVE ***" if b.get("is_active") else ""
        w(f"  {b['city']}, {b['country']}  ×{b['count']}  in {dr['span_days']}d  {tag}")
        w(f"    Period: {dr['from']} → {dr['to']}")
        if ar.get("mean"):
            w(f"    Ages: {ar['min']}–{ar['max']}  mean={ar['mean']}")
        if b["gender_breakdown"]:
            w(f"    Gender: {b['gender_breakdown']}")
        for m in b["members"][:5]:
            w(f"    • {m['name']}  age={m.get('age','?')}  {m['date_missing']}")
        w("")

    # 3. Corridors
    corr = results["corridors"]
    w(f"3. TRAFFICKING CORRIDORS  ({len(corr['corridor_hits'])} active)")
    w("-"*72)
    for c in corr["corridor_hits"]:
        ar = c.get("age_range",{})
        w(f"  {c['label']}")
        w(f"  → {c['count']} cases")
        if ar.get("min") is not None:
            w(f"    Ages: {ar['min']}–{ar['max']}")
        for m in c["members"][:4]:
            w(f"    • {m['name']}  nat={m['nationality']}  → {m['country']}  age={m.get('age','?')}")
        w("")
    w("  FLOW MATRIX (top origin→destination pairs):")
    for f in corr["flow_matrix"][:10]:
        w(f"    {f['from']:28s} → {f['to']:28s}  ({f['count']})")
    w("")

    # 4. Targeting
    tgt = [t for t in results["targeting"] if t["flags"]]
    w(f"4. DEMOGRAPHIC FLAGS  ({len(tgt)} countries flagged)")
    w("-"*72)
    for t in tgt[:15]:
        w(f"  {t['country']}  ×{t['total_cases']}  — {', '.join(t['flags'])}")
        if t["age_mean"]:
            w(f"    Mean age={t['age_mean']}  Under-12={t['age_under_12']}  Teen={t['age_teen']}")
        if t["gender_breakdown"]:
            w(f"    Gender: {t['gender_breakdown']}")
        w("")

    # 5. Timeline
    spikes = results["timeline"]["spikes"]
    active_spikes = [s for s in spikes if s.get("is_active")]
    w(f"5. TIMELINE ANOMALIES  ({len(spikes)} spikes | {len(active_spikes)} recent)")
    w("-"*72)
    for s in spikes[:20]:
        tag = "ACTIVE" if s.get("is_active") else ""
        w(f"  {s['month']}  {s['country']:32s}  ×{s['count']}  z={s['z_score']}  {tag}")
    w("")
    w("="*72)
    w("END OF REPORT")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def run_analysis(db_path="missing_children.db", min_cluster=2, date_window=180):
    engine, Session = init_db(db_path)
    db = Session()
    print(f"Loading active cases from {db_path}...")
    cases = db.query(MissingPerson).filter(MissingPerson.is_resolved==False).all()
    db.close()
    print(f"Loaded {len(cases)} cases.")

    print("  [1/5] Surname clusters...")
    surname_clusters = analyse_surname_clusters(cases, min_cluster, date_window)
    print(f"        → {len(surname_clusters)} clusters")

    print("  [2/5] Spatio-temporal bursts...")
    spatiotemporal = analyse_spatiotemporal(cases, 30, min_cluster)
    print(f"        → {len(spatiotemporal)} bursts")

    print("  [3/5] Corridor detection...")
    corridors = analyse_corridors(cases)
    print(f"        → {len(corridors['corridor_hits'])} corridors active")

    print("  [4/5] Demographic targeting...")
    targeting = analyse_targeting(cases)
    flagged   = sum(1 for t in targeting if t["flags"])
    print(f"        → {flagged} countries flagged")

    print("  [5/5] Timeline anomalies...")
    timeline = analyse_timeline(cases)
    print(f"        → {len(timeline['spikes'])} spikes detected")

    summary = {
        "surname_cluster_count":  len(surname_clusters),
        "sibling_units":          sum(1 for c in surname_clusters if c["type"]=="SIBLING_UNIT"),
        "family_groups":          sum(1 for c in surname_clusters if c["type"]=="FAMILY_GROUP"),
        "cross_border_clusters":  sum(1 for c in surname_clusters if c["type"]=="CROSS_BORDER"),
        "spatiotemporal_bursts":  len(spatiotemporal),
        "active_bursts":          sum(1 for b in spatiotemporal if b.get("is_active")),
        "active_corridors":       len(corridors["corridor_hits"]),
        "flagged_countries":      flagged,
        "timeline_spikes":        len(timeline["spikes"]),
        "active_spikes":          sum(1 for s in timeline["spikes"] if s.get("is_active")),
    }

    return {
        "generated_at":     datetime.now().isoformat(),
        "db_path":          db_path,
        "total_cases":      len(cases),
        "summary":          summary,
        "surname_clusters": surname_clusters,
        "spatiotemporal":   spatiotemporal,
        "corridors":        corridors,
        "targeting":        targeting,
        "timeline":         timeline,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Missing Children Pattern Analysis")
    parser.add_argument("--db",          default="missing_children.db")
    parser.add_argument("--min-cluster", type=int, default=2)
    parser.add_argument("--date-window", type=int, default=180)
    parser.add_argument("--out",         default="analysis/output")
    args = parser.parse_args()

    Path(args.out).mkdir(parents=True, exist_ok=True)

    results = run_analysis(args.db, args.min_cluster, args.date_window)

    json_path = f"{args.out}/pattern_report.json"
    with open(json_path,"w",encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nJSON → {json_path}")

    txt_path = f"{args.out}/pattern_report.txt"
    with open(txt_path,"w",encoding="utf-8") as f:
        f.write(build_text_report(results, args))
    print(f"TXT  → {txt_path}")

    s = results["summary"]
    print()
    print("="*52)
    print(f"  Cases analysed           : {results['total_cases']:,}")
    print(f"  Surname clusters         : {s['surname_cluster_count']:,}")
    print(f"    Sibling units          : {s['sibling_units']:,}")
    print(f"    Family groups (6+)     : {s['family_groups']:,}")
    print(f"    Cross-border           : {s['cross_border_clusters']:,}")
    print(f"  Spatio-temporal bursts   : {s['spatiotemporal_bursts']:,}  ({s['active_bursts']} ACTIVE)")
    print(f"  Active corridors         : {s['active_corridors']:,}")
    print(f"  Countries flagged        : {s['flagged_countries']:,}")
    print(f"  Timeline spikes          : {s['timeline_spikes']:,}  ({s['active_spikes']} recent)")
    print("="*52)
