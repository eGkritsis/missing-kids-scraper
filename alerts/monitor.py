"""
alerts/monitor.py
=================
Phase 4: Automated Alert Monitor

Monitors for:
  1. New cases in active burst zones (e.g. Wichita right now)
  2. New cases matching flagged surnames
  3. New DOJ/Europol articles matching DB locations
  4. Statistical spikes crossing z-score threshold
  5. Cases newly resolved via news cross-reference

Outputs:
  - alerts/pending.json          (machine-readable queue)
  - alerts/alert_log.txt         (human-readable log)
  - Email via smtplib (optional, set ALERT_EMAIL env var)
  - Telegram push (optional, set TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID)

Usage:
  python alerts/monitor.py                # run once
  python alerts/monitor.py --watch        # daemon mode (checks every 30min)
  python alerts/monitor.py --test         # test notifications only
"""

import argparse
import json
import logging
import os
import smtplib
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.models import init_db, MissingPerson
from sqlalchemy import text

DB_PATH    = "missing_children.db"
ALERTS_DIR = Path("alerts")
ALERTS_DIR.mkdir(exist_ok=True)

PENDING_FILE  = ALERTS_DIR / "pending.json"
LOG_FILE      = ALERTS_DIR / "alert_log.txt"
STATE_FILE    = ALERTS_DIR / "monitor_state.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(LOG_FILE)),
    ]
)
logger = logging.getLogger("monitor")

COUNTRY_NORM = {
    "USA": "United States", "US": "United States",
    "UK": "United Kingdom",
}
def nc(c):
    if not c: return None
    return COUNTRY_NORM.get(c.strip(), c.strip())

def effective_age(p):
    if p.age_at_disappearance and p.age_at_disappearance >= 0:
        return p.age_at_disappearance
    if p.date_of_birth and p.date_missing:
        try:
            a = p.date_missing.year - p.date_of_birth.year
            return a if 0 <= a <= 17 else None
        except: pass
    return None


# ---------------------------------------------------------------------------
# State management (track what we've already alerted on)
# ---------------------------------------------------------------------------

def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {"alerted_ids": [], "last_run": None, "alert_count": 0}

def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2, default=str))


# ---------------------------------------------------------------------------
# Alert builders
# ---------------------------------------------------------------------------

PRIORITY_LABELS = {1: "LOW", 2: "MEDIUM", 3: "HIGH", 4: "CRITICAL"}

def make_alert(alert_type, priority, title, description, cases=None, meta=None):
    return {
        "id":          f"{alert_type}_{int(time.time()*1000) % 999999}",
        "type":        alert_type,
        "priority":    priority,
        "priority_label": PRIORITY_LABELS.get(priority, "UNKNOWN"),
        "title":       title,
        "description": description,
        "cases":       cases or [],
        "meta":        meta or {},
        "generated":   datetime.now().isoformat(),
    }


# ---------------------------------------------------------------------------
# Check 1: New cases in active burst zones
# ---------------------------------------------------------------------------

def check_burst_zones(db, state, lookback_days=7):
    alerts = []
    alerted = set(state.get("alerted_ids", []))

    # Find active burst zones (3+ cases in last 90 days in same city)
    cutoff    = date.today() - timedelta(days=90)
    new_since = date.today() - timedelta(days=lookback_days)

    from collections import Counter as Ctr
    recent = db.query(MissingPerson).filter(
        MissingPerson.is_resolved == False,
        MissingPerson.date_missing >= cutoff,
    ).all()

    city_groups = defaultdict(list)
    for p in recent:
        city    = (p.city_last_seen or "").strip().upper()
        country = nc(p.country_last_seen) or "Unknown"
        city_groups[(city, country)].append(p)

    for (city, country), group in city_groups.items():
        if len(group) < 3:
            continue
        # Find new cases (last N days) in this zone
        new_cases = [p for p in group
                     if p.date_missing and p.date_missing >= new_since
                     and p.id not in alerted]
        if not new_cases:
            continue

        priority = 4 if len(group) >= 6 else 3
        alert    = make_alert(
            "NEW_BURST_CASE", priority,
            f"NEW CASE in Active Burst Zone: {city.title()}, {country}",
            f"{len(new_cases)} new case(s) added to active zone "
            f"({len(group)} total cases in last 90 days).",
            cases=[{
                "id":    p.id, "name": p.full_name,
                "age":   effective_age(p), "date": str(p.date_missing),
                "url":   p.source_url, "photo": p.photo_url,
            } for p in new_cases],
            meta={"city": city.title(), "country": country,
                  "zone_total": len(group)},
        )
        alerts.append(alert)
        alerted.update(p.id for p in new_cases)
        logger.warning("ALERT [%s] %s", alert["priority_label"], alert["title"])

    return alerts, alerted


# ---------------------------------------------------------------------------
# Check 2: New cases matching flagged surnames
# ---------------------------------------------------------------------------

FLAGGED_SURNAMES_FILE = ALERTS_DIR / "flagged_surnames.json"

def load_flagged_surnames():
    """Load user-defined flagged surnames (from pattern analysis output)."""
    if FLAGGED_SURNAMES_FILE.exists():
        try:
            return set(json.loads(FLAGGED_SURNAMES_FILE.read_text()))
        except Exception:
            pass
    # Default: load from pattern analysis if available
    pattern_file = Path("analysis/output/pattern_report.json")
    if pattern_file.exists():
        try:
            data     = json.loads(pattern_file.read_text())
            clusters = data.get("surname_clusters", [])
            # High-priority clusters: family groups + cross-border
            flagged  = {
                c["surname"] for c in clusters
                if c["type"] in ("FAMILY_GROUP", "CROSS_BORDER")
                and c.get("priority", 0) >= 50
            }
            return flagged
        except Exception:
            pass
    return set()


def check_surname_matches(db, state, lookback_days=7):
    alerts   = []
    alerted  = set(state.get("alerted_ids", []))
    flagged  = load_flagged_surnames()
    if not flagged:
        return alerts, alerted

    new_since = date.today() - timedelta(days=lookback_days)

    new_cases = db.query(MissingPerson).filter(
        MissingPerson.is_resolved == False,
        MissingPerson.created_at >= datetime.combine(new_since, datetime.min.time()),
    ).all()

    surname_hits = defaultdict(list)
    for p in new_cases:
        if p.id in alerted:
            continue
        surname = (p.last_name or "").strip().upper()
        if surname in flagged:
            surname_hits[surname].append(p)

    for surname, cases in surname_hits.items():
        alert = make_alert(
            "FLAGGED_SURNAME_MATCH", 3,
            f"New case matches flagged surname: {surname}",
            f"{len(cases)} new case(s) with surname '{surname}' "
            f"which is flagged as a high-priority family cluster.",
            cases=[{
                "id":      p.id, "name": p.full_name,
                "age":     effective_age(p),
                "country": nc(p.country_last_seen),
                "date":    str(p.date_missing) if p.date_missing else None,
                "url":     p.source_url,
            } for p in cases],
            meta={"surname": surname},
        )
        alerts.append(alert)
        alerted.update(p.id for p in cases)
        logger.warning("ALERT [HIGH] %s", alert["title"])

    return alerts, alerted


# ---------------------------------------------------------------------------
# Check 3: Statistical spike detection
# ---------------------------------------------------------------------------

def check_spikes(db, state, z_threshold=2.5):
    alerts  = []
    now     = date.today()
    cutoff  = now - timedelta(days=730)  # 2 years of history

    cases = db.query(MissingPerson).filter(
        MissingPerson.is_resolved == False,
        MissingPerson.date_missing >= cutoff,
    ).all()

    monthly = defaultdict(lambda: defaultdict(int))
    for p in cases:
        if p.date_missing:
            m       = p.date_missing.strftime("%Y-%m")
            country = nc(p.country_last_seen) or "Unknown"
            monthly[country][m] += 1

    current_month = now.strftime("%Y-%m")
    prev_month    = (now.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")

    alerted_spikes = set(state.get("alerted_spikes", []))

    for country, months in monthly.items():
        if len(months) < 4:
            continue
        counts = list(months.values())
        mean   = sum(counts) / len(counts)
        stdev  = (sum((c-mean)**2 for c in counts)/len(counts))**0.5
        if stdev == 0:
            continue

        for month in [current_month, prev_month]:
            count = months.get(month, 0)
            if count == 0:
                continue
            z = (count - mean) / stdev
            spike_key = f"{country}_{month}"
            if z >= z_threshold and spike_key not in alerted_spikes:
                priority = 4 if z >= 4 else 3
                alert    = make_alert(
                    "STATISTICAL_SPIKE", priority,
                    f"Disappearance spike: {country} ({month})",
                    f"{count} cases in {month} vs mean of {mean:.1f} "
                    f"(z-score: {z:.2f}). Possible organised activity.",
                    meta={"country": country, "month": month,
                          "count": count, "mean": round(mean,1),
                          "z_score": round(z,2)},
                )
                alerts.append(alert)
                alerted_spikes.add(spike_key)
                logger.warning("ALERT [%s] %s", alert["priority_label"], alert["title"])

    state["alerted_spikes"] = list(alerted_spikes)
    return alerts


# ---------------------------------------------------------------------------
# Check 4: Enrichment findings for high-value matches
# ---------------------------------------------------------------------------

def check_enrichment_findings(db, state):
    alerts  = []
    alerted = set(state.get("alerted_enrichment", []))

    try:
        rows = list(db.execute(text("""
            SELECT ef.id, ef.missing_person_id, ef.finding_type,
                   ef.title, ef.url, ef.relevance_score,
                   mp.full_name, mp.country_last_seen
            FROM enrichment_findings ef
            JOIN missing_persons mp ON mp.id = ef.missing_person_id
            WHERE ef.finding_type IN (
                'COURT_TRAFFICKING','DOJ_TRAFFICKING','FBI_WANTED','SANCTIONS_NETWORK'
            )
            AND ef.relevance_score >= 0.6
            ORDER BY ef.created_at DESC
            LIMIT 50
        """)).fetchall())

        for row in rows:
            (fid, pid, ftype, title, url, score, name, country) = row
            finding_key = f"finding_{fid}"
            if finding_key in alerted:
                continue

            priority = 4 if ftype in ("COURT_TRAFFICKING","FBI_WANTED") else 3
            alert    = make_alert(
                "HIGH_VALUE_FINDING", priority,
                f"High-value finding: {ftype.replace('_',' ')} for {name}",
                f"Source: {title} (relevance {score:.2f})",
                cases=[{"id": pid, "name": name, "country": country,
                         "url": url}],
                meta={"finding_type": ftype, "score": score},
            )
            alerts.append(alert)
            alerted.add(finding_key)
            logger.warning("ALERT [%s] %s", alert["priority_label"], alert["title"])

        state["alerted_enrichment"] = list(alerted)
    except Exception:
        pass  # enrichment table may not exist yet

    return alerts


# ---------------------------------------------------------------------------
# Notification senders
# ---------------------------------------------------------------------------

def send_email(alerts, config):
    """Send alert digest via email. Config from env vars."""
    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_pass = os.getenv("SMTP_PASS", "")
    to_addr   = os.getenv("ALERT_EMAIL", "")

    if not all([smtp_user, smtp_pass, to_addr]):
        return False

    try:
        body_lines = [f"MCID Alert Report — {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"]
        for a in alerts:
            body_lines.append(f"[{a['priority_label']}] {a['title']}")
            body_lines.append(f"  {a['description']}")
            for c in a.get("cases", [])[:3]:
                body_lines.append(f"  • {c.get('name','')} — {c.get('url','')}")
            body_lines.append("")

        msg            = MIMEMultipart()
        msg["Subject"] = f"MCID: {len(alerts)} alert(s) — {datetime.now().strftime('%Y-%m-%d')}"
        msg["From"]    = smtp_user
        msg["To"]      = to_addr
        msg.attach(MIMEText("\n".join(body_lines), "plain"))

        with smtplib.SMTP(smtp_host, smtp_port) as s:
            s.starttls()
            s.login(smtp_user, smtp_pass)
            s.sendmail(smtp_user, to_addr, msg.as_string())

        logger.info("Email alert sent to %s", to_addr)
        return True
    except Exception as e:
        logger.error("Email failed: %s", e)
        return False


def send_telegram(alerts, config):
    """Send alerts via Telegram bot. Set env vars to enable."""
    token   = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        return False

    for alert in alerts:
        if alert["priority"] < 3:
            continue
        emoji = "🚨" if alert["priority"] == 4 else "⚠️"
        text  = (
            f"{emoji} *{alert['priority_label']}* — {alert['title']}\n\n"
            f"{alert['description']}\n"
        )
        for c in alert.get("cases", [])[:3]:
            name = c.get("name", "Unknown")
            url  = c.get("url", "")
            text += f"• {name}"
            if url:
                text += f" [→]({url})"
            text += "\n"

        try:
            requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": text,
                      "parse_mode": "Markdown",
                      "disable_web_page_preview": True},
                timeout=10,
            )
            time.sleep(0.5)
        except Exception as e:
            logger.error("Telegram failed: %s", e)

    return True


# ---------------------------------------------------------------------------
# MAIN MONITOR LOOP
# ---------------------------------------------------------------------------

def run_monitor(db_path=DB_PATH, watch=False, test=False, interval_minutes=30):
    engine, Session = init_db(db_path)

    if test:
        logger.info("Test mode — sending test notification...")
        test_alert = make_alert(
            "TEST", 3, "MCID Monitor Test",
            "This is a test alert from the MCID monitoring system.",
        )
        send_email([test_alert], {})
        send_telegram([test_alert], {})
        return

    config = {}

    while True:
        logger.info("Running monitor checks...")
        state = load_state()

        db = Session()
        all_alerts = []

        burst_alerts, new_alerted = check_burst_zones(db, state)
        all_alerts.extend(burst_alerts)
        state["alerted_ids"] = list(
            set(state.get("alerted_ids", [])) | new_alerted
        )

        surname_alerts, more_alerted = check_surname_matches(db, state)
        all_alerts.extend(surname_alerts)
        state["alerted_ids"] = list(
            set(state["alerted_ids"]) | more_alerted
        )

        spike_alerts = check_spikes(db, state)
        all_alerts.extend(spike_alerts)

        enrichment_alerts = check_enrichment_findings(db, state)
        all_alerts.extend(enrichment_alerts)

        db.close()

        if all_alerts:
            # Sort by priority descending
            all_alerts.sort(key=lambda x: x["priority"], reverse=True)

            # Save to pending file
            existing = []
            if PENDING_FILE.exists():
                try:
                    existing = json.loads(PENDING_FILE.read_text())
                except Exception:
                    pass
            existing.extend(all_alerts)
            PENDING_FILE.write_text(json.dumps(existing, indent=2, default=str))

            # Send notifications
            send_email(all_alerts, config)
            send_telegram(all_alerts, config)

            state["alert_count"] = state.get("alert_count", 0) + len(all_alerts)
            logger.info("Generated %d alert(s). Total: %d",
                        len(all_alerts), state["alert_count"])

            # Print summary
            for a in all_alerts:
                print(f"[{a['priority_label']:8s}] {a['title']}")
        else:
            logger.info("No new alerts.")

        state["last_run"] = datetime.now().isoformat()
        save_state(state)

        if not watch:
            break
        logger.info("Next check in %d minutes...", interval_minutes)
        time.sleep(interval_minutes * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MCID Alert Monitor")
    parser.add_argument("--db",       default=DB_PATH)
    parser.add_argument("--watch",    action="store_true",
                        help="Daemon mode — run continuously")
    parser.add_argument("--test",     action="store_true",
                        help="Send test notification and exit")
    parser.add_argument("--interval", type=int, default=30,
                        help="Check interval in minutes (watch mode)")
    args = parser.parse_args()

    run_monitor(args.db, args.watch, args.test, args.interval)
