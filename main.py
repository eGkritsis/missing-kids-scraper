"""
main.py
=======
Orchestrator for the Missing Children Tracker.

Usage:
  python main.py run                   # Run all scrapers once
  python main.py run ncmec             # Run specific scraper
  python main.py run interpol          # Interpol Yellow Notices
  python main.py run international     # All international scrapers
  python main.py schedule              # Daemon mode
  python main.py report                # Database summary
  python main.py export                # Export to CSV
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import csv
import time
import argparse
import schedule
from datetime import datetime
from pathlib import Path

from rich.console import Console
from rich.table import Table
from rich import print as rprint

from database.models import init_db, MissingPerson, NewsArticle, ScraperRun
from scrapers.ncmec import NCMECScraper
from scrapers.namus import NamusScraper
from scrapers.news import NewsScraper
from scrapers.gmcn import GMCNScraper
from scrapers.missing_people_uk import MissingPeopleUKScraper
from scrapers.international import (
    GlobalMissingKidsScraper,
    InterpolScraper,
    RCMPScraper,
)
from utils.helpers import setup_logger
from report import run_report

console = Console()
logger  = setup_logger("main")

DB_PATH = "missing_children.db"

SCRAPERS = {
    # US official
    "ncmec":             NCMECScraper,
    "namus":             NamusScraper,
    # International official
    "interpol":            InterpolScraper,
    "gmcn":                GMCNScraper,
    "global_missing_kids": GlobalMissingKidsScraper,
    "missing_people_uk": MissingPeopleUKScraper,
    "rcmp_canada":       RCMPScraper,
    # Media
    "news":              NewsScraper,
}

# Logical groups
GROUPS = {
    "official":      ["ncmec", "namus", "interpol", "missing_people_uk", "rcmp_canada"],
    "international": ["interpol", "gmcn", "global_missing_kids", "missing_people_uk", "rcmp_canada"],
    "media":         ["news"],
    "all":           list(SCRAPERS.keys()),
}


# ---------------------------------------------------------------------------
# Core runner
# ---------------------------------------------------------------------------

def run_scrapers(names: list[str] | None = None):
    engine, Session = init_db(DB_PATH)
    db = Session()

    # Expand group names
    targets = []
    for name in (names or ["all"]):
        if name in GROUPS:
            targets.extend(GROUPS[name])
        elif name in SCRAPERS:
            targets.append(name)
        else:
            logger.error("Unknown scraper or group: %s (available: %s)",
                         name, ", ".join(list(SCRAPERS) + list(GROUPS)))

    # Deduplicate while preserving order
    seen = set()
    targets = [x for x in targets if not (x in seen or seen.add(x))]

    results = {}
    for name in targets:
        console.rule(f"[bold cyan]Running: {name.upper()}")
        scraper = SCRAPERS[name](db)
        summary = scraper.execute()
        results[name] = summary
        rprint(f"[green]✓[/green] {name}: {summary}")

    db.close()
    console.rule("[bold green]All done")
    return results


# ---------------------------------------------------------------------------
# Scheduled mode
# ---------------------------------------------------------------------------

def run_schedule():
    logger.info("Starting scheduled mode. Press Ctrl+C to stop.")

    def job_official():
        run_scrapers(["official"])

    def job_media():
        run_scrapers(["media"])

    schedule.every(12).hours.do(job_official)
    schedule.every(2).hours.do(job_media)

    job_official()
    job_media()

    while True:
        schedule.run_pending()
        time.sleep(60)


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def print_report_old():  # replaced by report.py
    engine, Session = init_db(DB_PATH)
    db = Session()

    total    = db.query(MissingPerson).count()
    active   = db.query(MissingPerson).filter(MissingPerson.is_resolved == False).count()
    resolved = db.query(MissingPerson).filter(MissingPerson.is_resolved == True).count()
    news_count = db.query(NewsArticle).count()

    from sqlalchemy import func

    by_source = db.query(
        MissingPerson.source, func.count(MissingPerson.id)
    ).group_by(MissingPerson.source).all()

    by_country = db.query(
        MissingPerson.country_last_seen, func.count(MissingPerson.id)
    ).filter(
        MissingPerson.country_last_seen != None,
        MissingPerson.is_resolved == False,
    ).group_by(MissingPerson.country_last_seen).order_by(
        func.count(MissingPerson.id).desc()
    ).limit(15).all()

    by_state = db.query(
        MissingPerson.state_last_seen, func.count(MissingPerson.id)
    ).filter(
        MissingPerson.state_last_seen != None,
        MissingPerson.is_resolved == False,
    ).group_by(MissingPerson.state_last_seen).order_by(
        func.count(MissingPerson.id).desc()
    ).limit(10).all()

    recent_runs = db.query(ScraperRun).order_by(
        ScraperRun.started_at.desc()
    ).limit(15).all()

    console.rule("[bold]Missing Children Tracker — Global Database Report")
    rprint(f"\n[bold]Total records:[/bold]      {total}")
    rprint(f"[bold red]Active cases:[/bold red]       {active}")
    rprint(f"[bold green]Resolved:[/bold green]           {resolved}")
    rprint(f"[bold]News articles:[/bold]      {news_count}\n")

    t1 = Table(title="Records by Source")
    t1.add_column("Source", style="cyan")
    t1.add_column("Count", justify="right")
    for src, cnt in by_source:
        t1.add_row(src, str(cnt))
    console.print(t1)

    t2 = Table(title="Top 15 Countries (active cases)")
    t2.add_column("Country", style="cyan")
    t2.add_column("Cases", justify="right")
    for country, cnt in by_country:
        t2.add_row(country or "Unknown", str(cnt))
    console.print(t2)

    t3 = Table(title="Top 10 US States (active cases)")
    t3.add_column("State", style="cyan")
    t3.add_column("Cases", justify="right")
    for state, cnt in by_state:
        t3.add_row(state or "Unknown", str(cnt))
    console.print(t3)

    t4 = Table(title="Recent Scraper Runs")
    t4.add_column("Scraper")
    t4.add_column("Started")
    t4.add_column("Status")
    t4.add_column("New")
    t4.add_column("Updated")
    t4.add_column("Errors")
    for run in recent_runs:
        color = "green" if run.status == "success" else "red"
        t4.add_row(
            run.scraper_name,
            str(run.started_at)[:16] if run.started_at else "—",
            f"[{color}]{run.status}[/{color}]",
            str(run.records_new),
            str(run.records_updated),
            str(run.errors),
        )
    console.print(t4)
    db.close()


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

def export_csv(output_path: str = "output/missing_children_export.csv"):
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    engine, Session = init_db(DB_PATH)
    db = Session()

    records = db.query(MissingPerson).filter(
        MissingPerson.is_resolved == False
    ).order_by(MissingPerson.date_missing.desc()).all()

    fields = [
        "id", "source", "source_id", "source_url", "full_name",
        "first_name", "last_name", "date_of_birth", "age_at_disappearance",
        "gender", "race_ethnicity", "nationality", "height_cm", "weight_kg",
        "eye_color", "hair_color", "distinguishing_marks",
        "date_missing", "city_last_seen", "state_last_seen", "country_last_seen",
        "circumstances", "case_type", "contact_agency", "contact_phone",
        "ncic_number", "namus_id", "photo_url",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for rec in records:
            writer.writerow({f: getattr(rec, f, "") for f in fields})

    db.close()
    console.print(f"[green]✓ Exported {len(records)} records to {output_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Missing Children Tracker — Global",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Scrapers:  ncmec, namus, interpol, missing_people_uk, rcmp_canada, news, twitter
Groups:    official, international, media, all
        """
    )
    sub = parser.add_subparsers(dest="command")

    run_p = sub.add_parser("run", help="Run scrapers now")
    run_p.add_argument("scrapers", nargs="*",
                       help="Scraper names or groups (default: all)")

    sub.add_parser("schedule", help="Run on schedule (daemon)")
    sub.add_parser("report",   help="Print database summary")

    exp_p = sub.add_parser("export", help="Export to CSV")
    exp_p.add_argument("--out", default="output/missing_children_export.csv")

    args = parser.parse_args()

    if args.command == "run":
        run_scrapers(args.scrapers or None)
    elif args.command == "schedule":
        run_schedule()
    elif args.command == "report":
        run_report()
    elif args.command == "export":
        export_csv(args.out)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
