"""
main.py — Missing Children Tracker orchestrator

Usage:
  python main.py run                    # Run all scrapers
  python main.py run ncmec              # Run one scraper
  python main.py run international      # Run a group
  python main.py schedule               # Daemon mode
  python main.py report                 # DB summary
  python main.py export [--out PATH]    # CSV export
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import csv
import time
import argparse
import schedule
from pathlib import Path

from rich.console import Console
from rich import print as rprint

from database.models import init_db, MissingPerson
from scrapers.ncmec           import NCMECScraper
from scrapers.namus           import NamusScraper
from scrapers.news            import NewsScraper
from scrapers.gmcn            import GMCNScraper
from scrapers.missing_people_uk import MissingPeopleUKScraper
from scrapers.interpol        import InterpolScraper
from scrapers.international   import RCMPScraper
from report import run_report
from utils.helpers import setup_logger

console = Console()
logger  = setup_logger("main")

DB_PATH = "missing_children.db"

SCRAPERS = {
    "ncmec":             NCMECScraper,
    "namus":             NamusScraper,
    "interpol":          InterpolScraper,
    "gmcn":              GMCNScraper,
    "missing_people_uk": MissingPeopleUKScraper,
    "rcmp_canada":       RCMPScraper,
    "news":              NewsScraper,
}

GROUPS = {
    "us":            ["ncmec", "namus"],
    "international": ["interpol", "gmcn", "missing_people_uk", "rcmp_canada"],
    "official":      ["ncmec", "namus", "interpol", "gmcn", "missing_people_uk", "rcmp_canada"],
    "media":         ["news"],
    "all":           list(SCRAPERS.keys()),
}


# ---------------------------------------------------------------------------
# Core runner
# ---------------------------------------------------------------------------

def run_scrapers(names=None):
    engine, Session = init_db(DB_PATH)
    db = Session()

    targets = []
    for name in (names or ["all"]):
        if name in GROUPS:
            targets.extend(GROUPS[name])
        elif name in SCRAPERS:
            targets.append(name)
        else:
            logger.error("Unknown scraper/group: %s", name)
            logger.error("Scrapers: %s", ", ".join(SCRAPERS))
            logger.error("Groups:   %s", ", ".join(GROUPS))

    # Deduplicate, preserve order
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
# Scheduled daemon
# ---------------------------------------------------------------------------

def run_schedule():
    logger.info("Scheduled mode started — Ctrl+C to stop")

    schedule.every(12).hours.do(lambda: run_scrapers(["official"]))
    schedule.every(2).hours.do(lambda: run_scrapers(["media"]))

    # Run immediately on start
    run_scrapers(["official"])
    run_scrapers(["media"])

    while True:
        schedule.run_pending()
        time.sleep(60)


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

def export_csv(output_path="output/missing_children_export.csv"):
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
    console.print(f"[green]✓ Exported {len(records):,} records → {output_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Missing Children Tracker — Global",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Scrapers : ncmec, namus, interpol, gmcn, missing_people_uk, rcmp_canada, news
Groups   : us, international, official, media, all
        """,
    )
    sub = parser.add_subparsers(dest="command")

    run_p = sub.add_parser("run", help="Run scrapers now")
    run_p.add_argument("scrapers", nargs="*",
                       help="Scraper names or group (default: all)")

    sub.add_parser("schedule", help="Run on schedule (daemon)")
    sub.add_parser("report",   help="Print database report")

    exp_p = sub.add_parser("export", help="Export active cases to CSV")
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
