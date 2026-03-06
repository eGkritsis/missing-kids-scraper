"""
report.py

Enhanced reporting module for the Missing Children Tracker.
Run: python3 report.py
Or:  python3 report.py --export   (also saves full report to HTML)
"""

import sys
import os
import argparse
from datetime import datetime, date, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import func, case, extract, and_, or_
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.columns import Columns
from rich.text import Text
from rich import box

from database.models import init_db, MissingPerson, NewsArticle, ScraperRun

console = Console(width=120)
DB_PATH = "missing_children.db"


def section(title: str):
    console.print()
    console.rule(f"[bold yellow]{title}[/bold yellow]")


def run_report(export_html: bool = False):
    engine, Session = init_db(DB_PATH)
    db = Session()

    # =========================================================
    # 1. HEADLINE STATS
    # =========================================================
    section("OVERVIEW")

    total        = db.query(MissingPerson).count()
    active       = db.query(MissingPerson).filter(MissingPerson.is_resolved == False).count()
    resolved     = db.query(MissingPerson).filter(MissingPerson.is_resolved == True).count()
    with_photo   = db.query(MissingPerson).filter(
        MissingPerson.is_resolved == False,
        MissingPerson.photo_url != None,
        MissingPerson.photo_url != "",
    ).count()
    news_total   = db.query(NewsArticle).count()
    resolve_rate = f"{resolved/total*100:.1f}%" if total else "0%"

    # New in last 7 / 30 days
    seven_days_ago  = datetime.utcnow() - timedelta(days=7)
    thirty_days_ago = datetime.utcnow() - timedelta(days=30)
    new_7d  = db.query(MissingPerson).filter(MissingPerson.created_at >= seven_days_ago).count()
    new_30d = db.query(MissingPerson).filter(MissingPerson.created_at >= thirty_days_ago).count()

    panels = [
        Panel(f"[bold white]{total:,}[/bold white]\n[dim]Total Records[/dim]",         style="blue"),
        Panel(f"[bold red]{active:,}[/bold red]\n[dim]Active Cases[/dim]",              style="red"),
        Panel(f"[bold green]{resolved:,}[/bold green]\n[dim]Resolved[/dim]",            style="green"),
        Panel(f"[bold cyan]{resolve_rate}[/bold cyan]\n[dim]Resolution Rate[/dim]",     style="cyan"),
        Panel(f"[bold magenta]{with_photo:,}[/bold magenta]\n[dim]Have Photos[/dim]",   style="magenta"),
        Panel(f"[bold yellow]{news_total:,}[/bold yellow]\n[dim]News Articles[/dim]",   style="yellow"),
        Panel(f"[bold white]{new_7d:,}[/bold white]\n[dim]New (7 days)[/dim]",          style="blue"),
        Panel(f"[bold white]{new_30d:,}[/bold white]\n[dim]New (30 days)[/dim]",        style="blue"),
    ]
    console.print(Columns(panels, equal=True))

    # =========================================================
    # 2. RECORDS BY SOURCE
    # =========================================================
    section("RECORDS BY SOURCE")

    by_source = db.query(
        MissingPerson.source,
        func.count(MissingPerson.id).label("total"),
        func.sum(case((MissingPerson.is_resolved == False, 1), else_=0)).label("active"),
        func.sum(case((MissingPerson.is_resolved == True,  1), else_=0)).label("resolved"),
        func.sum(case((and_(MissingPerson.is_resolved == False,
                            MissingPerson.photo_url != None,
                            MissingPerson.photo_url != ""), 1), else_=0)).label("photos"),
    ).group_by(MissingPerson.source).order_by(func.count(MissingPerson.id).desc()).all()

    t = Table(box=box.ROUNDED, show_footer=False)
    t.add_column("Source",          style="cyan",    min_width=20)
    t.add_column("Total",           justify="right", style="white")
    t.add_column("Active",          justify="right", style="red")
    t.add_column("Resolved",        justify="right", style="green")
    t.add_column("With Photo",      justify="right", style="magenta")
    t.add_column("Resolve Rate",    justify="right", style="yellow")

    for row in by_source:
        rate = f"{int(row.resolved)/int(row.total)*100:.1f}%" if row.total else "0%"
        t.add_row(
            row.source,
            f"{int(row.total):,}",
            f"{int(row.active):,}",
            f"{int(row.resolved):,}",
            f"{int(row.photos):,}",
            rate,
        )
    console.print(t)

    # =========================================================
    # 3. TOP COUNTRIES
    # =========================================================
    section("TOP 20 COUNTRIES (Active Cases)")

    by_country = db.query(
        MissingPerson.country_last_seen,
        func.count(MissingPerson.id).label("cnt"),
    ).filter(
        MissingPerson.is_resolved == False,
        MissingPerson.country_last_seen != None,
        MissingPerson.country_last_seen != "",
    ).group_by(MissingPerson.country_last_seen).order_by(func.count(MissingPerson.id).desc()).limit(20).all()

    t = Table(box=box.ROUNDED)
    t.add_column("Rank", justify="right", style="dim", width=5)
    t.add_column("Country",          style="cyan",  min_width=25)
    t.add_column("Active Cases",     justify="right", style="red")
    t.add_column("Bar",              style="red",   min_width=30)

    max_cnt = by_country[0].cnt if by_country else 1
    for i, row in enumerate(by_country, 1):
        bar_len  = int((row.cnt / max_cnt) * 30)
        bar      = "█" * bar_len
        t.add_row(str(i), row.country_last_seen or "Unknown", f"{row.cnt:,}", bar)
    console.print(t)

    # =========================================================
    # 4. TOP US STATES
    # =========================================================
    section("TOP 15 US STATES (Active Cases)")

    by_state = db.query(
        MissingPerson.state_last_seen,
        func.count(MissingPerson.id).label("cnt"),
    ).filter(
        MissingPerson.is_resolved == False,
        MissingPerson.country_last_seen.in_(["USA", "United States", "US"]),
        MissingPerson.state_last_seen != None,
        MissingPerson.state_last_seen != "",
    ).group_by(MissingPerson.state_last_seen).order_by(func.count(MissingPerson.id).desc()).limit(15).all()

    t = Table(box=box.ROUNDED)
    t.add_column("Rank",         justify="right", style="dim", width=5)
    t.add_column("State",        style="cyan",    min_width=20)
    t.add_column("Active Cases", justify="right", style="red")
    t.add_column("Bar",          style="red",     min_width=30)

    max_cnt = by_state[0].cnt if by_state else 1
    for i, row in enumerate(by_state, 1):
        bar_len = int((row.cnt / max_cnt) * 30)
        t.add_row(str(i), row.state_last_seen or "Unknown", f"{row.cnt:,}", "█" * bar_len)
    console.print(t)

    # =========================================================
    # 5. GENDER BREAKDOWN
    # =========================================================
    section("GENDER BREAKDOWN (Active Cases)")

    by_gender = db.query(
        MissingPerson.gender,
        func.count(MissingPerson.id).label("cnt"),
    ).filter(MissingPerson.is_resolved == False).group_by(MissingPerson.gender).order_by(
        func.count(MissingPerson.id).desc()
    ).all()

    t = Table(box=box.ROUNDED)
    t.add_column("Gender",       style="cyan",    min_width=15)
    t.add_column("Count",        justify="right", style="white")
    t.add_column("% of Active",  justify="right", style="yellow")

    for row in by_gender:
        pct = f"{row.cnt/active*100:.1f}%" if active else "0%"
        t.add_row(row.gender or "Unknown", f"{row.cnt:,}", pct)
    console.print(t)

    # =========================================================
    # 6. AGE DISTRIBUTION
    # =========================================================
    section("AGE AT DISAPPEARANCE (Active Cases)")

    age_buckets = {
        "0–5":   (0, 5),
        "6–10":  (6, 10),
        "11–13": (11, 13),
        "14–15": (14, 15),
        "16–17": (16, 17),
        "18+":   (18, 999),
        "Unknown": (None, None),
    }

    t = Table(box=box.ROUNDED)
    t.add_column("Age Group",    style="cyan",    min_width=12)
    t.add_column("Count",        justify="right", style="white")
    t.add_column("% of Active",  justify="right", style="yellow")
    t.add_column("Bar",          style="blue",    min_width=25)

    bucket_counts = {}
    for label, (lo, hi) in age_buckets.items():
        if lo is None:
            cnt = db.query(MissingPerson).filter(
                MissingPerson.is_resolved == False,
                MissingPerson.age_at_disappearance == None,
            ).count()
        else:
            cnt = db.query(MissingPerson).filter(
                MissingPerson.is_resolved == False,
                MissingPerson.age_at_disappearance >= lo,
                MissingPerson.age_at_disappearance <= hi,
            ).count()
        bucket_counts[label] = cnt

    max_cnt = max(bucket_counts.values()) or 1
    for label, cnt in bucket_counts.items():
        pct     = f"{cnt/active*100:.1f}%" if active else "0%"
        bar_len = int((cnt / max_cnt) * 25)
        t.add_row(label, f"{cnt:,}", pct, "█" * bar_len)
    console.print(t)

    # =========================================================
    # 7. CASE TYPE BREAKDOWN
    # =========================================================
    section("CASE TYPE BREAKDOWN (Active Cases)")

    by_type = db.query(
        MissingPerson.case_type,
        func.count(MissingPerson.id).label("cnt"),
    ).filter(
        MissingPerson.is_resolved == False,
        MissingPerson.case_type != None,
        MissingPerson.case_type != "",
    ).group_by(MissingPerson.case_type).order_by(func.count(MissingPerson.id).desc()).limit(15).all()

    t = Table(box=box.ROUNDED)
    t.add_column("Case Type",    style="cyan",    min_width=30)
    t.add_column("Count",        justify="right", style="white")
    t.add_column("% of Active",  justify="right", style="yellow")

    for row in by_type:
        pct = f"{row.cnt/active*100:.1f}%" if active else "0%"
        t.add_row(row.case_type or "Unknown", f"{row.cnt:,}", pct)
    console.print(t)

    # =========================================================
    # 8. MISSING BY YEAR
    # =========================================================
    section("CASES BY YEAR OF DISAPPEARANCE (last 10 years, active only)")

    current_year = datetime.utcnow().year
    t = Table(box=box.ROUNDED)
    t.add_column("Year",         style="cyan",  width=8)
    t.add_column("Cases",        justify="right", style="white")
    t.add_column("Bar",          style="yellow", min_width=35)

    year_data = []
    for yr in range(current_year, current_year - 11, -1):
        cnt = db.query(MissingPerson).filter(
            MissingPerson.is_resolved == False,
            extract("year", MissingPerson.date_missing) == yr,
        ).count()
        year_data.append((yr, cnt))

    max_cnt = max(c for _, c in year_data) or 1
    for yr, cnt in year_data:
        bar_len = int((cnt / max_cnt) * 35)
        t.add_row(str(yr), f"{cnt:,}", "█" * bar_len)
    console.print(t)

    # =========================================================
    # 9. RECENTLY RESOLVED CASES
    # =========================================================
    section("RECENTLY RESOLVED CASES (last 20)")

    recent_resolved = db.query(MissingPerson).filter(
        MissingPerson.is_resolved == True,
        MissingPerson.resolution_notes != None,
    ).order_by(MissingPerson.updated_at.desc()).limit(20).all()

    if recent_resolved:
        t = Table(box=box.ROUNDED)
        t.add_column("Name",         style="green",  min_width=25)
        t.add_column("Source",       style="cyan",   width=12)
        t.add_column("Country",      style="white",  width=15)
        t.add_column("Resolution",   style="dim",    min_width=40)

        for r in recent_resolved:
            notes = (r.resolution_notes or "")[:80] + "..." if len(r.resolution_notes or "") > 80 else (r.resolution_notes or "")
            t.add_row(
                r.full_name or "Unknown",
                r.source,
                r.country_last_seen or "",
                notes,
            )
        console.print(t)
    else:
        console.print("[dim]No resolved cases with notes yet.[/dim]")

    # =========================================================
    # 10. MOST RECENT MISSING CHILDREN
    # =========================================================
    section("10 MOST RECENTLY REPORTED MISSING")

    recent_missing = db.query(MissingPerson).filter(
        MissingPerson.is_resolved == False,
        MissingPerson.date_missing != None,
    ).order_by(MissingPerson.date_missing.desc()).limit(10).all()

    t = Table(box=box.ROUNDED)
    t.add_column("Name",         style="red",    min_width=25)
    t.add_column("Missing Since",style="white",  width=14)
    t.add_column("Age",          justify="right", width=5)
    t.add_column("Country",      style="cyan",   width=15)
    t.add_column("State/City",   style="white",  width=20)
    t.add_column("Type",         style="yellow", width=20)
    t.add_column("Source",       style="dim",    width=10)

    for r in recent_missing:
        location = ", ".join(filter(None, [r.city_last_seen, r.state_last_seen]))
        t.add_row(
            r.full_name or "Unknown",
            str(r.date_missing) if r.date_missing else "",
            str(r.age_at_disappearance or ""),
            r.country_last_seen or "",
            location,
            r.case_type or "",
            r.source,
        )
    console.print(t)

    # =========================================================
    # 11. NEWS CROSS-REFERENCE MATCHES
    # =========================================================
    section("NEWS ARTICLES WITH DB MATCHES")

    matched_articles = db.query(NewsArticle).filter(
        NewsArticle.names_mentioned != None,
        NewsArticle.names_mentioned != "",
    ).order_by(NewsArticle.published_at.desc()).limit(15).all()

    t = Table(box=box.ROUNDED)
    t.add_column("Published",    style="dim",    width=12)
    t.add_column("Source",       style="cyan",   width=20)
    t.add_column("Title",        style="white",  min_width=40)
    t.add_column("Names Found",  style="yellow", min_width=20)

    for a in matched_articles:
        pub  = str(a.published_at)[:10] if a.published_at else ""
        name = (a.names_mentioned or "")[:50]
        title = (a.title or "")[:60] + ("..." if len(a.title or "") > 60 else "")
        t.add_row(pub, a.source_name or "", title, name)
    console.print(t)

    # =========================================================
    # 12. SCRAPER HEALTH
    # =========================================================
    section("SCRAPER RUN HISTORY")

    # Last run per scraper
    subq = db.query(
        ScraperRun.scraper_name,
        func.max(ScraperRun.started_at).label("last_run"),
    ).group_by(ScraperRun.scraper_name).subquery()

    recent_runs = db.query(ScraperRun).join(
        subq,
        and_(
            ScraperRun.scraper_name == subq.c.scraper_name,
            ScraperRun.started_at   == subq.c.last_run,
        )
    ).order_by(ScraperRun.records_new.desc()).all()

    t = Table(box=box.ROUNDED)
    t.add_column("Scraper",      style="cyan",   min_width=20)
    t.add_column("Last Run",     style="white",  width=17)
    t.add_column("Status",       width=10)
    t.add_column("Found",        justify="right", style="white",  width=7)
    t.add_column("New",          justify="right", style="green",  width=7)
    t.add_column("Updated",      justify="right", style="yellow", width=8)
    t.add_column("Errors",       justify="right", style="red",    width=7)
    t.add_column("Duration",     justify="right", style="dim",    width=10)

    for run in recent_runs:
        color    = "green" if run.status == "success" else ("yellow" if run.status == "partial" else "red")
        duration = ""
        if run.started_at and run.finished_at:
            secs = int((run.finished_at - run.started_at).total_seconds())
            duration = f"{secs//60}m {secs%60}s" if secs >= 60 else f"{secs}s"
        t.add_row(
            run.scraper_name,
            str(run.started_at)[:16] if run.started_at else "—",
            f"[{color}]{run.status or '?'}[/{color}]",
            str(run.records_found or 0),
            str(run.records_new or 0),
            str(run.records_updated or 0),
            str(run.errors or 0),
            duration,
        )
    console.print(t)

    # All-time totals
    all_runs = db.query(ScraperRun).all()
    total_runs      = len(all_runs)
    total_errors    = sum(r.errors or 0 for r in all_runs)
    total_new_ever  = sum(r.records_new or 0 for r in all_runs)
    console.print(f"\n[dim]Total scraper runs: {total_runs} | "
                  f"Total new records ever ingested: {total_new_ever:,} | "
                  f"Total errors ever: {total_errors}[/dim]")

    # =========================================================
    # 13. DATA QUALITY
    # =========================================================
    section("DATA QUALITY (Active Cases)")

    fields = [
        ("Full Name",        MissingPerson.full_name),
        ("Date of Birth",    MissingPerson.date_of_birth),
        ("Date Missing",     MissingPerson.date_missing),
        ("Age",              MissingPerson.age_at_disappearance),
        ("Gender",           MissingPerson.gender),
        ("Country",          MissingPerson.country_last_seen),
        ("State",            MissingPerson.state_last_seen),
        ("City",             MissingPerson.city_last_seen),
        ("Photo",            MissingPerson.photo_url),
        ("Case Type",        MissingPerson.case_type),
        ("Circumstances",    MissingPerson.circumstances),
        ("Contact Agency",   MissingPerson.contact_agency),
    ]

    t = Table(box=box.ROUNDED)
    t.add_column("Field",            style="cyan",    min_width=18)
    t.add_column("Populated",        justify="right", style="white")
    t.add_column("Missing",          justify="right", style="red")
    t.add_column("Coverage",         justify="right", style="yellow")
    t.add_column("Bar",              style="green",   min_width=25)

    for label, col in fields:
        populated = db.query(MissingPerson).filter(
            MissingPerson.is_resolved == False,
            col != None,
            col != "",
        ).count()
        missing_cnt = active - populated
        pct         = populated / active * 100 if active else 0
        bar_len     = int(pct / 4)
        t.add_row(
            label,
            f"{populated:,}",
            f"{missing_cnt:,}",
            f"{pct:.1f}%",
            "█" * bar_len,
        )
    console.print(t)

    console.print(f"\n[dim]Report generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC[/dim]")
    console.print()

    db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Missing Children Tracker — Detailed Report")
    parser.add_argument("--export", action="store_true", help="Also export report to HTML")
    args = parser.parse_args()
    run_report(export_html=args.export)
