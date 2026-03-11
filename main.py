"""
main.py — Missing Children Intelligence Dashboard (MCID)
=========================================================
Single entry point for everything.

SCRAPING:
  python main.py run                        # all scrapers
  python main.py run ncmec                  # one scraper
  python main.py run interpol gmcn          # multiple
  python main.py run international          # group
  python main.py schedule                   # daemon mode

ANALYSIS:
  python main.py patterns                   # pattern analysis
  python main.py enrich                     # OSINT enrichment
  python main.py enrich --limit 100         # limit cases
  python main.py enrich --source doj        # single enricher
  python main.py network                    # build network graph

ALERTS:
  python main.py monitor                    # run once
  python main.py monitor --watch            # daemon
  python main.py monitor --test             # test notifications

DOCUMENTS:
  python main.py docs report.pdf            # process PDF
  python main.py docs /path/to/folder/      # process directory

REPORTS & EXPORT:
  python main.py report                     # full HTML dashboard
  python main.py report --skip-patterns     # fast rebuild
  python main.py export                     # CSV export
  python main.py export --format xml        # XML for i2/I-BASE
  python main.py export --format all        # all formats
  python main.py le-report                  # LE PDF briefs (all)
  python main.py le-report --cluster GARCIA # specific cluster
  python main.py le-report --burst GUAYAQUIL

FULL PIPELINE:
  python main.py pipeline                   # scrape → analyse → report
  python main.py pipeline --skip-scrape     # analyse → report only
"""

import argparse
import csv
import os
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import schedule
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import print as rprint

from database.models import init_db, MissingPerson
from scrapers.ncmec             import NCMECScraper
from scrapers.namus             import NamusScraper
from scrapers.news              import NewsScraper
from scrapers.gmcn              import GMCNScraper
from scrapers.missing_people_uk import MissingPeopleUKScraper
from scrapers.interpol          import InterpolScraper
from utils.helpers import setup_logger

console = Console()
logger  = setup_logger("main")

DB_PATH = "missing_children.db"

# ---------------------------------------------------------------------------
# Scrapers & groups
# ---------------------------------------------------------------------------

SCRAPERS = {
    "ncmec":             NCMECScraper,
    "namus":             NamusScraper,
    "interpol":          InterpolScraper,
    "gmcn":              GMCNScraper,
    "missing_people_uk": MissingPeopleUKScraper,
    "news":              NewsScraper,
}

GROUPS = {
    "us":            ["ncmec", "namus"],
    "international": ["interpol", "gmcn", "missing_people_uk"],
    "official":      ["ncmec", "namus", "interpol", "gmcn", "missing_people_uk"],
    "media":         ["news"],
    "all":           list(SCRAPERS.keys()),
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _dedup(lst):
    seen = set()
    return [x for x in lst if not (x in seen or seen.add(x))]

def _get_db():
    engine, Session = init_db(DB_PATH)
    return Session()

def _import(module_path, from_dir=None):
    """Safely import a module, printing clear error if missing."""
    import importlib.util
    if from_dir:
        search = os.path.join(os.path.dirname(__file__), from_dir)
        sys.path.insert(0, search)
    try:
        return __import__(module_path)
    except ImportError as e:
        console.print(f"[red]Import error:[/red] {e}")
        console.print(f"[yellow]Make sure all files are in place. See README.[/yellow]")
        return None

# ---------------------------------------------------------------------------
# CMD: run scrapers
# ---------------------------------------------------------------------------

def cmd_run(args):
    targets_raw = args.scrapers or ["all"]
    targets = []
    for name in targets_raw:
        if name in GROUPS:
            targets.extend(GROUPS[name])
        elif name in SCRAPERS:
            targets.append(name)
        else:
            console.print(f"[red]Unknown scraper/group:[/red] {name}")
            console.print(f"Scrapers: {', '.join(SCRAPERS)}")
            console.print(f"Groups:   {', '.join(GROUPS)}")
            return
    targets = _dedup(targets)

    db      = _get_db()
    results = {}
    total_new = 0

    for name in targets:
        console.rule(f"[bold cyan]{name.upper()}")
        scraper = SCRAPERS[name](db)
        try:
            summary = scraper.execute()
            results[name] = summary
            total_new += summary.get("new", 0)
            rprint(f"[green]✓[/green] {name}: {summary}")
        except Exception as e:
            logger.error("Scraper %s crashed: %s", name, e)
            rprint(f"[red]✗[/red] {name}: {e}")

    db.close()
    console.rule(f"[bold green]Done — {total_new} new records")
    return results


# ---------------------------------------------------------------------------
# CMD: schedule
# ---------------------------------------------------------------------------

def cmd_schedule(args):
    logger.info("Scheduled mode started. Ctrl+C to stop.")

    def job_official(): cmd_run(type("A", (), {"scrapers": ["official"]})())
    def job_media():    cmd_run(type("A", (), {"scrapers": ["media"]})())

    schedule.every(12).hours.do(job_official)
    schedule.every(2).hours.do(job_media)

    job_official()
    job_media()

    while True:
        schedule.run_pending()
        time.sleep(60)


# ---------------------------------------------------------------------------
# CMD: patterns
# ---------------------------------------------------------------------------

def cmd_patterns(args):
    console.rule("[bold cyan]PATTERN ANALYSIS")
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "analysis"))
        from patterns import run_analysis, build_text_report
        import argparse as ap

        Path("analysis/output").mkdir(parents=True, exist_ok=True)

        results = run_analysis(
            db_path     = getattr(args, "db", DB_PATH),
            min_cluster = getattr(args, "min_cluster", 2),
            date_window = getattr(args, "date_window", 180),
        )

        import json
        json_path = "analysis/output/pattern_report.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, default=str)

        # build_text_report needs an args-like object
        fake_args = type("A", (), {
            "db": getattr(args, "db", DB_PATH),
            "min_cluster": getattr(args, "min_cluster", 2),
            "date_window": getattr(args, "date_window", 180),
        })()
        txt = build_text_report(results, fake_args)
        txt_path = "analysis/output/pattern_report.txt"
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(txt)

        s = results["summary"]
        console.print(Panel(
            f"[green]Cases analysed:[/green] {results['total_cases']:,}\n"
            f"[yellow]Surname clusters:[/yellow] {s['surname_cluster_count']} "
            f"({s['sibling_units']} sibling · {s['family_groups']} family · "
            f"{s['cross_border_clusters']} cross-border)\n"
            f"[red]Spatio-temporal bursts:[/red] {s['spatiotemporal_bursts']} "
            f"({s['active_bursts']} ACTIVE)\n"
            f"[yellow]Active corridors:[/yellow] {s['active_corridors']}\n"
            f"[cyan]Flagged countries:[/cyan] {s['flagged_countries']}\n"
            f"[blue]Timeline spikes:[/blue] {s['timeline_spikes']} ({s['active_spikes']} recent)\n\n"
            f"[dim]JSON → {json_path}\n"
            f"TXT  → {txt_path}[/dim]",
            title="Pattern Analysis Complete",
        ))

    except ImportError as e:
        console.print(f"[red]Error:[/red] {e}")
        console.print("[yellow]Make sure analysis/patterns.py is in place.[/yellow]")


# ---------------------------------------------------------------------------
# CMD: enrich
# ---------------------------------------------------------------------------

def cmd_enrich(args):
    console.rule("[bold cyan]OSINT ENRICHMENT")
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "analysis"))
        from enrichment import run_enrichment, ensure_enrichment_table
        from database.models import init_db as _init
        engine, _ = _init(DB_PATH)
        ensure_enrichment_table(engine)

        n = run_enrichment(
            db_path       = getattr(args, "db", DB_PATH),
            limit         = getattr(args, "limit", None),
            source_filter = getattr(args, "source", None),
            case_id       = getattr(args, "case_id", None),
        )
        console.print(f"[green]✓[/green] Enrichment complete — {n} new findings")
    except ImportError as e:
        console.print(f"[red]Error:[/red] {e}")
        console.print("[yellow]Make sure analysis/enrichment.py is in place.[/yellow]")


# ---------------------------------------------------------------------------
# CMD: network
# ---------------------------------------------------------------------------

def cmd_network(args):
    console.rule("[bold cyan]NETWORK GRAPH")
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "analysis"))
        from network import build_network, build_network_html

        Path("output").mkdir(exist_ok=True)
        Path("analysis/output").mkdir(parents=True, exist_ok=True)

        graph    = build_network(
            db_path         = getattr(args, "db", DB_PATH),
            min_connections = getattr(args, "min_connections", 1),
        )

        import json
        json_path = "analysis/output/network.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(graph, f, default=str)

        html      = build_network_html(graph)
        html_path = "output/network.html"
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html)

        st = graph["stats"]
        console.print(Panel(
            f"[green]Nodes:[/green] {st['total_nodes']:,}\n"
            f"[yellow]Edges:[/yellow] {st['total_edges']:,}\n"
            f"[cyan]By type:[/cyan] {st['by_type']}\n\n"
            f"[dim]HTML → {html_path}\n"
            f"JSON → {json_path}[/dim]",
            title="Network Graph Built",
        ))

    except ImportError as e:
        console.print(f"[red]Error:[/red] {e}")
        console.print("[yellow]Make sure analysis/network.py is in place.[/yellow]")


# ---------------------------------------------------------------------------
# CMD: monitor
# ---------------------------------------------------------------------------

def cmd_monitor(args):
    console.rule("[bold cyan]ALERT MONITOR")
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "alerts"))
        from monitor import run_monitor
        run_monitor(
            db_path          = getattr(args, "db", DB_PATH),
            watch            = getattr(args, "watch", False),
            test             = getattr(args, "test", False),
            interval_minutes = getattr(args, "interval", 30),
        )
    except ImportError as e:
        console.print(f"[red]Error:[/red] {e}")
        console.print("[yellow]Make sure alerts/monitor.py is in place.[/yellow]")


# ---------------------------------------------------------------------------
# CMD: documents
# ---------------------------------------------------------------------------

def cmd_docs(args):
    console.rule("[bold cyan]DOCUMENT INTELLIGENCE")
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "analysis"))
        from documents import process_document, process_directory, build_doc_report
        from database.models import init_db as _init

        engine, Session = _init(DB_PATH)
        # Ensure enrichment table
        try:
            from enrichment import ensure_enrichment_table
            ensure_enrichment_table(engine)
        except Exception:
            pass

        db    = Session()
        input_path = Path(args.input)
        Path("analysis/output").mkdir(parents=True, exist_ok=True)

        if input_path.is_dir():
            results = process_directory(input_path, db)
        else:
            r       = process_document(input_path, db)
            results = [r] if r else []

        db.close()

        import json
        out_json = "analysis/output/doc_findings.json"
        with open(out_json, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, default=str)

        report   = build_doc_report(results)
        out_txt  = "analysis/output/doc_findings.txt"
        with open(out_txt, "w", encoding="utf-8") as f:
            f.write(report)

        total_matches = sum(len(r.get("db_matches", [])) for r in results if r)
        console.print(Panel(
            f"[green]Documents processed:[/green] {len(results)}\n"
            f"[yellow]DB matches found:[/yellow] {total_matches}\n\n"
            f"[dim]JSON → {out_json}\n"
            f"TXT  → {out_txt}[/dim]",
            title="Document Processing Complete",
        ))
        console.print(report)

    except ImportError as e:
        console.print(f"[red]Error:[/red] {e}")
        console.print("[yellow]Make sure analysis/documents.py is in place.[/yellow]")
        console.print("[yellow]Install: pip install pdfplumber spacy --break-system-packages[/yellow]")


# ---------------------------------------------------------------------------
# CMD: report (HTML dashboard)
# ---------------------------------------------------------------------------

def cmd_report(args):
    console.rule("[bold cyan]HTML DASHBOARD")
    try:
        from report import run_report
        out = run_report(
            db_path       = getattr(args, "db", DB_PATH),
            skip_patterns = getattr(args, "skip_patterns", False),
            out_path      = getattr(args, "out", "output/dashboard.html"),
        )
        console.print(f"[green]✓[/green] Dashboard → {out}")
        console.print("[dim]Open in browser: xdg-open output/dashboard.html[/dim]")
    except ImportError as e:
        console.print(f"[red]Error:[/red] {e}")
        console.print("[yellow]Make sure report.py is in place.[/yellow]")


# ---------------------------------------------------------------------------
# CMD: export
# ---------------------------------------------------------------------------

def cmd_export(args):
    console.rule("[bold cyan]DATA EXPORT")
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "export"))
        from ibase import run_export
        run_export(
            db_path       = getattr(args, "db", DB_PATH),
            fmt           = getattr(args, "format", "csv"),
            clusters_only = getattr(args, "clusters_only", False),
            since         = getattr(args, "since", None),
            out_dir       = getattr(args, "out", "export/output"),
        )
        console.print(f"[green]✓[/green] Export complete → export/output/")
    except ImportError as e:
        console.print(f"[red]Error:[/red] {e}")
        console.print("[yellow]Make sure export/ibase.py is in place.[/yellow]")


# ---------------------------------------------------------------------------
# CMD: le-report
# ---------------------------------------------------------------------------

def cmd_le_report(args):
    console.rule("[bold cyan]LE PDF REPORTS")
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "export"))
        from le_report import run_le_report
        run_le_report(
            db_path          = getattr(args, "db", DB_PATH),
            cluster_surname  = getattr(args, "cluster", None),
            burst_city       = getattr(args, "burst", None),
            export_all       = getattr(args, "all", False),
            out_dir          = getattr(args, "out", "export/output"),
        )
        console.print(f"[green]✓[/green] PDF reports → export/output/")
    except ImportError as e:
        console.print(f"[red]Error:[/red] {e}")
        console.print("[yellow]Make sure export/le_report.py is in place.[/yellow]")
        console.print("[yellow]Install: pip install reportlab Pillow --break-system-packages[/yellow]")


# ---------------------------------------------------------------------------
# CMD: pipeline (everything in sequence)
# ---------------------------------------------------------------------------

def cmd_pipeline(args):
    console.rule("[bold magenta]FULL PIPELINE")
    skip_scrape = getattr(args, "skip_scrape", False)

    steps = []
    if not skip_scrape:
        steps.append(("Scraping all sources",
                       lambda: cmd_run(type("A",(),{"scrapers":["all"]})())))
        steps.append(("Cleanup adults from DB",
                       lambda: cmd_cleanup(type("A",(),{"yes":True})())))
    steps.append(("Pattern analysis",
                   lambda: cmd_patterns(args)))
    steps.append(("OSINT enrichment (limit 500)",
                   lambda: cmd_enrich(type("A",(),{
                       "db": DB_PATH, "limit": 500,
                       "source": None, "case_id": None,
                   })())))
    steps.append(("Network graph",
                   lambda: cmd_network(args)))
    steps.append(("Alert monitor (single pass)",
                   lambda: cmd_monitor(type("A",(),{
                       "db": DB_PATH, "watch": False,
                       "test": False, "interval": 30,
                   })())))
    steps.append(("HTML dashboard",
                   lambda: cmd_report(args)))
    steps.append(("CSV export",
                   lambda: cmd_export(type("A",(),{
                       "db": DB_PATH, "format": "csv",
                       "clusters_only": False, "since": None,
                       "out": "export/output",
                   })())))


    for i, (label, fn) in enumerate(steps, 1):
        console.rule(f"[cyan]Step {i}/{len(steps)}: {label}")
        try:
            fn()
        except Exception as e:
            logger.error("Pipeline step '%s' failed: %s", label, e)
            console.print(f"[red]Step failed:[/red] {e}")
            if not getattr(args, "continue_on_error", True):
                break

    console.rule("[bold green]Pipeline Complete")
    console.print("[dim]Dashboard: output/dashboard.html")
    console.print("Network:   output/network.html")
    console.print("Exports:   export/output/")
    console.print("Alerts:    alerts/pending.json[/dim]")


# ---------------------------------------------------------------------------
# CMD: cleanup (remove adults that slipped past age filters)
# ---------------------------------------------------------------------------

def cmd_cleanup(args):
    console.rule("[bold cyan]CLEANUP — REMOVING ADULTS FROM DB")
    from utils.helpers import is_minor
    from datetime import date

    db = _get_db()

    # Records with explicit age >= 18
    confirmed_adults = db.query(MissingPerson).filter(
        MissingPerson.age_at_disappearance >= 18
    ).all()

    # Records where DOB tells us they were 18+ at disappearance
    dob_adults = []
    for r in db.query(MissingPerson).filter(
        MissingPerson.age_at_disappearance == None,
        MissingPerson.date_of_birth != None,
    ).all():
        if not is_minor(None, r.date_of_birth, r.date_missing):
            dob_adults.append(r)

    to_delete = confirmed_adults + dob_adults

    console.print(f"  Records with age >= 18:          [red]{len(confirmed_adults):,}[/red]")
    console.print(f"  Records where DOB indicates 18+: [red]{len(dob_adults):,}[/red]")
    console.print(f"  Total to remove:                 [bold red]{len(to_delete):,}[/bold red]")

    if not to_delete:
        console.print("[green]✓ Nothing to delete — DB is clean.[/green]")
        db.close()
        return

    if not getattr(args, "yes", False):
        confirm = input(f"\nDelete {len(to_delete):,} adult records? [y/N] ").strip().lower()
        if confirm != "y":
            console.print("[yellow]Aborted.[/yellow]")
            db.close()
            return

    for r in to_delete:
        db.delete(r)
    db.commit()

    # Verify
    remaining = db.query(MissingPerson).count()
    db.close()

    console.print(f"[green]✓ Deleted {len(to_delete):,} adult records.[/green]")
    console.print(f"[green]  Remaining records: {remaining:,}[/green]")


# ---------------------------------------------------------------------------
# CMD: status (quick DB overview)
# ---------------------------------------------------------------------------

def cmd_status(args):
    from sqlalchemy import func
    db = _get_db()

    total    = db.query(MissingPerson).count()
    active   = db.query(MissingPerson).filter(MissingPerson.is_resolved==False).count()
    resolved = db.query(MissingPerson).filter(MissingPerson.is_resolved==True).count()

    sources = db.query(MissingPerson.source, func.count(MissingPerson.id))\
                .group_by(MissingPerson.source).all()

    t = Table(title="MCID Database Status", show_header=True,
              header_style="bold cyan")
    t.add_column("Source",   style="cyan")
    t.add_column("Records",  justify="right")
    for s, n in sources:
        t.add_row(s or "unknown", str(n))

    console.print(Panel(
        f"[green]Total:[/green]    {total:,}\n"
        f"[red]Active:[/red]   {active:,}\n"
        f"[green]Resolved:[/green] {resolved:,}\n"
        f"[yellow]Rate:[/yellow]     {round(resolved/total*100,1) if total else 0}%",
        title="Summary",
    ))
    console.print(t)

    # Check file availability
    files = {
        "analysis/patterns.py":   Path("analysis/patterns.py").exists(),
        "analysis/enrichment.py": Path("analysis/enrichment.py").exists(),
        "analysis/network.py":    Path("analysis/network.py").exists(),
        "analysis/documents.py":  Path("analysis/documents.py").exists(),
        "alerts/monitor.py":      Path("alerts/monitor.py").exists(),
        "export/le_report.py":    Path("export/le_report.py").exists(),
        "export/ibase.py":        Path("export/ibase.py").exists(),
        "report.py":              Path("report.py").exists(),
    }
    ft = Table(title="Module Status", show_header=True, header_style="bold cyan")
    ft.add_column("Module")
    ft.add_column("Status", justify="center")
    for name, exists in files.items():
        ft.add_row(name, "[green]✓[/green]" if exists else "[red]✗ missing[/red]")
    console.print(ft)

    db.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        prog="mcid",
        description="Missing Children Intelligence Dashboard — Full OSINT Platform",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:
  python main.py status
  python main.py run all
  python main.py run interpol gmcn missing_people_uk
  python main.py patterns
  python main.py enrich --limit 200
  python main.py network
  python main.py monitor --watch
  python main.py docs court_doc.pdf
  python main.py report
  python main.py export --format all
  python main.py le-report --cluster GARCIA
  python main.py cleanup
  python main.py cleanup --yes
  python main.py pipeline
  python main.py pipeline --skip-scrape

SCRAPER GROUPS:
  us            ncmec, namus
  international interpol, gmcn, missing_people_uk
  official      all official sources
  media         news feeds
  all           everything
        """,
    )
    parser.add_argument("--db", default=DB_PATH,
                        help="Database path (default: missing_children.db)")

    sub = parser.add_subparsers(dest="command", metavar="COMMAND")

    # status
    sub.add_parser("status", help="Show DB stats and module availability")

    # run
    p_run = sub.add_parser("run", help="Run scrapers")
    p_run.add_argument("scrapers", nargs="*",
                       help="Scraper names or groups (default: all)")

    # schedule
    sub.add_parser("schedule", help="Run scrapers on schedule (daemon)")

    # patterns
    p_pat = sub.add_parser("patterns", help="Run pattern analysis")
    p_pat.add_argument("--min-cluster", type=int, default=2)
    p_pat.add_argument("--date-window", type=int, default=180)

    # enrich
    p_enr = sub.add_parser("enrich", help="Run OSINT enrichment pipeline")
    p_enr.add_argument("--limit",    type=int, default=None,
                       help="Max cases to enrich")
    p_enr.add_argument("--source",   default=None,
                       help="Single enricher: courtlistener|doj|europol|"
                            "fbi|google_news|opensanctions")
    p_enr.add_argument("--case-id",  type=int, default=None,
                       help="Enrich single DB case by id")

    # network
    p_net = sub.add_parser("network", help="Build network graph")
    p_net.add_argument("--min-connections", type=int, default=1)

    # monitor
    p_mon = sub.add_parser("monitor", help="Run alert monitor")
    p_mon.add_argument("--watch",    action="store_true",
                       help="Daemon mode")
    p_mon.add_argument("--test",     action="store_true",
                       help="Send test notification")
    p_mon.add_argument("--interval", type=int, default=30,
                       help="Minutes between checks (watch mode)")

    # docs
    p_doc = sub.add_parser("docs", help="Process PDF document(s)")
    p_doc.add_argument("input", help="PDF file or directory")

    # report
    p_rep = sub.add_parser("report", help="Generate HTML dashboard")
    p_rep.add_argument("--skip-patterns", action="store_true")
    p_rep.add_argument("--out", default="output/dashboard.html")

    # export
    p_exp = sub.add_parser("export", help="Export data")
    p_exp.add_argument("--format", default="csv",
                       choices=["csv","json","xml","all"])
    p_exp.add_argument("--clusters-only", action="store_true")
    p_exp.add_argument("--since", default=None, help="YYYY-MM-DD")
    p_exp.add_argument("--out",   default="export/output")

    # le-report
    p_le = sub.add_parser("le-report", help="Generate LE PDF briefs")
    p_le.add_argument("--cluster", default=None, help="Surname")
    p_le.add_argument("--burst",   default=None, help="City name")
    p_le.add_argument("--all",     action="store_true",
                      help="All high-priority clusters")
    p_le.add_argument("--out",     default="export/output")

    # pipeline
    p_pip = sub.add_parser("pipeline", help="Run full pipeline")
    p_pip.add_argument("--skip-scrape",      action="store_true")
    p_pip.add_argument("--skip-patterns",    action="store_true")
    p_pip.add_argument("--continue-on-error",action="store_true", default=True)

    # cleanup
    p_cln = sub.add_parser("cleanup",
                            help="Remove adult records that slipped past age filters")
    p_cln.add_argument("--yes", "-y", action="store_true",
                       help="Skip confirmation prompt")

    args = parser.parse_args()

    # Attach db to args
    if not hasattr(args, "db"):
        args.db = DB_PATH

    dispatch = {
        "status":    cmd_status,
        "run":       cmd_run,
        "schedule":  cmd_schedule,
        "patterns":  cmd_patterns,
        "enrich":    cmd_enrich,
        "network":   cmd_network,
        "monitor":   cmd_monitor,
        "docs":      cmd_docs,
        "report":    cmd_report,
        "export":    cmd_export,
        "le-report": cmd_le_report,
        "pipeline":  cmd_pipeline,
        "cleanup":   cmd_cleanup,
    }

    if args.command in dispatch:
        dispatch[args.command](args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
