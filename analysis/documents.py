"""
analysis/documents.py
=====================
Phase 5: Document Intelligence

Processes PDFs and extracts:
  - Victim descriptions (age, nationality, location, date)
  - Named suspects and organizations
  - Case/statute references
  - Fuzzy-matches extracted entities against the DB

Supports:
  - Court documents
  - Europol annual reports
  - UNODC trafficking reports
  - NGO field reports
  - Any PDF with relevant text

Dependencies (all free/open source):
  pip install pdfplumber spacy rapidfuzz
  python -m spacy download en_core_web_sm

Usage:
  python analysis/documents.py report.pdf
  python analysis/documents.py /path/to/docs/  # process directory
  python analysis/documents.py report.pdf --out analysis/output/doc_findings.json
"""

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from rapidfuzz import fuzz, process as fz_process

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.models import init_db, MissingPerson

DB_PATH = "missing_children.db"

# ---------------------------------------------------------------------------
# NLP setup — spaCy for NER, fallback to regex if not installed
# ---------------------------------------------------------------------------

NLP = None

def load_nlp():
    global NLP
    if NLP is not None:
        return NLP
    try:
        import spacy
        try:
            NLP = spacy.load("en_core_web_sm")
        except OSError:
            print("spaCy model not found. Run: python -m spacy download en_core_web_sm")
            print("Falling back to regex-only extraction.")
            NLP = False
    except ImportError:
        print("spaCy not installed. Run: pip install spacy --break-system-packages")
        print("Falling back to regex-only extraction.")
        NLP = False
    return NLP


def load_pdf(path):
    """Extract text from PDF using pdfplumber."""
    try:
        import pdfplumber
    except ImportError:
        print("pdfplumber not installed. Run: pip install pdfplumber --break-system-packages")
        return None

    text_pages = []
    try:
        with pdfplumber.open(path) as pdf:
            for i, page in enumerate(pdf.pages):
                text = page.extract_text()
                if text:
                    text_pages.append({"page": i+1, "text": text})
    except Exception as e:
        print(f"PDF error {path}: {e}")
        return None
    return text_pages


# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

# Age patterns: "14-year-old", "aged 14", "age 14", "minor aged 14"
AGE_PATTERNS = [
    r'\b(\d{1,2})[- ]?year[- ]?old\b',
    r'\baged?\s+(\d{1,2})\b',
    r'\b(\d{1,2})[- ]?yo\b',
    r'\bminor[,\s]+age[d]?\s+(\d{1,2})\b',
    r'\bvictim[,\s]+(\d{1,2})\b',
    r'\b(\d{1,2})[- ]?years?\s+of\s+age\b',
]

# Nationality/origin patterns
NAT_PATTERNS = [
    r'\b(Guatemalan|Honduran|Mexican|Salvadoran|Nicaraguan|Haitian|Dominican)\b',
    r'\b(Nigerian|Ghanaian|Kenyan|South African|Congolese|Camerounian)\b',
    r'\b(Romanian|Ukrainian|Moldovan|Bulgarian|Albanian|Belarusian)\b',
    r'\b(Filipino|Indonesian|Thai|Vietnamese|Cambodian|Burmese|Myanmar)\b',
    r'\b(Indian|Bangladeshi|Pakistani|Nepali|Sri Lankan)\b',
    r'\b(Colombian|Ecuadorian|Peruvian|Bolivian|Venezuelan|Brazilian|Argentine)\b',
    r'\b(Chinese|South Korean|Japanese|Taiwanese)\b',
    r'\b(Jamaican|Trinidadian|Barbadian)\b',
]

# Legal references
STATUTE_PATTERNS = [
    r'\b18\s+U\.?S\.?C\.?\s+[§§\s]*(\d{4}[a-z]?)\b',
    r'\bSection\s+(\d{4}[a-z]?)\b',
    r'\b(1591|1594|2251|2252|2422|2423|2241|2243)\b',
]

# Case number patterns
CASE_PATTERNS = [
    r'\b(\d{1,2}:\d{2}-[Cc][Rr]-\d{4,6})\b',      # 1:23-cr-00456
    r'\bCase\s+No\.?\s*([\w\-]+)\b',
    r'\bDocket\s+No\.?\s*([\w\-]+)\b',
]

# Date patterns
DATE_PATTERNS = [
    r'\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})\b',
    r'\b(\d{1,2})[/\-](\d{1,2})[/\-](\d{2,4})\b',
    r'\b(\d{4})[/\-](\d{1,2})[/\-](\d{1,2})\b',
]


def extract_ages(text):
    ages = []
    for pat in AGE_PATTERNS:
        for m in re.finditer(pat, text, re.IGNORECASE):
            try:
                a = int(m.group(1))
                if 0 <= a <= 17:
                    ages.append({"age": a, "context": text[max(0,m.start()-40):m.end()+40]})
            except Exception:
                pass
    return ages


def extract_nationalities(text):
    nats = []
    for pat in NAT_PATTERNS:
        for m in re.finditer(pat, text, re.IGNORECASE):
            nats.append(m.group(1))
    return list(set(nats))


def extract_statutes(text):
    statutes = []
    for pat in STATUTE_PATTERNS:
        for m in re.finditer(pat, text):
            statutes.append(m.group(0))
    return list(set(statutes))


def extract_case_numbers(text):
    cases = []
    for pat in CASE_PATTERNS:
        for m in re.finditer(pat, text, re.IGNORECASE):
            cases.append(m.group(1) if len(m.groups()) > 0 else m.group(0))
    return list(set(cases))


def extract_entities_spacy(text):
    """Extract persons, organizations, locations using spaCy NER."""
    nlp = load_nlp()
    if not nlp:
        return [], [], []

    persons = []
    orgs    = []
    locs    = []

    # Process in chunks to avoid memory issues with large docs
    chunk_size = 50000
    for i in range(0, len(text), chunk_size):
        chunk = text[i:i+chunk_size]
        try:
            doc = nlp(chunk)
            for ent in doc.ents:
                if ent.label_ == "PERSON" and len(ent.text.split()) >= 2:
                    persons.append(ent.text.strip())
                elif ent.label_ == "ORG":
                    orgs.append(ent.text.strip())
                elif ent.label_ in ("GPE", "LOC"):
                    locs.append(ent.text.strip())
        except Exception:
            pass

    return (
        list(set(persons[:200])),
        list(set(orgs[:100])),
        list(set(locs[:100])),
    )


def extract_entities_regex(text):
    """Fallback regex-based name extraction."""
    # Capitalized word pairs (rough person name heuristic)
    names = re.findall(
        r'\b([A-Z][a-z]+(?:\s[A-Z][a-z]+)+)\b', text
    )
    # Filter out common false positives
    stopwords = {
        "United States", "New York", "Los Angeles", "District Court",
        "Supreme Court", "Federal Bureau", "Department Justice",
        "United Nations", "Human Trafficking", "Task Force",
        "Press Release", "Case Number",
    }
    names = [n for n in names if n not in stopwords]
    return list(set(names[:200])), [], []


# ---------------------------------------------------------------------------
# DB matching
# ---------------------------------------------------------------------------

def match_against_db(persons, ages, nationalities, db):
    """
    Fuzzy-match extracted person names against the DB.
    Returns list of (db_record, matched_name, score) tuples.
    """
    # Build name index
    cases     = db.query(MissingPerson).filter(
        MissingPerson.is_resolved == False
    ).all()
    db_names  = [(c.full_name or "", c) for c in cases if c.full_name]

    matches = []
    for name in persons:
        if len(name) < 4:
            continue
        result = fz_process.extractOne(
            name,
            [n for n, _ in db_names],
            scorer=fuzz.token_sort_ratio,
            score_cutoff=75,
        )
        if result:
            matched_name, score, idx = result
            record = db_names[idx][1]

            # Boost score if age matches
            age_boost = 0
            if ages:
                for age_info in ages:
                    rec_age = record.age_at_disappearance
                    if rec_age and abs(rec_age - age_info["age"]) <= 2:
                        age_boost = 10
                        break

            # Boost if nationality matches
            nat_boost = 0
            if nationalities and record.nationality:
                for nat in nationalities:
                    if nat.lower() in record.nationality.lower():
                        nat_boost = 10
                        break

            final_score = min(score + age_boost + nat_boost, 100) / 100.0
            if final_score >= 0.70:
                matches.append({
                    "db_id":        record.id,
                    "db_name":      record.full_name,
                    "matched_name": name,
                    "score":        round(final_score, 3),
                    "source":       record.source,
                    "source_url":   record.source_url,
                    "country":      record.country_last_seen,
                    "age":          record.age_at_disappearance,
                    "photo":        record.photo_url,
                })

    return sorted(matches, key=lambda x: x["score"], reverse=True)


# ---------------------------------------------------------------------------
# Document processor
# ---------------------------------------------------------------------------

def process_document(pdf_path, db):
    """Process a single PDF and return findings dict."""
    print(f"Processing: {pdf_path}")
    pages = load_pdf(pdf_path)
    if not pages:
        return None

    full_text    = "\n\n".join(p["text"] for p in pages)
    word_count   = len(full_text.split())
    print(f"  {len(pages)} pages, {word_count:,} words")

    # Extract entities
    nlp = load_nlp()
    if nlp:
        persons, orgs, locs = extract_entities_spacy(full_text)
        print(f"  NER: {len(persons)} persons, {len(orgs)} orgs, {len(locs)} locations")
    else:
        persons, orgs, locs = extract_entities_regex(full_text)
        print(f"  Regex: {len(persons)} name candidates")

    ages          = extract_ages(full_text)
    nationalities = extract_nationalities(full_text)
    statutes      = extract_statutes(full_text)
    case_numbers  = extract_case_numbers(full_text)

    print(f"  Ages found: {len(ages)}, Nationalities: {len(nationalities)}")
    print(f"  Statutes: {statutes[:5]}, Cases: {case_numbers[:5]}")

    # Match against DB
    db_matches = match_against_db(persons, ages, nationalities, db)
    print(f"  DB matches: {len(db_matches)}")

    # Save findings to enrichment table
    saved = 0
    if db_matches:
        try:
            from sqlalchemy import text as sqlt
            for match in db_matches:
                db.execute(sqlt("""
                    INSERT OR IGNORE INTO enrichment_findings
                    (missing_person_id, source_type, source_name, title,
                     url, snippet, relevance_score, finding_type, raw_data)
                    VALUES (:pid, :stype, :sname, :title, :url,
                            :snippet, :rel, :ftype, :raw)
                """), {
                    "pid":     match["db_id"],
                    "stype":   "document",
                    "sname":   str(pdf_path),
                    "title":   f"Document match: {match['matched_name']}",
                    "url":     "",
                    "snippet": f"Matched '{match['matched_name']}' in {pdf_path.name}",
                    "rel":     match["score"],
                    "ftype":   "DOCUMENT_MATCH",
                    "raw":     json.dumps(match),
                })
                saved += 1
            db.commit()
        except Exception as e:
            print(f"  DB save error: {e}")
            db.rollback()

    return {
        "file":          str(pdf_path),
        "pages":         len(pages),
        "word_count":    word_count,
        "persons":       persons[:50],
        "organizations": orgs[:30],
        "locations":     locs[:30],
        "ages_found":    ages[:20],
        "nationalities": nationalities,
        "statutes":      statutes,
        "case_numbers":  case_numbers,
        "db_matches":    db_matches,
        "findings_saved":saved,
        "processed_at":  datetime.now().isoformat(),
    }


def process_directory(dir_path, db):
    """Process all PDFs in a directory."""
    pdfs = list(Path(dir_path).rglob("*.pdf"))
    print(f"Found {len(pdfs)} PDFs in {dir_path}")
    results = []
    for pdf in pdfs:
        result = process_document(pdf, db)
        if result:
            results.append(result)
    return results


# ---------------------------------------------------------------------------
# Report builder
# ---------------------------------------------------------------------------

def build_doc_report(findings_list):
    lines = []
    w = lines.append
    w("="*70)
    w("  DOCUMENT INTELLIGENCE REPORT")
    w(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    w("="*70)

    for f in findings_list:
        if not f:
            continue
        w(f"\nFILE: {f['file']}")
        w(f"  Pages: {f['pages']}  |  Words: {f['word_count']:,}")
        w(f"  Statutes referenced: {', '.join(f['statutes']) or 'none'}")
        w(f"  Case numbers: {', '.join(f['case_numbers'][:5]) or 'none'}")
        w(f"  Nationalities: {', '.join(f['nationalities']) or 'none'}")
        w(f"  Age mentions: {len(f['ages_found'])}")

        if f["db_matches"]:
            w(f"\n  DB MATCHES ({len(f['db_matches'])}):")
            for m in f["db_matches"][:10]:
                w(f"    [{m['score']:.2f}] '{m['matched_name']}' → DB: {m['db_name']}")
                w(f"           Source: {m['source']} | Country: {m.get('country','?')}")
                if m.get("source_url"):
                    w(f"           URL: {m['source_url']}")

    w("\n" + "="*70)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Document Intelligence")
    parser.add_argument("input",   help="PDF file or directory of PDFs")
    parser.add_argument("--db",    default=DB_PATH)
    parser.add_argument("--out",   default="analysis/output/doc_findings.json")
    args = parser.parse_args()

    engine, Session = init_db(args.db)

    # Ensure enrichment table exists
    try:
        from analysis.enrichment import ensure_enrichment_table
        ensure_enrichment_table(engine)
    except ImportError:
        try:
            from enrichment import ensure_enrichment_table
            ensure_enrichment_table(engine)
        except Exception:
            pass

    db = Session()

    input_path = Path(args.input)
    if input_path.is_dir():
        results = process_directory(input_path, db)
    elif input_path.suffix.lower() == ".pdf":
        result = process_document(input_path, db)
        results = [result] if result else []
    else:
        print(f"Error: {args.input} is not a PDF or directory")
        sys.exit(1)

    db.close()

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nJSON → {args.out}")

    report = build_doc_report(results)
    txt_path = args.out.replace(".json", ".txt")
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"TXT  → {txt_path}")
    print(report)
