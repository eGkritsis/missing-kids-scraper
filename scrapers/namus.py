"""
scrapers/namus.py
"""
import time
from database.models import MissingPerson
from scrapers.base import BaseScraper
from utils.helpers import clean_text, parse_date, safe_json

NAMUS_SEARCH = "https://www.namus.gov/api/CaseSets/NamUs/MissingPersons/search"

PROJECTIONS = [
    "namus2Number", "firstName", "lastName",
    "computedMissingMinAge", "computedMissingMaxAge",
    "raceEthnicity",
    "cityOfLastContact", "countyOfLastContact",
    "stateOfLastContact",
]

TAKE = 250


class NamusScraper(BaseScraper):
    name = "namus"

    WORKERS = 8

    def run(self) -> dict:
        import requests as req
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from database.models import init_db

        self.logger.info("Starting NamUs scrape (parallel, %d workers)...", self.WORKERS)

        # First request to get total count
        resp = self.http.post(NAMUS_SEARCH, json={"take": 1, "skip": 0, "projections": ["namus2Number"]}, timeout=30)
        resp.raise_for_status()
        total = resp.json().get("count", 0)
        self.logger.info("Total records: %d", total)

        skips = list(range(0, total, TAKE))
        found = new = updated = errors = 0
        lock = __import__("threading").Lock()

        # Each worker gets its own DB session and HTTP session
        _, Session = init_db()

        def fetch_batch(skip):
            session = Session()
            http = __import__("requests").Session()
            http.headers.update(self.http.headers)
            nonlocal found, new, updated, errors
            try:
                time.sleep(0.5)
                r = http.post(NAMUS_SEARCH, json={"take": TAKE, "skip": skip, "projections": PROJECTIONS}, timeout=30)
                r.raise_for_status()
                rows = r.json().get("results", [])
                batch_new = batch_updated = 0
                for row in rows:
                    try:
                        source_id = str(row["namus2Number"])
                        data = _build_update(row)
                        inst = session.query(MissingPerson).filter_by(source="namus", source_id=source_id).first()
                        if inst is None:
                            inst = MissingPerson(source="namus", source_id=source_id, **data)
                            session.add(inst)
                            batch_new += 1
                        else:
                            for k,v in data.items(): setattr(inst, k, v)
                            batch_updated += 1
                        session.commit()
                    except Exception as e:
                        session.rollback()
                        self.logger.debug("Row error (skipped): %s", e)
                with lock:
                    found[0] += len(rows)
                    new[0] += batch_new
                    updated[0] += batch_updated
                self.logger.info("skip=%d done (%d records)", skip, len(rows))
                return len(rows)
            except Exception as exc:
                self.logger.error("Batch skip=%d failed: %s", skip, exc)
                with lock: errors[0] += 1
                return 0
            finally:
                session.close()

        # Use mutable containers for nonlocal counter sharing
        found = [0]; new = [0]; updated = [0]; errors = [0]

        with ThreadPoolExecutor(max_workers=self.WORKERS) as pool:
            futures = {pool.submit(fetch_batch, s): s for s in skips}
            for fut in as_completed(futures):
                pass  # logging happens inside fetch_batch

        self.logger.info("NamUs done. found=%d new=%d updated=%d errors=%d",
                         found[0], new[0], updated[0], errors[0])
        return {"found": found[0], "new": new[0], "updated": updated[0], "errors": errors[0]}

    def _upsert(self, row: dict) -> tuple:
        case_id = str(row["namus2Number"])

        sex = row.get("sex")
        gender = sex.get("name") if isinstance(sex, dict) else sex

        re_raw = row.get("raceEthnicity") or []
        if isinstance(re_raw, list):
            race = ", ".join(
                r.get("name", str(r)) if isinstance(r, dict) else str(r)
                for r in re_raw
            )
        else:
            race = str(re_raw)

        update_data = {
            "source_url":           f"https://www.namus.gov{row.get('link', '')}",
            "first_name":           clean_text(row.get("firstName")),
            "last_name":            clean_text(row.get("lastName")),
            "full_name":            clean_text(f"{row.get('firstName','')} {row.get('lastName','')}".strip()),
            "date_of_birth":        parse_date(row.get("dateOfBirth")),
            "age_at_disappearance": row.get("computedMissingMinAge"),
            "gender":               clean_text(gender),
            "race_ethnicity":       clean_text(race),
            "date_missing":         parse_date(row.get("dateMissing")),
            "city_last_seen":       clean_text(row.get("cityOfLastContact")),
            "state_last_seen":      clean_text(row.get("stateOfLastContact")),
            "country_last_seen":    "USA",
            "namus_id":             case_id,
            "raw_data":             safe_json(row),
        }

        return self.upsert(
            MissingPerson,
            lookup_kwargs={"source": "namus", "source_id": case_id},
            update_kwargs=update_data,
        )


def _build_update(row: dict) -> dict:
    from utils.helpers import clean_text, parse_date, safe_json
    re_raw = row.get("raceEthnicity") or []
    if isinstance(re_raw, list):
        race = ", ".join(r.get("name", str(r)) if isinstance(r, dict) else str(r) for r in re_raw)
    else:
        race = str(re_raw)
    return {
        "source_url":           f"https://www.namus.gov/MissingPersons/Case#/{row['namus2Number']}",
        "first_name":           clean_text(row.get("firstName")),
        "last_name":            clean_text(row.get("lastName")),
        "full_name":            clean_text(f"{row.get('firstName','')} {row.get('lastName','')}".strip()),
        "age_at_disappearance": row.get("computedMissingMinAge"),
        "race_ethnicity":       clean_text(race),
        "city_last_seen":       clean_text(row.get("cityOfLastContact")),
        "state_last_seen":      clean_text(row.get("stateOfLastContact")),
        "country_last_seen":    "USA",
        "namus_id":             str(row["namus2Number"]),
        "raw_data":             safe_json(row),
    }
