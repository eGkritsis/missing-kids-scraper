"""
scrapers/base.py
Abstract base class all scrapers inherit from.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session as DBSession

from database.models import ScraperRun
from utils.helpers import build_session, setup_logger, is_minor


class BaseScraper(ABC):
    """
    Every scraper must implement `run()` and return a summary dict.
    The base class handles:
      - HTTP session creation
      - Audit logging to scraper_runs table
      - Consistent error handling
    """

    name: str = "base"

    def __init__(self, db_session: DBSession):
        self.db = db_session
        self.http = build_session()
        self.logger = setup_logger(self.name)
        self._run_record: Optional[ScraperRun] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def execute(self) -> dict:
        """Public entry-point. Wraps run() with audit logging."""
        self._run_record = ScraperRun(scraper_name=self.name)
        self.db.add(self._run_record)
        self.db.commit()

        try:
            summary = self.run()
            self._run_record.status = "success"
            self._run_record.records_found = summary.get("found", 0)
            self._run_record.records_new = summary.get("new", 0)
            self._run_record.records_updated = summary.get("updated", 0)
            self._run_record.errors = summary.get("errors", 0)
        except Exception as exc:
            self.logger.exception("Scraper %s failed: %s", self.name, exc)
            self._run_record.status = "failed"
            self._run_record.notes = str(exc)
            summary = {"found": 0, "new": 0, "updated": 0, "errors": 1}
        finally:
            self._run_record.finished_at = datetime.utcnow()
            self.db.commit()

        return summary

    @abstractmethod
    def run(self) -> dict:
        """Implement scraping logic. Return dict with found/new/updated/errors keys."""
        ...

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def upsert(self, model_class, lookup_kwargs: dict, update_kwargs: dict) -> tuple:
        """
        Insert or update a record.
        Skips any record confirmed to be 18+ at time of disappearance.
        Returns (instance, created: bool)  or  (None, False) if skipped.
        """
        from database.models import MissingPerson

        # --- Minor-only gate ---
        if model_class is MissingPerson:
            age = update_kwargs.get("age_at_disappearance")
            dob = update_kwargs.get("date_of_birth")
            dm  = update_kwargs.get("date_missing")
            if not is_minor(age, dob, dm):
                self.logger.debug(
                    "Skipping adult record: %s (age=%s)",
                    update_kwargs.get("full_name", "?"), age,
                )
                return None, False

        instance = self.db.query(model_class).filter_by(**lookup_kwargs).first()
        created = False
        if instance is None:
            instance = model_class(**lookup_kwargs, **update_kwargs)
            self.db.add(instance)
            created = True
        else:
            for k, v in update_kwargs.items():
                setattr(instance, k, v)
        self.db.commit()
        return instance, created
