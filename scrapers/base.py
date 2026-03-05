from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional
from sqlalchemy.orm import Session as DBSession
from database.models import ScraperRun
from utils.helpers import build_session, setup_logger


class BaseScraper(ABC):
    name: str = "base"

    def __init__(self, db_session: DBSession):
        self.db = db_session
        self.http = build_session()
        self.logger = setup_logger(self.name)
        self._run_record: Optional[ScraperRun] = None

    def execute(self) -> dict:
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
    def run(self) -> dict: ...

    def upsert(self, model_class, lookup_kwargs: dict, update_kwargs: dict) -> tuple:
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
