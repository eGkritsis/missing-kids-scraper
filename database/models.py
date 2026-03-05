"""
database/models.py
SQLAlchemy models and database initialization for the Missing Children Tracker.
"""

from sqlalchemy import (
    create_engine, Column, Integer, String, Text, Date,
    DateTime, Float, Boolean, UniqueConstraint, Index
)
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime

Base = declarative_base()


class MissingPerson(Base):
    """Core record for a missing child."""
    __tablename__ = "missing_persons"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Identity
    source = Column(String(64), nullable=False)          # ncmec | namus | news | social
    source_id = Column(String(128))                      # ID from the originating database
    source_url = Column(Text)                            # Direct link to original record

    # Personal info
    first_name = Column(String(128))
    last_name = Column(String(128))
    full_name = Column(String(256))
    date_of_birth = Column(Date)
    age_at_disappearance = Column(Integer)
    current_age_estimate = Column(String(32))            # e.g. "16-18" for aged progressions
    gender = Column(String(32))
    race_ethnicity = Column(String(128))
    nationality = Column(String(128))

    # Physical description
    height_cm = Column(Float)
    weight_kg = Column(Float)
    eye_color = Column(String(64))
    hair_color = Column(String(64))
    hair_length = Column(String(64))
    distinguishing_marks = Column(Text)

    # Disappearance details
    date_missing = Column(Date)
    city_last_seen = Column(String(128))
    state_last_seen = Column(String(128))
    country_last_seen = Column(String(128), default="USA")
    circumstances = Column(Text)
    case_type = Column(String(64))                       # Endangered Runaway | Abduction | etc.

    # Case status
    is_resolved = Column(Boolean, default=False)
    resolution_notes = Column(Text)

    # Media
    photo_url = Column(Text)
    has_age_progression = Column(Boolean, default=False)

    # Contact
    contact_agency = Column(String(256))
    contact_phone = Column(String(64))
    ncic_number = Column(String(64))
    namus_id = Column(String(64))

    # Meta
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    raw_data = Column(Text)                              # JSON dump of original record

    __table_args__ = (
        UniqueConstraint("source", "source_id", name="uq_source_record"),
        Index("idx_name", "first_name", "last_name"),
        Index("idx_date_missing", "date_missing"),
        Index("idx_location", "state_last_seen", "country_last_seen"),
    )

    def __repr__(self):
        return f"<MissingPerson {self.full_name} ({self.source}/{self.source_id})>"


class NewsArticle(Base):
    """News article referencing a missing child case."""
    __tablename__ = "news_articles"

    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(Text, unique=True, nullable=False)
    title = Column(Text)
    summary = Column(Text)
    source_name = Column(String(256))
    published_at = Column(DateTime)
    names_mentioned = Column(Text)                       # comma-separated extracted names
    scraped_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_published", "published_at"),
    )


class ScraperRun(Base):
    """Audit log of every scraper execution."""
    __tablename__ = "scraper_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    scraper_name = Column(String(64), nullable=False)
    started_at = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime)
    records_found = Column(Integer, default=0)
    records_new = Column(Integer, default=0)
    records_updated = Column(Integer, default=0)
    errors = Column(Integer, default=0)
    status = Column(String(32), default="running")       # running | success | partial | failed
    notes = Column(Text)


def init_db(db_path: str = "missing_children.db"):
    """Create the database and all tables."""
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return engine, Session
