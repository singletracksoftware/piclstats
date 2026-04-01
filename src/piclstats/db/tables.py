"""SQLAlchemy Core table definitions."""

from sqlalchemy import (
    Column,
    DateTime,
    Index,
    Integer,
    Interval,
    MetaData,
    SmallInteger,
    Table,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB

metadata = MetaData()

events = Table(
    "events",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("raceresult_id", Integer, nullable=False, unique=True),
    Column("season", SmallInteger, nullable=False),
    Column("event_name", Text, nullable=False),
    Column("event_order", SmallInteger),
    Column("scraped_at", DateTime(timezone=True), server_default=func.now()),
    Index("idx_events_season", "season"),
)

riders = Table(
    "riders",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", Text, nullable=False),
    Column("team", Text),
    Column("school", Text),
    UniqueConstraint("name", "team", name="uq_riders_name_team"),
    Index("idx_riders_name", "name"),
)

results = Table(
    "results",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("event_id", Integer, nullable=False),
    Column("rider_id", Integer, nullable=False),
    Column("bib", Integer, nullable=False),
    Column("category", Text, nullable=False),
    Column("category_order", SmallInteger),
    Column("gender", Text),
    Column("division", Text),
    Column("place", SmallInteger),
    Column("status", Text),
    Column("points", SmallInteger),
    Column("conference", Text),
    Column("lap1", Interval),
    Column("lap2", Interval),
    Column("lap3", Interval),
    Column("lap4", Interval),
    Column("lap5", Interval),
    Column("lap6", Interval),
    Column("penalty", Interval),
    Column("total_time", Interval),
    Column("total_time_raw", Text, nullable=False),
    Column("raw_data", JSONB),
    UniqueConstraint("event_id", "bib", name="uq_results_event_bib"),
    Index("idx_results_rider", "rider_id"),
    Index("idx_results_category", "category"),
    Index("idx_results_event_category", "event_id", "category"),
    Index("idx_results_conference", "conference", postgresql_where=Column("conference").isnot(None)),
)
