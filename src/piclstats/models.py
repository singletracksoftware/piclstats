"""Data models for scraped race results."""

from dataclasses import dataclass, field
from datetime import timedelta


@dataclass(frozen=True)
class EventConfig:
    """Parsed config from raceresult.com config endpoint."""

    raceresult_id: int
    event_name: str
    server: str
    key: str
    result_list_name: str


@dataclass(frozen=True)
class RaceResult:
    """Single rider result row, parsed and normalized."""

    bib: int
    place: int | None
    status: str  # OK, DNF, DNS, DSQ
    name: str
    team: str | None
    school: str | None
    conference: str | None
    category: str  # e.g. "Varsity - Male"
    category_order: int
    gender: str | None
    division: str | None
    points: int | None
    lap1: timedelta | None
    lap2: timedelta | None
    lap3: timedelta | None
    lap4: timedelta | None
    lap5: timedelta | None
    lap6: timedelta | None
    penalty: timedelta | None
    total_time: timedelta | None
    total_time_raw: str
    raw_row: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class EventResults:
    """All results for a single event."""

    config: EventConfig
    season: int
    event_order: int
    results: list[RaceResult]
