"""Seed reference data: conferences, courses, division lap profiles."""

from __future__ import annotations

import logging

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# ── Courses ──────────────────────────────────────────────────────────
# Venue name → event name patterns that map to this course
COURSES = {
    "Granite": {
        "location": "Granite, PA",
        "patterns": ["Granite"],
    },
    "Johnstown": {
        "location": "Johnstown, PA",
        "patterns": ["Johnstown"],
    },
    "Boyce": {
        "location": "Boyce Park, PA",
        "patterns": ["Boyce"],
    },
    "Blue Mountain": {
        "location": "Blue Mountain, PA",
        "patterns": ["Blue Mtn", "Blue Mountain"],
    },
    "Fair Hill": {
        "location": "Fair Hill, MD",
        "patterns": ["Fair Hill", "FairHill"],
    },
    "Penn College": {
        "location": "Williamsport, PA",
        "patterns": ["Penn College"],
    },
    "Oesterling": {
        "location": "Oesterling, PA",
        "patterns": ["Osterling", "Oesterling"],
    },
    "Coleman": {
        "location": "Coleman, PA",
        "patterns": ["Coleman"],
    },
    "Wainer": {
        "location": "Wainer, PA",
        "patterns": ["Wainer"],
    },
    "Belmont": {
        "location": "Belmont, PA",
        "patterns": ["Belmont"],
    },
    "Alameda": {
        "location": "Alameda, PA",
        "patterns": ["Alameda"],
    },
    "Harvest Fields": {
        "location": "Harvest Fields, PA",
        "patterns": ["Harvest Fields"],
    },
    "Hershey": {
        "location": "Hershey, PA",
        "patterns": ["Hershey"],
    },
}

# ── Division lap profiles (from PICL spreadsheet) ────────────────────
# These are the standard profiles; may vary slightly by course/year
DIVISION_PROFILES = [
    # (division, gender, laps, max_duration_mins, cutoff_mins)
    ("Varsity", "Male", 4, 90, 68),
    ("Varsity", "Female", 4, 90, 68),
    ("JV1", "Male", 3, 75, 50),
    ("JV1", "Female", 3, 75, 50),
    ("JV2", "Male", 2, 75, 38),
    ("JV2", "Female", 2, 75, 38),
    ("JV3", "Male", 2, 75, 38),
    ("JV3", "Female", 2, 75, 38),
    ("Middle School Advanced", "Male", 4, 60, 45),
    ("Middle School Advanced", "Female", 4, 60, 45),
    ("MS Advanced", "Male", 4, 60, 45),
    ("MS Advanced", "Female", 4, 60, 45),
    ("8th Grade", "Male", 2, 45, 23),
    ("8th Grade", "Female", 2, 45, 23),
    ("7th Grade", "Male", 2, 45, 23),
    ("7th Grade", "Female", 2, 45, 23),
    ("6th Grade", "Male", 2, 45, 23),
    ("6th Grade", "Female", 2, 45, 23),
    ("5th Grade", "Male", 2, 45, 23),
    ("5th Grade", "Female", 2, 45, 23),
    ("Single Lap High School", None, 1, None, None),
    ("Single Lap Middle School", None, 1, None, None),
]

# ── Conference lineage ──────────────────────────────────────────────
# Maps 2025 conferences back to their historical grouping
CONFERENCE_LINEAGE = {
    "Eastern": "Eastern",
    "Eastern Blue": "Eastern",
    "Eastern  Blue": "Eastern",  # double-space variant in data
    "Eastern Gold": "Eastern",
    "Central": "Central",
    "Western": "Western",
}


def seed_courses(session: Session) -> dict[str, int]:
    """Insert courses and return name→id mapping."""
    course_ids: dict[str, int] = {}
    for name, info in COURSES.items():
        session.execute(text(
            "INSERT INTO courses (name, location) VALUES (:name, :loc) "
            "ON CONFLICT (name) DO NOTHING"
        ), {"name": name, "loc": info.get("location")})

    # Fetch IDs
    rows = session.execute(text("SELECT id, name FROM courses")).all()
    course_ids = {r[1]: r[0] for r in rows}
    logger.info("Seeded %d courses", len(course_ids))
    return course_ids


def map_events_to_courses(session: Session, course_ids: dict[str, int]) -> int:
    """Map events to courses based on event name patterns."""
    count = 0
    events = session.execute(text(
        "SELECT id, event_name FROM events WHERE course_id IS NULL"
    )).all()

    for event_id, event_name in events:
        for course_name, info in COURSES.items():
            if any(p.lower() in event_name.lower() for p in info["patterns"]):
                session.execute(text(
                    "UPDATE events SET course_id = :cid WHERE id = :eid"
                ), {"cid": course_ids[course_name], "eid": event_id})
                count += 1
                break

    logger.info("Mapped %d events to courses", count)
    return count


def seed_division_laps(session: Session, course_ids: dict[str, int]) -> int:
    """Seed division lap profiles for all courses."""
    count = 0
    for course_name, course_id in course_ids.items():
        for div, gender, laps, max_dur, cutoff in DIVISION_PROFILES:
            session.execute(text("""
                INSERT INTO division_laps (course_id, division, gender, lap_count,
                    max_duration_mins, cutoff_mins)
                VALUES (:cid, :div, :gender, :laps, :max_dur, :cutoff)
                ON CONFLICT (course_id, division, gender, season)
                DO UPDATE SET lap_count = :laps, max_duration_mins = :max_dur,
                    cutoff_mins = :cutoff
            """), {
                "cid": course_id, "div": div, "gender": gender,
                "laps": laps, "max_dur": max_dur, "cutoff": cutoff,
            })
            count += 1

    logger.info("Seeded %d division-lap profiles", count)
    return count


def seed_conferences(session: Session) -> int:
    """Derive team→conference mapping from results data."""
    count = 0

    # Extract from results where conference field is populated
    rows = session.execute(text("""
        SELECT DISTINCT ri.team, e.season, r.conference
        FROM results r
        JOIN riders ri ON r.rider_id = ri.id
        JOIN events e ON r.event_id = e.id
        WHERE r.conference IS NOT NULL
          AND r.conference != ''
          AND r.conference != 'Conference'
          AND ri.team IS NOT NULL
        ORDER BY e.season, r.conference, ri.team
    """)).all()

    for team, season, conference in rows:
        # Normalize conference name (fix double spaces)
        conf = conference.strip()
        conf_group = CONFERENCE_LINEAGE.get(conf, conf)

        session.execute(text("""
            INSERT INTO team_conferences (team, season, conference, conference_group, source)
            VALUES (:team, :season, :conf, :group, 'derived')
            ON CONFLICT (team, season) DO UPDATE
            SET conference = :conf, conference_group = :group
        """), {"team": team, "season": season, "conf": conf, "group": conf_group})
        count += 1

    logger.info("Seeded %d team-conference mappings", count)
    return count


def seed_all(session: Session) -> None:
    """Run all seed operations."""
    course_ids = seed_courses(session)
    map_events_to_courses(session, course_ids)
    seed_division_laps(session, course_ids)
    seed_conferences(session)
    session.commit()
    logger.info("Seed complete")
