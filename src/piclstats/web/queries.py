"""SQL queries powering the dashboard."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.orm import Session


def _serialize(row) -> dict:
    """Convert a RowMapping to a plain dict with JSON-safe values."""
    d = dict(row)
    for k, v in d.items():
        if isinstance(v, timedelta):
            total = int(v.total_seconds())
            hours, rem = divmod(total, 3600)
            minutes, seconds = divmod(rem, 60)
            d[k] = f"{hours}:{minutes:02d}:{seconds:02d}" if hours else f"{minutes}:{seconds:02d}"
        elif isinstance(v, Decimal):
            d[k] = float(v)
    return d


def overview_stats(session: Session) -> dict:
    """High-level counts for the dashboard home."""
    row = session.execute(text("""
        SELECT
            count(DISTINCT e.id) AS events,
            count(DISTINCT r.rider_id) AS riders,
            count(DISTINCT ri.team) AS teams,
            count(r.id) AS results,
            count(DISTINCT e.season) AS seasons
        FROM results r
        JOIN events e ON r.event_id = e.id
        JOIN riders ri ON r.rider_id = ri.id
    """)).one()
    return _serialize(row._mapping)


def seasons_list(session: Session) -> list[int]:
    rows = session.execute(text(
        "SELECT DISTINCT season FROM events ORDER BY season"
    )).all()
    return [r[0] for r in rows]


def divisions_list(session: Session) -> list[str]:
    rows = session.execute(text(
        "SELECT DISTINCT division FROM results WHERE division IS NOT NULL ORDER BY division"
    )).all()
    return [r[0] for r in rows]


def teams_list(session: Session) -> list[str]:
    rows = session.execute(text(
        "SELECT DISTINCT team FROM riders WHERE team IS NOT NULL ORDER BY team"
    )).all()
    return [r[0] for r in rows]


def search_riders(
    session: Session, q: str, team: str | None = None, season: int | None = None
) -> list[dict]:
    """Search riders by name, optionally filtered by team and season."""
    sql = """
        SELECT
            ri.id,
            ri.name,
            ri.team,
            count(DISTINCT r.event_id) AS race_count,
            count(DISTINCT e.season) AS seasons_active,
            round(avg(r.points)::numeric, 1) AS avg_points,
            round(avg(r.place)::numeric, 1) AS avg_place,
            min(e.season) AS first_season,
            max(e.season) AS last_season
        FROM riders ri
        JOIN results r ON r.rider_id = ri.id
        JOIN events e ON r.event_id = e.id
        WHERE ri.name ILIKE :q
    """
    params: dict = {"q": f"%{q}%"}
    if team:
        sql += " AND ri.team ILIKE :team"
        params["team"] = f"%{team}%"
    if season:
        sql += " AND e.season = :season"
        params["season"] = season
    sql += """
        GROUP BY ri.id, ri.name, ri.team
        ORDER BY avg_points DESC NULLS LAST, ri.name
        LIMIT 100
    """
    rows = session.execute(text(sql), params).all()
    return [_serialize(r._mapping) for r in rows]


def rider_detail(session: Session, rider_id: int) -> dict | None:
    """Full rider profile with race history."""
    info = session.execute(text("""
        SELECT ri.id, ri.name, ri.team, ri.school
        FROM riders ri WHERE ri.id = :id
    """), {"id": rider_id}).one_or_none()
    if not info:
        return None

    races = session.execute(text("""
        SELECT
            e.season,
            e.event_name,
            e.event_order,
            r.category,
            r.division,
            r.gender,
            r.place,
            r.points,
            r.status,
            r.conference,
            r.total_time,
            r.total_time_raw,
            r.lap1, r.lap2, r.lap3, r.lap4, r.lap5, r.lap6,
            r.penalty
        FROM results r
        JOIN events e ON r.event_id = e.id
        WHERE r.rider_id = :id
        ORDER BY e.season, e.event_order
    """), {"id": rider_id}).all()

    season_stats = session.execute(text("""
        SELECT
            e.season,
            count(*) AS races,
            round(avg(r.points)::numeric, 1) AS avg_points,
            round(avg(r.place)::numeric, 1) AS avg_place,
            min(r.place) AS best_place,
            max(r.points) AS best_points,
            sum(r.points) AS total_points,
            r.division AS primary_division
        FROM results r
        JOIN events e ON r.event_id = e.id
        WHERE r.rider_id = :id AND r.place IS NOT NULL
        GROUP BY e.season, r.division
        ORDER BY e.season
    """), {"id": rider_id}).all()

    # Percentile within category per race
    percentiles = session.execute(text("""
        WITH ranked AS (
            SELECT
                r.event_id,
                r.rider_id,
                r.category,
                r.place,
                count(*) OVER (PARTITION BY r.event_id, r.category) AS field_size,
                r.place::numeric / NULLIF(count(*) OVER (PARTITION BY r.event_id, r.category), 0) AS pct_rank
            FROM results r
            WHERE r.place IS NOT NULL
        )
        SELECT
            e.season,
            e.event_name,
            e.event_order,
            ranked.category,
            ranked.place,
            ranked.field_size,
            round(((1.0 - ranked.pct_rank) * 100)::numeric, 1) AS percentile
        FROM ranked
        JOIN events e ON ranked.event_id = e.id
        WHERE ranked.rider_id = :id
        ORDER BY e.season, e.event_order
    """), {"id": rider_id}).all()

    return {
        "info": _serialize(info._mapping),
        "races": [_serialize(r._mapping) for r in races],
        "season_stats": [_serialize(r._mapping) for r in season_stats],
        "percentiles": [_serialize(r._mapping) for r in percentiles],
    }


def search_teams(session: Session, q: str, season: int | None = None) -> list[dict]:
    """Search teams by name."""
    sql = """
        SELECT
            ri.team,
            count(DISTINCT ri.id) AS rider_count,
            count(DISTINCT r.event_id) AS race_count,
            count(DISTINCT e.season) AS seasons_active,
            round(avg(r.points)::numeric, 1) AS avg_points,
            round(avg(r.place)::numeric, 1) AS avg_place
        FROM riders ri
        JOIN results r ON r.rider_id = ri.id
        JOIN events e ON r.event_id = e.id
        WHERE ri.team ILIKE :q
    """
    params: dict = {"q": f"%{q}%"}
    if season:
        sql += " AND e.season = :season"
        params["season"] = season
    sql += """
        GROUP BY ri.team
        ORDER BY avg_points DESC NULLS LAST
        LIMIT 50
    """
    rows = session.execute(text(sql), params).all()
    return [_serialize(r._mapping) for r in rows]


def team_detail(session: Session, team_name: str, season: int | None = None) -> dict | None:
    """Full team profile."""
    params: dict = {"team": team_name}
    season_filter = ""
    if season:
        season_filter = "AND e.season = :season"
        params["season"] = season

    roster = session.execute(text(f"""
        SELECT
            ri.id,
            ri.name,
            r.division,
            r.gender,
            count(DISTINCT r.event_id) AS races,
            round(avg(r.points)::numeric, 1) AS avg_points,
            round(avg(r.place)::numeric, 1) AS avg_place,
            min(r.place) AS best_place,
            max(r.points) AS best_points,
            sum(r.points) AS total_points
        FROM riders ri
        JOIN results r ON r.rider_id = ri.id
        JOIN events e ON r.event_id = e.id
        WHERE ri.team = :team {season_filter}
          AND r.place IS NOT NULL
        GROUP BY ri.id, ri.name, r.division, r.gender
        ORDER BY r.division, avg_points DESC NULLS LAST
    """), params).all()

    division_summary = session.execute(text(f"""
        SELECT
            r.division,
            r.gender,
            count(DISTINCT ri.id) AS riders,
            round(avg(r.points)::numeric, 1) AS avg_points,
            round(avg(r.place)::numeric, 1) AS avg_place
        FROM riders ri
        JOIN results r ON r.rider_id = ri.id
        JOIN events e ON r.event_id = e.id
        WHERE ri.team = :team {season_filter}
          AND r.place IS NOT NULL
        GROUP BY r.division, r.gender
        ORDER BY r.division, r.gender
    """), params).all()

    event_performance = session.execute(text(f"""
        SELECT
            e.season,
            e.event_name,
            e.event_order,
            count(DISTINCT ri.id) AS riders,
            round(avg(r.points)::numeric, 1) AS avg_points,
            round(avg(r.place)::numeric, 1) AS avg_place,
            sum(r.points) AS total_points
        FROM riders ri
        JOIN results r ON r.rider_id = ri.id
        JOIN events e ON r.event_id = e.id
        WHERE ri.team = :team {season_filter}
        GROUP BY e.season, e.event_name, e.event_order, e.id
        ORDER BY e.season, e.event_order
    """), params).all()

    seasons_available = session.execute(text("""
        SELECT DISTINCT e.season
        FROM riders ri
        JOIN results r ON r.rider_id = ri.id
        JOIN events e ON r.event_id = e.id
        WHERE ri.team = :team
        ORDER BY e.season
    """), {"team": team_name}).all()

    return {
        "team_name": team_name,
        "roster": [_serialize(r._mapping) for r in roster],
        "division_summary": [_serialize(r._mapping) for r in division_summary],
        "event_performance": [_serialize(r._mapping) for r in event_performance],
        "seasons_available": [r[0] for r in seasons_available],
    }


def leaderboard(
    session: Session,
    season: int | None = None,
    division: str | None = None,
    gender: str | None = None,
    metric: str = "avg_points",
    limit: int = 25,
) -> list[dict]:
    """Top riders by chosen metric."""
    params: dict = {}
    filters: list[str] = ["r.place IS NOT NULL"]
    if season:
        filters.append("e.season = :season")
        params["season"] = season
    if division:
        filters.append("r.division = :division")
        params["division"] = division
    if gender:
        filters.append("r.gender = :gender")
        params["gender"] = gender

    where = " AND ".join(filters)

    order_col = {
        "avg_points": "avg_points DESC NULLS LAST",
        "avg_place": "avg_place ASC NULLS LAST",
        "total_points": "total_points DESC NULLS LAST",
        "races": "races DESC",
        "best_place": "best_place ASC NULLS LAST",
    }.get(metric, "avg_points DESC NULLS LAST")

    sql = f"""
        SELECT
            ri.id AS rider_id,
            ri.name,
            ri.team,
            r.division,
            r.gender,
            count(DISTINCT r.event_id) AS races,
            round(avg(r.points)::numeric, 1) AS avg_points,
            round(avg(r.place)::numeric, 1) AS avg_place,
            min(r.place) AS best_place,
            sum(r.points) AS total_points
        FROM results r
        JOIN riders ri ON r.rider_id = ri.id
        JOIN events e ON r.event_id = e.id
        WHERE {where}
        GROUP BY ri.id, ri.name, ri.team, r.division, r.gender
        HAVING count(DISTINCT r.event_id) >= 2
        ORDER BY {order_col}
        LIMIT :limit
    """
    params["limit"] = limit
    rows = session.execute(text(sql), params).all()
    return [_serialize(r._mapping) for r in rows]


def team_leaderboard(
    session: Session,
    season: int | None = None,
    limit: int = 25,
) -> list[dict]:
    """Top teams by average points."""
    params: dict = {"limit": limit}
    season_filter = ""
    if season:
        season_filter = "AND e.season = :season"
        params["season"] = season

    sql = f"""
        SELECT
            ri.team,
            count(DISTINCT ri.id) AS riders,
            count(DISTINCT r.event_id) AS races,
            round(avg(r.points)::numeric, 1) AS avg_points,
            round(avg(r.place)::numeric, 1) AS avg_place,
            sum(r.points) AS total_points,
            min(r.place) AS best_place
        FROM results r
        JOIN riders ri ON r.rider_id = ri.id
        JOIN events e ON r.event_id = e.id
        WHERE r.place IS NOT NULL AND ri.team IS NOT NULL
          {season_filter}
        GROUP BY ri.team
        HAVING count(DISTINCT ri.id) >= 3
        ORDER BY avg_points DESC NULLS LAST
        LIMIT :limit
    """
    rows = session.execute(text(sql), params).all()
    return [_serialize(r._mapping) for r in rows]
