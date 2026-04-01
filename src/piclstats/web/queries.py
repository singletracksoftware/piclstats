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


# Common CTE fragment: resolve any rider to its canonical ID
_CANONICAL_CTE = """
    canonical AS (
        SELECT ri.id AS rider_id, COALESCE(ra.canonical_id, ri.id) AS cid,
               ri.name, ri.team, ri.school
        FROM riders ri
        LEFT JOIN rider_aliases ra ON ra.rider_id = ri.id
    )
"""


def overview_stats(session: Session) -> dict:
    row = session.execute(text(f"""
        WITH {_CANONICAL_CTE}
        SELECT
            count(DISTINCT e.id) AS events,
            count(DISTINCT c.cid) AS riders,
            count(DISTINCT c.team) AS teams,
            count(r.id) AS results,
            count(DISTINCT e.season) AS seasons
        FROM results r
        JOIN events e ON r.event_id = e.id
        JOIN canonical c ON c.rider_id = r.rider_id
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
    """Search riders by name. Merged riders appear as one row."""
    sql = f"""
        WITH {_CANONICAL_CTE}
        SELECT
            c.cid AS id,
            c.name,
            string_agg(DISTINCT c.team, ' / ' ORDER BY c.team) AS team,
            count(DISTINCT r.event_id) AS race_count,
            count(DISTINCT e.season) AS seasons_active,
            round(avg(r.points)::numeric, 1) AS avg_points,
            round(avg(r.place)::numeric, 1) AS avg_place,
            min(e.season) AS first_season,
            max(e.season) AS last_season
        FROM canonical c
        JOIN results r ON r.rider_id = c.rider_id
        JOIN events e ON r.event_id = e.id
        WHERE c.name ILIKE :q
    """
    params: dict = {"q": f"%{q}%"}
    if team:
        sql += " AND c.team ILIKE :team"
        params["team"] = f"%{team}%"
    if season:
        sql += " AND e.season = :season"
        params["season"] = season
    sql += """
        GROUP BY c.cid, c.name
        ORDER BY avg_points DESC NULLS LAST, c.name
        LIMIT 100
    """
    rows = session.execute(text(sql), params).all()
    return [_serialize(r._mapping) for r in rows]


def rider_detail(session: Session, rider_id: int) -> dict | None:
    """Full rider profile — unified across all aliases."""
    # Resolve to canonical
    canonical_id = session.execute(text("""
        SELECT COALESCE(
            (SELECT canonical_id FROM rider_aliases WHERE rider_id = :id),
            :id
        )
    """), {"id": rider_id}).scalar()

    # Get all rider_ids in this canonical group
    group_ids = session.execute(text("""
        SELECT rider_id FROM rider_aliases WHERE canonical_id = :cid
        UNION
        SELECT :cid
    """), {"cid": canonical_id}).all()
    all_ids = [r[0] for r in group_ids]

    info = session.execute(text("""
        SELECT ri.id, ri.name, ri.school,
               string_agg(DISTINCT ri2.team, ' / ' ORDER BY ri2.team) AS team
        FROM riders ri
        CROSS JOIN riders ri2
        WHERE ri.id = :cid AND ri2.id = ANY(:ids)
        GROUP BY ri.id, ri.name, ri.school
    """), {"cid": canonical_id, "ids": all_ids}).one_or_none()
    if not info:
        return None

    # Team history
    team_history = session.execute(text("""
        SELECT DISTINCT ri.team, min(e.season) AS from_season, max(e.season) AS to_season,
               count(r.id) AS races
        FROM riders ri
        JOIN results r ON r.rider_id = ri.id
        JOIN events e ON r.event_id = e.id
        WHERE ri.id = ANY(:ids)
        GROUP BY ri.team
        ORDER BY from_season
    """), {"ids": all_ids}).all()

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
            r.penalty,
            ri.team
        FROM results r
        JOIN events e ON r.event_id = e.id
        JOIN riders ri ON r.rider_id = ri.id
        WHERE r.rider_id = ANY(:ids)
        ORDER BY e.season, e.event_order
    """), {"ids": all_ids}).all()

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
        WHERE r.rider_id = ANY(:ids) AND r.place IS NOT NULL
        GROUP BY e.season, r.division
        ORDER BY e.season
    """), {"ids": all_ids}).all()

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
        WHERE ranked.rider_id = ANY(:ids)
        ORDER BY e.season, e.event_order
    """), {"ids": all_ids}).all()

    return {
        "info": _serialize(info._mapping),
        "team_history": [_serialize(r._mapping) for r in team_history],
        "races": [_serialize(r._mapping) for r in races],
        "season_stats": [_serialize(r._mapping) for r in season_stats],
        "percentiles": [_serialize(r._mapping) for r in percentiles],
    }


def search_teams(session: Session, q: str, season: int | None = None) -> list[dict]:
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
    params: dict = {"team": team_name}
    season_filter = ""
    if season:
        season_filter = "AND e.season = :season"
        params["season"] = season

    # Use canonical IDs so riders who changed teams still show their full stats
    # when viewing from any of their teams
    roster = session.execute(text(f"""
        WITH {_CANONICAL_CTE}
        SELECT
            c.cid AS id,
            c.name,
            r.division,
            r.gender,
            count(DISTINCT r.event_id) AS races,
            round(avg(r.points)::numeric, 1) AS avg_points,
            round(avg(r.place)::numeric, 1) AS avg_place,
            min(r.place) AS best_place,
            max(r.points) AS best_points,
            sum(r.points) AS total_points
        FROM canonical c
        JOIN results r ON r.rider_id = c.rider_id
        JOIN events e ON r.event_id = e.id
        WHERE c.team = :team {season_filter}
          AND r.place IS NOT NULL
        GROUP BY c.cid, c.name, r.division, r.gender
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
    """Top riders by chosen metric — merged riders unified."""
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
        WITH {_CANONICAL_CTE}
        SELECT
            c.cid AS rider_id,
            c.name,
            string_agg(DISTINCT c.team, ' / ' ORDER BY c.team) AS team,
            r.division,
            r.gender,
            count(DISTINCT r.event_id) AS races,
            round(avg(r.points)::numeric, 1) AS avg_points,
            round(avg(r.place)::numeric, 1) AS avg_place,
            min(r.place) AS best_place,
            sum(r.points) AS total_points
        FROM results r
        JOIN canonical c ON c.rider_id = r.rider_id
        JOIN events e ON r.event_id = e.id
        WHERE {where}
        GROUP BY c.cid, c.name, r.division, r.gender
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
