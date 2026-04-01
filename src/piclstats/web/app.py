"""FastAPI web dashboard for PICL Stats."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import quote_plus

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from piclstats.db.engine import get_session
from piclstats.web import queries

TEMPLATE_DIR = Path(__file__).parent / "templates"

app = FastAPI(title="PICL Stats Dashboard")
templates = Jinja2Templates(directory=str(TEMPLATE_DIR))


def _ctx(request: Request, **kwargs) -> dict:
    """Build base template context."""
    return {"request": request, **kwargs}


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    with get_session() as session:
        stats = queries.overview_stats(session)
        seasons = queries.seasons_list(session)
        top_riders = queries.leaderboard(session, limit=10)
        top_teams = queries.team_leaderboard(session, limit=10)
    return templates.TemplateResponse("home.html", _ctx(
        request, stats=stats, seasons=seasons,
        top_riders=top_riders, top_teams=top_teams,
    ))


@app.get("/riders", response_class=HTMLResponse)
def rider_search(
    request: Request,
    q: str = Query("", description="Rider name search"),
    team: str = Query("", description="Team filter"),
    season: int | None = Query(None),
):
    with get_session() as session:
        results = queries.search_riders(session, q, team or None, season) if q else []
        seasons = queries.seasons_list(session)
        teams = queries.teams_list(session)
    return templates.TemplateResponse("riders.html", _ctx(
        request, results=results, q=q, team=team,
        season=season, seasons=seasons, teams=teams,
    ))


@app.get("/rider/{rider_id}", response_class=HTMLResponse)
def rider_profile(request: Request, rider_id: int):
    with get_session() as session:
        data = queries.rider_detail(session, rider_id)
    if not data:
        return HTMLResponse("Rider not found", status_code=404)
    return templates.TemplateResponse("rider_detail.html", _ctx(request, **data))


@app.get("/teams", response_class=HTMLResponse)
def team_search(
    request: Request,
    q: str = Query("", description="Team name search"),
    season: int | None = Query(None),
):
    with get_session() as session:
        results = queries.search_teams(session, q, season) if q else []
        seasons = queries.seasons_list(session)
    return templates.TemplateResponse("teams.html", _ctx(
        request, results=results, q=q, season=season, seasons=seasons,
    ))


@app.get("/team/{team_name}", response_class=HTMLResponse)
def team_profile(
    request: Request,
    team_name: str,
    season: int | None = Query(None),
):
    with get_session() as session:
        data = queries.team_detail(session, team_name, season)
    if not data:
        return HTMLResponse("Team not found", status_code=404)
    return templates.TemplateResponse("team_detail.html", _ctx(request, **data, season=season))


@app.get("/leaderboard", response_class=HTMLResponse)
def leaderboard_page(
    request: Request,
    season: int | None = Query(None),
    division: str = Query(""),
    gender: str = Query(""),
    metric: str = Query("avg_points"),
    view: str = Query("riders"),
):
    with get_session() as session:
        seasons = queries.seasons_list(session)
        divisions = queries.divisions_list(session)
        if view == "teams":
            results = queries.team_leaderboard(session, season, limit=50)
        else:
            results = queries.leaderboard(
                session, season, division or None, gender or None, metric, limit=50
            )
    return templates.TemplateResponse("leaderboard.html", _ctx(
        request, results=results, seasons=seasons, divisions=divisions,
        season=season, division=division, gender=gender, metric=metric, view=view,
    ))


@app.get("/courses", response_class=HTMLResponse)
def courses_page(request: Request):
    with get_session() as session:
        course_list = queries.courses_list(session)
    return templates.TemplateResponse("courses.html", _ctx(request, courses=course_list))


@app.get("/course/{course_id}", response_class=HTMLResponse)
def course_profile(
    request: Request,
    course_id: int,
    season: int | None = Query(None),
):
    with get_session() as session:
        data = queries.course_detail(session, course_id, season)
    if not data:
        return HTMLResponse("Course not found", status_code=404)
    return templates.TemplateResponse("course_detail.html", _ctx(request, **data, season=season))
