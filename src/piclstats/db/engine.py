"""SQLAlchemy engine and session factory."""

from functools import lru_cache

from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import Session, sessionmaker

from piclstats.config import settings


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    return create_engine(settings.database_url, echo=False)


def get_session() -> Session:
    return sessionmaker(bind=get_engine())()
