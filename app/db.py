import os
from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker


def _get_database_url() -> str:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL environment variable is required.")
    return database_url


def _build_connect_args() -> dict[str, str]:
    ssl_value = os.getenv("DATABASE_SSL", "").strip().lower()
    if ssl_value in {"true", "1", "yes", "require"}:
        return {"sslmode": "require"}
    return {}


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    return create_engine(
        _get_database_url(),
        pool_pre_ping=True,
        future=True,
        connect_args=_build_connect_args(),
    )


def get_session_factory() -> sessionmaker:
    return sessionmaker(bind=get_engine(), autoflush=False, autocommit=False, future=True)
