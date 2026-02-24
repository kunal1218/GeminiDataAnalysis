import os
from functools import lru_cache
from urllib.parse import urlparse

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker


_PUBLIC_DB_URL_ENV_NAMES = (
    "DATABASE_URL_PUBLIC",
    "DATABASE_PUBLIC_URL",
    "RAILWAY_DATABASE_PUBLIC_URL",
)


def _read_first_set_env(*names: str) -> str:
    for name in names:
        value = (os.getenv(name) or "").strip()
        if value:
            return value
    return ""


def _get_database_url() -> str:
    database_url = _read_first_set_env("DATABASE_URL")
    public_database_url = _read_first_set_env(*_PUBLIC_DB_URL_ENV_NAMES)

    # If DATABASE_URL is an internal Railway hostname and a public URL is provided,
    # prefer the public URL so external hosts (e.g. Vercel) can connect.
    if _is_railway_internal_url(database_url) and public_database_url:
        return public_database_url

    if database_url:
        return database_url

    if public_database_url:
        return public_database_url

    accepted = ", ".join(("DATABASE_URL",) + _PUBLIC_DB_URL_ENV_NAMES)
    raise RuntimeError(f"A database URL is required. Set one of: {accepted}.")


def _build_connect_args(database_url: str) -> dict[str, str]:
    if "sslmode=" in database_url.lower():
        return {}

    ssl_value = os.getenv("DATABASE_SSL", "").strip().lower()
    if ssl_value in {"true", "1", "yes", "require"}:
        return {"sslmode": "require"}
    if ssl_value in {"false", "0", "no", "disable"}:
        return {}

    # Sensible default for Railway public endpoints.
    if _is_railway_public_url(database_url):
        return {"sslmode": "require"}
    return {}


def _extract_db_host(database_url: str) -> str:
    parsed = urlparse(database_url)
    return (parsed.hostname or "").strip().lower()


def _is_railway_internal_url(database_url: str) -> bool:
    host = _extract_db_host(database_url)
    return host.endswith(".railway.internal")


def _is_railway_public_url(database_url: str) -> bool:
    host = _extract_db_host(database_url)
    return host.endswith(".proxy.rlwy.net") or host.endswith(".rlwy.net")


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    database_url = _get_database_url()
    return create_engine(
        database_url,
        pool_pre_ping=True,
        future=True,
        connect_args=_build_connect_args(database_url),
    )


def get_session_factory() -> sessionmaker:
    return sessionmaker(bind=get_engine(), autoflush=False, autocommit=False, future=True)
