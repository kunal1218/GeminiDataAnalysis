import logging
import os
from functools import lru_cache
from urllib.parse import urlparse

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

LOGGER = logging.getLogger(__name__)

_RAILWAY_MARKER_ENV_KEYS = ("RAILWAY_ENVIRONMENT", "RAILWAY_PROJECT_ID")
_POSTGRES_SCHEMES = {"postgresql", "postgres", "postgresql+psycopg2"}


def _read_env(name: str) -> str:
    return (os.getenv(name) or "").strip()


def _is_truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on", "require"}


def _is_running_on_railway() -> bool:
    return any(_read_env(name) for name in _RAILWAY_MARKER_ENV_KEYS)


def _extract_db_host(database_url: str) -> str:
    parsed = urlparse(database_url)
    return (parsed.hostname or "").strip().lower()


def _extract_db_port(database_url: str) -> int | None:
    parsed = urlparse(database_url)
    return parsed.port


def _is_railway_internal_host(hostname: str) -> bool:
    host = (hostname or "").strip().lower()
    return host == "postgres.railway.internal" or host.endswith(".railway.internal")


def _is_railway_public_host(hostname: str) -> bool:
    host = (hostname or "").strip().lower()
    return host.endswith(".proxy.rlwy.net") or host.endswith(".rlwy.net")


def _redact_host(hostname: str) -> str:
    host = (hostname or "").strip().lower()
    if not host:
        return "<missing>"
    labels = host.split(".")
    if len(labels) >= 2:
        return "***." + ".".join(labels[-2:])
    return host[:1] + "***"


def _validate_database_url(selected_key: str, selected_url: str) -> tuple[str, int | None]:
    parsed = urlparse(selected_url)
    scheme = (parsed.scheme or "").strip().lower()
    if scheme not in _POSTGRES_SCHEMES:
        raise RuntimeError(
            f"{selected_key} is not a valid Postgres URL. "
            "Set a full postgresql:// URL."
        )

    host = (parsed.hostname or "").strip().lower()
    if not host:
        raise RuntimeError(
            f"{selected_key} is missing a hostname. "
            "Set DATABASE_PUBLIC_URL to Railway's Public connection URL."
        )
    return host, parsed.port


def _select_database_url() -> tuple[str, str]:
    database_url = _read_env("DATABASE_URL")
    public_url = _read_env("DATABASE_PUBLIC_URL")
    on_railway = _is_running_on_railway()

    if on_railway:
        if database_url:
            selected_key, selected_url = "DATABASE_URL", database_url
        else:
            raise RuntimeError(
                "Database URL is missing. On Railway runtime, set DATABASE_URL."
            )
    else:
        if public_url:
            selected_key, selected_url = "DATABASE_PUBLIC_URL", public_url
        elif database_url:
            selected_key, selected_url = "DATABASE_URL", database_url
        else:
            raise RuntimeError(
                "Database URL is missing. Outside Railway, set DATABASE_PUBLIC_URL "
                "(or DATABASE_URL if it is publicly reachable)."
            )

    selected_host, _ = _validate_database_url(selected_key, selected_url)
    if not on_railway and _is_railway_internal_host(selected_host):
        raise RuntimeError(
            "Detected Railway internal DB host outside Railway runtime. "
            "Set DATABASE_PUBLIC_URL to Railway's Public connection URL."
        )
    return selected_key, selected_url


def _build_connect_args(database_url: str) -> dict[str, str]:
    # Preserve explicit URL-level sslmode.
    if "sslmode=" in database_url.lower():
        return {}

    host = _extract_db_host(database_url)
    if _is_railway_public_host(host):
        return {"sslmode": "require"}

    ssl_value = _read_env("DATABASE_SSL")
    if _is_truthy(ssl_value):
        return {"sslmode": "require"}
    return {}


def validate_database_config() -> None:
    selected_key, selected_url = _select_database_url()
    host, port = _validate_database_url(selected_key, selected_url)
    port_label = str(port) if port is not None else "unknown"
    LOGGER.info(
        "DB config selected source=%s runtime=%s host=%s port=%s",
        selected_key,
        "railway" if _is_running_on_railway() else "external",
        _redact_host(host),
        port_label,
    )


def verify_database_connection() -> None:
    selected_key, selected_url = _select_database_url()
    host, port = _validate_database_url(selected_key, selected_url)
    redacted_host = _redact_host(host)
    port_label = str(port) if port is not None else "unknown"

    try:
        engine = get_engine()
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except SQLAlchemyError as exc:
        raise RuntimeError(
            "Database connectivity check failed for "
            f"{selected_key} (host={redacted_host}, port={port_label}). "
            "Verify DATABASE_PUBLIC_URL for non-Railway runtimes."
        ) from exc


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    _, database_url = _select_database_url()
    return create_engine(
        database_url,
        pool_pre_ping=True,
        future=True,
        connect_args=_build_connect_args(database_url),
    )


def get_session_factory() -> sessionmaker:
    return sessionmaker(bind=get_engine(), autoflush=False, autocommit=False, future=True)
