import logging
import os
from functools import lru_cache
from urllib.parse import urlparse

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

LOGGER = logging.getLogger(__name__)

_DB_URL_ENV_KEYS = ("DATABASE_PUBLIC_URL", "DATABASE_URL", "DATABASE_PRIVATE_URL")
_CANDIDATE_HOST_ENV_KEYS = (
    "DATABASE_PUBLIC_URL",
    "DATABASE_URL",
    "DATABASE_PRIVATE_URL",
    "PGHOST",
    "POSTGRES_HOST",
    "POSTGRES_URL",
    "POSTGRES_PRIVATE_URL",
)


def _read_env(name: str) -> str:
    return (os.getenv(name) or "").strip()


def _is_truthy_env(name: str) -> bool:
    return _read_env(name).lower() in {"1", "true", "yes", "on"}


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


def _extract_candidate_hostname(env_key: str, raw_value: str) -> str | None:
    if not raw_value:
        return None

    if env_key in {"PGHOST", "POSTGRES_HOST"}:
        if "://" in raw_value:
            return _extract_db_host(raw_value) or None
        return raw_value.split(":", 1)[0].strip().lower() or None

    if "://" not in raw_value:
        return None
    return _extract_db_host(raw_value) or None


def _select_database_url() -> tuple[str, str]:
    public_url = _read_env("DATABASE_PUBLIC_URL")
    database_url = _read_env("DATABASE_URL")
    private_url = _read_env("DATABASE_PRIVATE_URL")
    allow_private = _is_truthy_env("ALLOW_DATABASE_PRIVATE_URL")

    if public_url:
        selected_key, selected_url = "DATABASE_PUBLIC_URL", public_url
    elif database_url:
        selected_key, selected_url = "DATABASE_URL", database_url
    elif private_url and allow_private:
        selected_key, selected_url = "DATABASE_PRIVATE_URL", private_url
    elif private_url and not allow_private:
        raise RuntimeError(
            "DATABASE_PRIVATE_URL is set but private DB URLs are disabled. "
            "Set DATABASE_PUBLIC_URL to Railway's Public connection URL."
        )
    else:
        accepted = ", ".join(_DB_URL_ENV_KEYS)
        raise RuntimeError(f"A database URL is required. Set one of: {accepted}.")

    selected_host = _extract_db_host(selected_url)
    if _is_railway_internal_host(selected_host) and not public_url:
        raise RuntimeError(
            "Selected database host is Railway internal "
            f"({selected_host}). Set DATABASE_PUBLIC_URL to Railway's Public connection URL."
        )

    return selected_key, selected_url


def _build_connect_args(database_url: str) -> dict[str, str]:
    if "sslmode=" in database_url.lower():
        return {}

    host = _extract_db_host(database_url)
    if _is_railway_public_host(host):
        return {"sslmode": "require"}

    ssl_value = _read_env("DATABASE_SSL").lower()
    if ssl_value in {"true", "1", "yes", "require"}:
        return {"sslmode": "require"}
    if ssl_value in {"false", "0", "no", "disable"}:
        return {}
    return {}


def validate_database_config() -> None:
    for env_key in _CANDIDATE_HOST_ENV_KEYS:
        raw_value = _read_env(env_key)
        hostname = _extract_candidate_hostname(env_key, raw_value)
        if hostname:
            LOGGER.info("Database candidate host: %s=%s", env_key, hostname)

    selected_key, selected_url = _select_database_url()
    selected_host = _extract_db_host(selected_url) or "unknown"
    selected_port = _extract_db_port(selected_url)
    port_label = str(selected_port) if selected_port is not None else "unknown"
    LOGGER.info(
        "Database URL selected from %s host=%s port=%s",
        selected_key,
        selected_host,
        port_label,
    )


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

