import logging
import os
import pathlib
import subprocess
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional

logger = logging.getLogger(__name__)

PROJECT_ROOT = pathlib.Path(__file__).parent.parent.resolve()
TEMPLATE_DIR = PROJECT_ROOT / "website" / "templates"
CACHE_DIR = PROJECT_ROOT / ".cache"
STATIC_DIR = PROJECT_ROOT / "static"


class ConfigError(RuntimeError):
    """Raised when required environment variables are missing or invalid."""


def get_env(name: str) -> str:
    """Get required environment variable or raise ConfigError."""
    value = os.environ.get(name)
    if not value:
        raise ConfigError(f"Missing required environment variable: {name}")
    return value


def get_int(name: str, default: int) -> int:
    """Get environment variable as int with default."""
    return int(os.environ.get(name) or default)


def get_float(name: str, default: float) -> float:
    """Get environment variable as float with default."""
    return float(os.environ.get(name) or default)


def get_version() -> str:
    """Get application version from env or git."""
    if v := os.environ.get("APP_VERSION"):
        return v
    try:
        rev = (
            subprocess.check_output(
                ["git", "rev-parse", "--short=6", "HEAD"],
                cwd=PROJECT_ROOT,
                stderr=subprocess.DEVNULL,
            )
            .decode()
            .strip()
        )
        return f"Beta-{rev}"
    except Exception:
        return "Beta"


@dataclass(frozen=True)
class Database:
    """PostGIS connection settings — shared by app and pipeline."""

    name: str
    user: str
    password: str
    host: str
    port: str

    @property
    def connect_kwargs(self) -> dict:
        return {
            "dbname": self.name,
            "user": self.user,
            "password": self.password,
            "host": self.host,
            "port": self.port,
        }


@dataclass(frozen=True)
class App:
    """Web application settings."""

    env: str
    api_url: str
    oauth_client_id: str
    oauth_client_secret: str
    app_base_url: str
    secret_key: str
    port: int
    app_version: str

    @property
    def is_dev(self) -> bool:
        return self.env == "DEVELOPMENT"


@dataclass(frozen=True)
class Pipeline:
    """Pipeline settings — all optional with sensible defaults."""

    workers: int
    min_free_gb: float


@dataclass(frozen=True)
class Settings:
    """Complete application settings — includes app settings and database config.

    This is the legacy class for backwards compatibility with code that used
    the old get_settings() function which returned Settings with a db field.
    """

    env: str
    api_url: str
    oauth_client_id: str
    oauth_client_secret: str
    app_base_url: str
    secret_key: str
    port: int
    app_version: str
    db: Database

    @property
    def is_dev(self) -> bool:
        return self.env == "DEVELOPMENT"


@lru_cache(maxsize=1)
def get_database() -> Database:
    """Get database configuration. Fails fast if DB vars are missing."""
    return Database(
        name=get_env("OSM_DB_NAME"),
        user=get_env("OSM_DB_USER"),
        password=get_env("OSM_DB_PASSWORD"),
        host=get_env("OSM_DB_HOST"),
        port=get_env("OSM_DB_PORT"),
    )


def get_app() -> Optional[App]:
    """Get app configuration if all required web variables are present."""
    required = ["OSM_API_HOST", "OSM_OAUTH_CLIENT_ID", "OSM_OAUTH_CLIENT_SECRET"]
    if not all(os.environ.get(v) for v in required):
        return None

    env = os.environ.get("APP_ENV", "").upper()
    if env and env not in ("DEVELOPMENT", "PRODUCTION"):
        raise ConfigError(f"APP_ENV must be DEVELOPMENT or PRODUCTION, got '{env}'")

    return App(
        env=env,
        api_url=get_env("OSM_API_HOST").strip("/"),
        oauth_client_id=get_env("OSM_OAUTH_CLIENT_ID"),
        oauth_client_secret=get_env("OSM_OAUTH_CLIENT_SECRET"),
        app_base_url=get_env("APP_BASE_URL").rstrip("/"),
        secret_key=get_env("SECRET_KEY"),
        port=get_int("PORT", 5000),
        app_version=get_version(),
    )


@lru_cache(maxsize=1)
def get_pipeline() -> Pipeline:
    """Get pipeline configuration with defaults."""
    return Pipeline(
        workers=get_int("PIPELINE_WORKERS", max(1, (os.cpu_count() or 4) // 2)),
        min_free_gb=get_float("OSM2PGSQL_MIN_FREE_GB", 15),
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Get complete application settings (app + database).

    This is the legacy function for backwards compatibility.
    Fails fast at startup if any required web env vars are missing.
    """
    app_settings = get_app()
    if app_settings is None:
        raise ConfigError(
            "App environment variables (APP_ENV, OSM_API_HOST, etc.) are missing"
        )

    return Settings(
        env=app_settings.env,
        api_url=app_settings.api_url,
        oauth_client_id=app_settings.oauth_client_id,
        oauth_client_secret=app_settings.oauth_client_secret,
        app_base_url=app_settings.app_base_url,
        secret_key=app_settings.secret_key,
        port=app_settings.port,
        app_version=app_settings.app_version,
        db=get_database(),
    )
