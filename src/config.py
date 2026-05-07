import logging
import os
import subprocess
import pathlib

logger = logging.getLogger(__name__)

PROJECT_ROOT = pathlib.Path(__file__).parent.parent.resolve()
TEMPLATE_DIR = PROJECT_ROOT / "website" / "templates"
CACHE_DIR = PROJECT_ROOT / ".cache"
STATIC_DIR = PROJECT_ROOT / "static"


def _get_version():
    if v := os.getenv("APP_VERSION"):
        return v
    try:
        hash = (
            subprocess.check_output(
                ["git", "rev-parse", "--short=6", "HEAD"],
                cwd=PROJECT_ROOT,
                stderr=subprocess.DEVNULL,
            )
            .decode()
            .strip()
        )
        return f"Beta-{hash}"
    except Exception:
        return "Beta"


APP_VERSION = _get_version()

api_url = os.getenv("OSM_API_HOST").strip("/")

env = os.getenv("APP_ENV")
if env is None:
    raise ValueError(
        "APP_ENV environment variable is required (DEVELOPMENT or PRODUCTION)"
    )
env = env.upper()
if env not in ("DEVELOPMENT", "PRODUCTION"):
    raise ValueError(f"APP_ENV must be DEVELOPMENT or PRODUCTION, got '{env}'")
logger.warning("*** Running in %s mode (OSM API: %s) ***", env, api_url)
logger.warning(f"App Version: {os.getenv('APP_VERSION', 'unknown')}")
