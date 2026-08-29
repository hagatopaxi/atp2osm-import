"""
Shared constants for the ATP2OSM import pipeline.
"""

import os
from pathlib import Path

from src.config import get_pipeline

# Parallel processing configuration
WORKERS = get_pipeline().workers

# File size limits
MAX_FILE_SIZE = 128 * 1024 * 1024  # 128 MB - maximum size for NDJSON chunks

# Directory paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
ATP_DIR = PROJECT_ROOT / "data" / "atp"
GEOJSON_DIR = ATP_DIR / "geojson"
NDGEOJSON_DIR = ATP_DIR / "ndgeojson"
SPLIT_DIR = ATP_DIR / "split"
PARQUET_PATH = ATP_DIR / "latest.parquet"
SPIDERS_PATH = ATP_DIR / "spiders.json"
ATP_HISTORY_URL = "https://data.alltheplaces.xyz/runs/history.json"
GEOFABRIK_BASE = "https://download.geofabrik.de"

# NSI (name-suggestion-index). dist/ is no longer committed on the GitHub main
# branch, so npm + jsDelivr is the only channel.
#
# The version is always pinned, and not only to keep the file in step with the
# data_imports row: jsDelivr answers `@latest` from a cache that can be years
# stale. It still serves a 6.x-era file under /dist/nsi.json, a path this
# package stopped shipping — hence the /dist/json/ one below. A pinned URL is
# resolved against the real tarball, so a wrong path fails loudly with a 404
# instead of silently returning obsolete data.
NSI_DIR = PROJECT_ROOT / "data" / "nsi"

# Where a run parks the Geofabrik timestamp it has already fetched, so the
# three steps that need it query the network once. Written at the probe,
# removed by the cleanup step, and cleared again at the start of every run —
# a file outlives a crashed process, unlike the value it stands for.
GEOFABRIK_TS_PATH = PROJECT_ROOT / "data" / "osm" / "geofabrik-timestamp.txt"
NSI_PATH = NSI_DIR / "nsi.json"
NSI_REGISTRY_URL = "https://registry.npmjs.org/name-suggestion-index"
NSI_CDN_URL = (
    "https://cdn.jsdelivr.net/npm/name-suggestion-index@{version}/dist/json/nsi.json"
)

# Administrative subdivision levels. ADMIN_LEVEL is the finest level a POI is
# attached to; the attachment falls back down to 2 (the country) when no polygon
# of that level covers it. ADMIN_LEVEL_MAX is how deep the PBF import goes: only
# margin, so that lowering ADMIN_LEVEL later costs an UPDATE, not a reimport.
# Phase D moves both to the country configuration file.
ADMIN_LEVEL = 6
ADMIN_LEVEL_MAX = 8
assert ADMIN_LEVEL_MAX >= ADMIN_LEVEL

# Each entry: geofabrik path suffix (without -latest.osm.pbf).
# url, state_url and pbf_path are derived automatically.
# DOM are sub-regions of europe/france on Geofabrik.
# COM in the Pacific are under australia-oceania (French names).
# Note: Saint-Pierre-et-Miquelon has no dedicated Geofabrik extract.
_GEOFABRIK_PATHS = {
    "france":              "europe/france",
    # DOM — overseas départements
    "guadeloupe":          "europe/france/guadeloupe",
    "martinique":          "europe/france/martinique",
    "guyane":              "europe/france/guyane",
    "reunion":             "europe/france/reunion",
    "mayotte":             "europe/france/mayotte",
    # COM — overseas collectivities (Pacific)
    "new-caledonia":       "australia-oceania/new-caledonia",
    "polynesie-francaise": "australia-oceania/polynesie-francaise",
    "wallis-et-futuna":    "australia-oceania/wallis-et-futuna",
}

# Local recipes need a fraction of the country, not the nine extracts: set
# ATP2OSM_GEOFABRIK_PATHS to a comma-separated list of Geofabrik paths and it
# replaces the table above entirely. The name of a region is the last path
# segment, which is also what names its PBF file.
# ponytail: dev-only override, superseded by the country file's `geofabrik` key
# in phase D.
_paths_override = os.getenv("ATP2OSM_GEOFABRIK_PATHS", "").strip()
if _paths_override:
    _GEOFABRIK_PATHS = {
        path.strip().rsplit("/", 1)[-1]: path.strip()
        for path in _paths_override.split(",")
        if path.strip()
    }

GEOFABRIK_REGIONS = {
    name: {
        "url":      f"{GEOFABRIK_BASE}/{path}-latest.osm.pbf",
        "state_url": f"{GEOFABRIK_BASE}/{path}-updates/state.txt",
        "pbf_path": PROJECT_ROOT / "data" / "osm" / f"{path.split('/')[-1]}-latest.osm.pbf",
    }
    for name, path in _GEOFABRIK_PATHS.items()
}
