"""
Shared constants for the ATP2OSM import pipeline.
"""

import os
from pathlib import Path

# Parallel processing configuration
WORKERS = int(os.getenv("PIPELINE_WORKERS") or max(1, (os.cpu_count() or 4) // 2))

# File size limits
MAX_FILE_SIZE = 16 * 1024 * 1024  # 16 MB - maximum size for NDJSON chunks

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

# Each entry: geofabrik path suffix (without -latest.osm.pbf).
# url, state_url and pbf_path are derived automatically.
# DOM are sub-regions of europe/france on Geofabrik.
# COM in the Pacific are under australia-oceania (French names).
# Note: Saint-Pierre-et-Miquelon has no dedicated Geofabrik extract.
_GEOFABRIK_PATHS = {
    "france":              "europe/france",
    # DOM — Départements d'Outre-Mer
    "guadeloupe":          "europe/france/guadeloupe",
    "martinique":          "europe/france/martinique",
    "guyane":              "europe/france/guyane",
    "reunion":             "europe/france/reunion",
    "mayotte":             "europe/france/mayotte",
    # COM — Collectivités d'Outre-Mer (Pacific)
    "new-caledonia":       "australia-oceania/new-caledonia",
    "polynesie-francaise": "australia-oceania/polynesie-francaise",
    "wallis-et-futuna":    "australia-oceania/wallis-et-futuna",
}

GEOFABRIK_REGIONS = {
    name: {
        "url":      f"{GEOFABRIK_BASE}/{path}-latest.osm.pbf",
        "state_url": f"{GEOFABRIK_BASE}/{path}-updates/state.txt",
        "pbf_path": PROJECT_ROOT / "data" / "osm" / f"{path.split('/')[-1]}-latest.osm.pbf",
    }
    for name, path in _GEOFABRIK_PATHS.items()
}
