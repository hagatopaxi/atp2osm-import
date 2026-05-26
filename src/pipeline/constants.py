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
PBF_PATH = PROJECT_ROOT / "data" / "osm" / "france-latest.osm.pbf"
GEOFABRIK_URL = "https://download.geofabrik.de/europe/france-latest.osm.pbf"
GEOFABRIK_STATE_URL = "https://download.geofabrik.de/europe/france-updates/state.txt"
