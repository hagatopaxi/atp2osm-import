#!/bin/bash
set -euo pipefail

# Weekly data refresh: downloads fresh OSM + ATP data, reimports everything.
# Designed to run via systemd timer. Expects .env vars in the environment
# (loaded by the systemd EnvironmentFile= directive).

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ATP_DIR="$PROJECT_DIR/data/atp"

GEOFABRIK_URL="https://download.geofabrik.de/europe/france-latest.osm.pbf"
LUA_STYLE="$PROJECT_DIR/osm2pgsql/generic.lua"
OSM2PGSQL_IMAGE="docker.io/iboates/osm2pgsql:latest"

# Derive container name the same way deploy/run does
PROJECT_NAME=$(basename "$PROJECT_DIR")
CONTAINER_NAME="${PROJECT_NAME//./-}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

# ---------- 1. Download OSM PBF ----------
OSM_PBF="$PROJECT_DIR/data/france-latest.osm.pbf"
mkdir -p "$PROJECT_DIR/data"
if [ -f "$OSM_PBF" ]; then
  log "Step 1a/3 — OSM PBF already exists, skipping download"
else
  log "Step 1a/3 — Downloading OSM PBF..."
  # Remove partial file if download fails
  trap 'rm -f "$OSM_PBF"' ERR
  curl -fSL --retry 3 -o "$OSM_PBF" "$GEOFABRIK_URL"
  trap - ERR
  log "Download complete"
fi

# ---------- 1b. Import OSM PBF via osm2pgsql container ----------
log "Step 1b/3 — Importing OSM PBF into PostGIS..."
podman run --rm \
    --network host \
    -v "$PROJECT_DIR/osm2pgsql:/osm2pgsql:ro,Z" \
    -v "$PROJECT_DIR/data:/data:Z" \
    -e PGPASSWORD="${OSM_DB_PASSWORD}" \
    "$OSM2PGSQL_IMAGE" \
    osm2pgsql \
      --output flex \
      -S "/osm2pgsql/generic.lua" \
      -d "${OSM_DB_NAME}" \
      -U "${OSM_DB_USER}" \
      -H "${OSM_DB_HOST}" \
      -P "${OSM_DB_PORT}" \
      --cache=1500 \
      "/data/france-latest.osm.pbf"
rm -f "$OSM_PBF"
log "osm2pgsql import complete"

# ---------- 2. Remove stale ATP data so setup.py re-downloads ----------
log "Step 2/3 — Clearing old ATP data..."
mkdir -p "$ATP_DIR"
rm -f "$ATP_DIR/latest.parquet" "$ATP_DIR/spiders.json"

# ---------- 3. Run setup.py inside the app container ----------
log "Step 3/3 — Running setup.py (ATP import + materialized view refresh)..."
podman exec \
  -e FORCE_OSM_SETUP=1 \
  -e FORCE_ATP_SETUP=1 \
  "$CONTAINER_NAME" \
  uv run --no-sync python -c "
import psycopg, os
from src.setup import setup_atp2osm_db
conn = psycopg.connect(
    dbname=os.environ['OSM_DB_NAME'],
    user=os.environ['OSM_DB_USER'],
    password=os.environ['OSM_DB_PASSWORD'],
    host=os.environ['OSM_DB_HOST'],
    port=os.environ['OSM_DB_PORT'],
)
setup_atp2osm_db(conn)
conn.close()
"

log "Data refresh complete"
