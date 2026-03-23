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

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

# ---------- 1. Download + import OSM PBF in one pass via stdin ----------
log "Step 1/3 — Downloading and importing OSM PBF into PostGIS..."
curl -fSL --retry 3 "$GEOFABRIK_URL" \
  | osm2pgsql \
    --output flex \
    -S "$LUA_STYLE" \
    -d "${OSM_DB_NAME}" \
    -U "${OSM_DB_USER}" \
    -H "${OSM_DB_HOST}" \
    -P "${OSM_DB_PORT}" \
    --drop \
    -
log "osm2pgsql import complete"

# ---------- 2. Remove stale ATP data so setup.py re-downloads ----------
log "Step 2/3 — Clearing old ATP data..."
rm -f "$ATP_DIR/latest.parquet" "$ATP_DIR/spriders.json"

# ---------- 3. Run setup.py (ATP download + import + mv_places refresh) ----------
log "Step 3/3 — Running setup.py (ATP import + materialized view refresh)..."
cd "$PROJECT_DIR"

# setup.py needs FORCE_OSM_SETUP=1 to recreate mv_places after osm2pgsql re-import
export FORCE_OSM_SETUP=1
export FORCE_ATP_SETUP=1

# Use uv from the container image or system
if command -v uv &>/dev/null; then
  uv run --env-file "$PROJECT_DIR/.env" python -c "
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
else
  echo "ERROR: uv not found in PATH" >&2
  exit 1
fi

log "Data refresh complete"
