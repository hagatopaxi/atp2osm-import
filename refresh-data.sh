#!/bin/bash
set -euo pipefail

# Weekly data refresh: downloads fresh OSM + ATP data, reimports everything.
# Designed to run via systemd timer. Expects .env vars in the environment
# (loaded by the systemd EnvironmentFile= directive).
#
# Options:
#   --skip-pbf    Skip OSM PBF download and osm2pgsql import (debug only)
#   --skip-mv     Skip mv_places materialized view recreation in setup.py

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
ATP_DIR="$PROJECT_DIR/data/atp"

GEOFABRIK_URL="https://download.geofabrik.de/europe/france-latest.osm.pbf"
GEOFABRIK_STATE_URL="https://download.geofabrik.de/europe/france-updates/state.txt"
OSM2PGSQL_IMAGE="docker.io/iboates/osm2pgsql:latest"

# Derive container name the same way deploy/run does
PROJECT_NAME=$(basename "$PROJECT_DIR")
CONTAINER_NAME="${PROJECT_NAME//./-}"

SKIP_PBF=0
SKIP_MV=0
GEOFABRIK_TS=""
for arg in "$@"; do
    case "$arg" in
        --skip-pbf) SKIP_PBF=1 ;;
        --skip-mv)  SKIP_MV=1 ;;
        *) echo "Unknown argument: $arg" >&2; exit 1 ;;
    esac
done

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

trap 'log "ERROR: Step failed at line $LINENO: $BASH_COMMAND"' ERR

# ---------- 1. Download + import OSM PBF ----------
if [ "$SKIP_PBF" -eq 1 ]; then
  log "Step 1/2 — Skipping OSM PBF download and import (--skip-pbf)"
else
  log "Step 1a/2 — Checking Geofabrik OSM timestamp..."
  GEOFABRIK_TS=$(curl -fsSL --retry 3 "$GEOFABRIK_STATE_URL" | grep '^timestamp=' | cut -d= -f2- | sed 's/\\:/:/g')
  LAST_OSM_DATE=$(PGPASSWORD="${OSM_DB_PASSWORD}" psql \
      -h "${OSM_DB_HOST}" -p "${OSM_DB_PORT}" -U "${OSM_DB_USER}" -d "${OSM_DB_NAME}" \
      -t -A -c "SELECT date FROM data_imports WHERE type='osm' ORDER BY date DESC LIMIT 1;" \
      2>/dev/null || true)

  if [ -n "$LAST_OSM_DATE" ] && [ -n "$GEOFABRIK_TS" ] \
      && [ "$(date -d "$LAST_OSM_DATE" +%s)" -ge "$(date -d "$GEOFABRIK_TS" +%s)" ]; then
    log "OSM data already up-to-date (db: $LAST_OSM_DATE, source: $GEOFABRIK_TS), skipping PBF download and import"
    SKIP_PBF=1
    SKIP_MV=1
  else
    [ -n "$GEOFABRIK_TS" ] && log "New OSM data available (source: $GEOFABRIK_TS)"

    OSM_PBF="$PROJECT_DIR/data/osm/france-latest.osm.pbf"
    mkdir -p "$PROJECT_DIR/data/osm"
    if [ -f "$OSM_PBF" ]; then
      log "Step 1b/2 — OSM PBF already exists, skipping download"
    else
      log "Step 1b/2 — Downloading OSM PBF..."
      trap 'rm -f "$OSM_PBF"; log "ERROR: Download failed, partial file removed"' ERR
      curl -fSL --retry 3 -o "$OSM_PBF" "$GEOFABRIK_URL"
      trap 'log "ERROR: Step failed at line $LINENO: $BASH_COMMAND"' ERR
      log "Download complete"
    fi

    log "Step 1c/2 — Importing OSM PBF into PostGIS..."
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
          "/data/osm/france-latest.osm.pbf"
    rm -f "$OSM_PBF"
    log "osm2pgsql import complete"
  fi
fi

# ---------- 2. Run setup.py inside the app container ----------
if [ "$SKIP_MV" -eq 1 ]; then
  log "Step 2/2 — Running setup.py (ATP import only, skipping materialized view)..."
else
  log "Step 2/2 — Running setup.py (ATP import + materialized view refresh)..."
fi

PYTHON_CMD="uv run --no-sync python"
PYTHON_SCRIPT="
import logging, psycopg, os

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

from datetime import datetime, timezone
from src.setup import setup_atp2osm_db
conn = psycopg.connect(
    dbname=os.environ['OSM_DB_NAME'],
    user=os.environ['OSM_DB_USER'],
    password=os.environ['OSM_DB_PASSWORD'],
    host=os.environ['OSM_DB_HOST'],
    port=os.environ['OSM_DB_PORT'],
)
osm_date_raw = '${GEOFABRIK_TS}'
osm_date = datetime.fromisoformat(osm_date_raw.replace('Z', '+00:00')) if osm_date_raw else None
try:
    setup_atp2osm_db(conn, skip_mv=bool(${SKIP_MV}), osm_date=osm_date)
finally:
    conn.close()
"

if podman container exists "$CONTAINER_NAME" 2>/dev/null; then
  podman exec --workdir /app "$CONTAINER_NAME" $PYTHON_CMD -c "$PYTHON_SCRIPT"
else
  log "Container '$CONTAINER_NAME' not running, executing locally..."
  cd "$PROJECT_DIR"
  $PYTHON_CMD -c "$PYTHON_SCRIPT"
fi

log "Data refresh complete"
