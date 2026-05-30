# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

atp2osm-import is a tool for importing [All The Places](https://alltheplaces.xyz) (ATP) data into OpenStreetMap (OSM). It focuses on French (metropolitan) POIs, matching ATP entries to existing OSM nodes/relations by spatial proximity (500m) and attribute similarity (brand, name, email, phone, website). A Flask web UI lets authenticated OSM users review, validate, and bulk-upload tag changes.

## Commands

```bash
# Install dependencies
uv sync

# Run the Flask server (development)
uv run --env-file .env flask --app ./src/app.py run --debug

# Production: app runs via gunicorn inside a container (see Containerfile)
# Deploy is triggered by git push to the server (deploy/run hook)

# Run tests
uv run pytest
uv run pytest tests/test_compute_diff.py            # single file
uv run pytest tests/test_compute_diff.py::test_apply_on_node_default  # single test

# Start infrastructure (PostGIS database)
podman-compose up -d

# Import OSM PBF data into PostGIS (local dev, via container)
podman-compose run osm2pgsql osm2pgsql --output flex -S /osm2pgsql/generic.lua -d o2p -U o2p -H 127.0.0.1 -P 5432 /data/osm/<file>.osm.pbf

# Refresh all data (ATP + OSM) — runs weekly via systemd timer in production
# Manual trigger on server:
#   systemctl --user start atp2osm-gwenael-leger-fr-refresh.service
# Manual trigger locally:
#   OSM_DB_NAME=o2p OSM_DB_USER=o2p OSM_DB_PASSWORD=... OSM_DB_HOST=127.0.0.1 OSM_DB_PORT=5432 ./run-pipeline.sh
```

## Architecture

**Data pipeline** (runs outside the web server, via `run-pipeline.sh` and `src/pipeline/`):
1. `run-pipeline.sh` — Entry point du refresh hebdomadaire : lance `src/pipeline` dans le container via podman. Copié dans le répertoire projet à chaque deploy. Déclenché par un timer systemd (lundi 04:00).
2. `src/pipeline/` — Module Python qui orchestre le pipeline complet : téléchargement OSM PBF depuis Geofabrik, import osm2pgsql, téléchargement ATP parquet, chargement dans `atp_fr` via DuckDB, refresh de la vue matérialisée.
3. `osm2pgsql/generic.lua` — Flex output style that imports OSM PBF into `points` and `polygons` tables in PostGIS (SRID 9794, Lambert-93 projection)

**Deploy** (`deploy/run` — git hook `post-receive`):
- Build l'image container, écrit le Quadlet `atp2osm.container`, écrit les unités systemd `refresh.service` + `refresh.timer` depuis les templates `deploy/`, puis fait `daemon-reload` + `restart` + `enable timer` directement.
- Provisioning one-time côté serveur : `loginctl enable-linger $USER` (garder les services actifs sans session ouverte).

**Web application** (`src/app.py`, Flask):
- Uses PostGIS with psycopg3, connection per-request via Flask `g`
- OSM OAuth2 authentication; tokens stored in-memory (`token_store` dict)
- Templates in `website/templates/`, static assets in `static/`
- SQL migrations in `migrations/` auto-run at startup (`src/migrate.py`), tracked in `schema_migrations` table

**Core modules:**
- `src/matching.py` — Spatial join queries between `mv_places` and `atp_fr`, tag diffing logic (`apply_on_node`), stats aggregation
- `src/upload.py` — `BulkUpload` class that creates OSM changesets grouped by département, uploads via `osmapi`
- `src/migrate.py` — Simple sequential SQL migration runner

**Key database objects:**
- `points`, `polygons` — Raw OSM data (from osm2pgsql)
- `mv_places` — Materialized view joining both with normalized columns
- `atp_fr` — ATP data filtered to metropolitan France
- `import_history` — Tracks import runs per brand

## Environment Variables

See `.env.sample`. Key variables: `OSM_DB_*` (PostGIS connection), `OSM_API_HOST` (OSM API base URL), `OSM_OAUTH_CLIENT_ID`/`SECRET` (OAuth2 app credentials).

## Testing

Tests use pytest with `--import-mode=importlib` and pythonpath set to `.` (see `pyproject.toml`). The test file currently imports from `src.compute_diff` which corresponds to functions now in `src.matching`.
