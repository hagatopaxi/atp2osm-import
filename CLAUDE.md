# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

atp2osm-import is a tool for importing [All The Places](https://alltheplaces.xyz) (ATP) data into OpenStreetMap (OSM). It focuses on French (metropolitan) POIs, matching ATP entries to existing OSM nodes/relations by spatial proximity (500m) and attribute similarity (brand, name, email, phone, website). A Flask web UI lets authenticated OSM users review, validate, and bulk-upload tag changes.

## Commands

```bash
# Install dependencies
uv sync

# Run the Flask server (copy .env.sample to .env first)
uv run --env-file .env flask --app ./src/app.py run          # production
uv run --env-file .env flask --app ./src/app.py run --debug   # development

# Run tests
uv run pytest
uv run pytest tests/test_compute_diff.py            # single file
uv run pytest tests/test_compute_diff.py::test_apply_on_node_default  # single test

# Start infrastructure (PostGIS database)
podman-compose up -d

# Import OSM PBF data into PostGIS
podman-compose run osm2pgsql osm2pgsql --output flex -S /osm2pgsql/generic.lua -d o2p -U o2p -H 127.0.0.1 -P 5432 /data/osm/<file>.osm.pbf
```

## Architecture

**Data pipeline** (runs outside the web server, via `src/setup.py`):
1. `osm2pgsql/generic.lua` — Flex output style that imports OSM PBF into `points` and `polygons` tables in PostGIS (SRID 9794, Lambert-93 projection)
2. `src/setup.py` — Downloads latest ATP parquet from alltheplaces.xyz, loads it into a `atp_fr` table via DuckDB's Postgres extension, creates a materialized view `mv_places` unifying points + polygons with normalized tag columns

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
