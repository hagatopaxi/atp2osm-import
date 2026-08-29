# AGENTS.md

This file provides guidance to coding agents when working with code in this repository. `CLAUDE.md` is a symlink to it.

## Project Overview

atp2osm-import is a tool for importing [All The Places](https://alltheplaces.xyz) (ATP) data into OpenStreetMap (OSM). It focuses on French (metropolitan) POIs, matching ATP entries to existing OSM nodes/relations by spatial proximity (500m) and attribute similarity (brand, name, email, phone, website). A Flask web UI lets authenticated OSM users review, validate, and bulk-upload tag changes.

## Language

Code is written **in English**: comments, docstrings, variable, function and test names. No French in code, ever. This file too.

Log messages and the output of the `scripts/` maintenance tools count as code, and are English too.

French stays the language of user-facing text (templates, displayed error messages, labels) and of the OSM changeset comment.

## Commits

Commit messages are written in English too, in the imperative ("Translate…",
"Add…", "Fix…"), one subject line and, when the change deserves it, a body
explaining the why. No `Co-Authored-By` trailer, ever.

## Worktrees

Git worktrees **always** live in `.worktrees/<name>` at the project root, never anywhere else (and definitely not under `.claude/`). `dev.sh` resolves worktree names from that directory.

## Commands

```bash
# Run the app for a worktree (port derived from the name, .env symlinked from the main checkout)
./dev.sh up [-d] [name]     # start (-d = detached)
./dev.sh down [name]        # stop
./dev.sh logs [name] [-f]   # show the logs

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

# Refresh all data (ATP + OSM) — runs daily via systemd timer in production
# Manual trigger on server:
#   systemctl --user start atp2osm-gwenael-leger-fr-refresh.service
# Manual trigger locally:
#   OSM_DB_NAME=o2p OSM_DB_USER=o2p OSM_DB_PASSWORD=... OSM_DB_HOST=127.0.0.1 OSM_DB_PORT=5432 ./run-pipeline.sh
```

## Architecture

**Data pipeline** (runs outside the web server, via `run-pipeline.sh` and `src/pipeline/`):
1. `run-pipeline.sh` — Entry point of the daily refresh: runs `src/pipeline` inside the container via podman. Copied into the project directory on every deploy. Triggered by a systemd timer (04:00 Europe/Paris). The ATP branch no-ops when the published export is not newer than the last import.
2. `src/pipeline/` — Python module orchestrating the whole pipeline: OSM PBF download from Geofabrik, osm2pgsql import, ATP parquet download, load into `atp_fr` through DuckDB, materialized view refresh.
3. `osm2pgsql/generic.lua` — Flex output style that imports OSM PBF into `points` and `polygons` tables in PostGIS (SRID 4326). Two filters run there: objects that are definitely not places (roads, boundaries, transport…) and objects carrying none of the attributes a match can key on — no name, brand, email, phone or website. The second one drops ~95% of the objects.

**Deploy** (`deploy/run` — git hook `post-receive`):
- Builds the container image, writes the `atp2osm.container` Quadlet, writes the `refresh.service` + `refresh.timer` systemd units from the `deploy/` templates, then runs `daemon-reload` + `restart` + `enable timer` directly.
- One-time server-side provisioning: `loginctl enable-linger $USER` (keeps the services running without an open session).

**Web application** (`src/app.py`, Flask):
- Uses PostGIS with psycopg3, connection per-request via Flask `g`
- OSM OAuth2 authentication; the token lives in the Flask session cookie, signed with `secret_key` (nothing server-side to share between workers)
- Templates in `website/templates/`, static assets in `static/`
- SQL migrations in `migrations/` auto-run at startup (`src/migrate.py`), tracked in `schema_migrations` table

**Core modules:**
- `src/matching.py` — Spatial join queries between `mv_places` and `atp_fr` (`MATCHED_POI_SQL`, shared with the `mv_places_brand` view and never duplicated), tag diffing logic (`apply_on_node`), batch composition (`pack_departements`, `select_batch`), cooldown SQL, stats aggregation
- `src/upload.py` — `BulkUpload` class that creates OSM changesets grouped by department, uploads via `osmapi`
- `src/migrate.py` — Simple sequential SQL migration runner

**Key database objects:**
- `points`, `polygons` — Raw OSM data (from osm2pgsql)
- `mv_places` — Materialized view joining both with normalized columns, restricted to objects a match can key on (same filter as `generic.lua`, kept as a safety net)
- `mv_places_brand` — Match count per (brand, département); `get_all` sums the départements that are not under cooldown
- `atp_fr` — ATP data filtered to metropolitan France
- `import_history` — One row per human integration action
- `import_departements` — One row per changeset: département, count, status. Carries the per-département blocking and the history detail

## Specs

Functional specs live in `specs/`, prefixed with a two-digit id in creation
order (`01_`, `02_`…). They state the intended behaviour, not the history of
the decisions that led to it.

## Environment Variables

See `.env.sample`. Key variables: `OSM_DB_*` (PostGIS connection), `OSM_API_HOST` (OSM API base URL), `OSM_OAUTH_CLIENT_ID`/`SECRET` (OAuth2 app credentials).

## Testing

Tests use pytest with `--import-mode=importlib` and pythonpath set to `.` (see `pyproject.toml`). The test file currently imports from `src.compute_diff` which corresponds to functions now in `src.matching`.
