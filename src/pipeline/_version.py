"""What tells the pipeline that its own code changed.

A step must never decide on the freshness of its source alone. Editing
`generic.lua`, `atp.py` or the NSI constants changes what the tables contain
while the upstream timestamp stays put, so a date-only guard holds the change
back until the source happens to publish. Production ran that way once: code
expecting a `subdivisions` table, and a database that had none.

The deployed revision answers it, and better than a digest of hand-picked
sources would. `deploy/run` passes the commit as GIT_COMMIT at build time and
`get_version()` refuses to start without it in production, so the value cannot
be forgotten, stale, or edited by hand. It also covers what a digest misses:
`MATCHED_POI_SQL` lives in `src/matching.py`, the parquet shape in
`ndgeojson_to_parquet.py`, and neither would move an import-local digest.

Coarser, on purpose: any deployed commit reimports every branch on the next
run, a CSS fix included. That costs little — Geofabrik publishes daily, so the
nightly run downloads anyway — and it buys a guard nobody has to remember.

In development the version is a constant, so the guard never fires: rerun the
step by hand (`python -m src.pipeline step osm-import`).
"""

from src.config import get_version


def app_version() -> str:
    """The deployed revision every rebuild guard compares against."""
    return get_version()
