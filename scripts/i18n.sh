#!/usr/bin/env bash
# Catalog workflow: extract the marked strings, report them into every .po,
# then compile. One command, so a catalog is never updated without being
# compiled — the .mo is what gettext actually reads.
set -euo pipefail
cd "$(dirname "$0")/.."

pybabel extract -F babel.cfg -o website/translations/messages.pot \
    --no-wrap --project atp2osm .
pybabel update -i website/translations/messages.pot -d website/translations --no-wrap
pybabel compile -d website/translations
