#!/usr/bin/env bash
# Catalog workflow: extract the marked strings, report them into every .po,
# then compile. One command, so a catalog is never updated without being
# compiled — the .mo is what gettext actually reads.
set -euo pipefail
cd "$(dirname "$0")/.."

# The identity flags are here rather than typed each time: pybabel init copies
# the .pot header into every new language, so a placeholder left in it would be
# duplicated once per catalog.
pybabel extract -F babel.cfg -o website/translations/messages.pot --no-wrap \
    --project atp2osm \
    --copyright-holder "the atp2osm contributors" \
    --msgid-bugs-address https://github.com/hagatopaxi/atp2osm-import/issues .
pybabel update -i website/translations/messages.pot -d website/translations --no-wrap
pybabel compile -d website/translations
