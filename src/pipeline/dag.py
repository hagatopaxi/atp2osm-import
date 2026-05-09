# Pipeline DAG — each entry is (step_function, [successor_step_names]) or
#                              (step_function, [successor_step_names], {options})
#
# Options:
#   serial: True — step runs alone, never in parallel with other steps in the same wave.
#                  Use for bandwidth-heavy operations where concurrency would be counterproductive.
#
# To add a step: implement a function in osm.py / atp.py / atp2osm.py,
# import it here, and wire it into PIPELINE.

from src.pipeline.atp import import_atp
from src.pipeline.atp2osm import create_mv_places_brand
from src.pipeline.osm import download_pbf, run_osm2pgsql, setup_mv_places

PIPELINE = {
    "start":        (None,                   ["osm-download", "atp-import"]),
    "osm-download": (download_pbf,           ["osm-import"], {"serial": True}),
    "osm-import":   (run_osm2pgsql,          ["osm-views"]),
    "osm-views":    (setup_mv_places,        ["mv-brand"]),
    "atp-import":   (import_atp,             ["mv-brand"],   {"serial": True}),
    "mv-brand":     (create_mv_places_brand, []),
}
