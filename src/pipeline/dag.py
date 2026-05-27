# Pipeline DAG — each entry is (step_function, [successor_step_names]) or
#                              (step_function, [successor_step_names], {options})
#
# Options:
#   lock: "<name>" — steps sharing the same lock name are serialized via a
#                    mutex; only one runs at a time, others queue behind it.
#                    Use for bandwidth-heavy operations (e.g. lock="network")
#                    where true concurrency would be counterproductive.
#
# Execution model: each branch runs independently — a step starts as soon as
# all its direct predecessors are done, with no synchronisation barrier between
# unrelated branches.
#
# To add a step: implement a function in osm.py / atp.py / atp2osm.py,
# import it here, and wire it into PIPELINE.

from src.pipeline.atp import (
    cleanup_atp,
    create_parquet_atp,
    download_atp,
    extract_atp,
    import_atp,
)
from src.pipeline.atp2osm import create_mv_places_brand
from src.pipeline.ndgeojson_to_parquet import convert_atp, split_atp
from src.pipeline.osm import download_pbf, run_osm2pgsql, setup_mv_places

PIPELINE = {
    "start": (None, ["osm-download", "atp-download"]),
    "osm-download": (download_pbf, ["osm-import"], {"lock": "network"}),
    "osm-import": (run_osm2pgsql, ["osm-views"]),
    "osm-views": (setup_mv_places, ["mv-brand"]),
    "atp-download": (download_atp, ["atp-extract"], {"lock": "network"}),
    "atp-extract": (extract_atp, ["atp-convert"]),
    "atp-convert": (convert_atp, ["atp-split"]),
    "atp-split": (split_atp, ["atp-parquet"]),
    "atp-parquet": (create_parquet_atp, ["atp-import"]),
    "atp-import": (import_atp, ["mv-brand"]),
    "mv-brand": (create_mv_places_brand, ["cleanup"]),
    "cleanup": (cleanup_atp, []),
}
