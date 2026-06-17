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

import logging
import traceback

from src.pipeline._db import connect
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

logger = logging.getLogger(__name__)

PIPELINE = {
    "start": (None, ["osm-download", "atp-download"]),
    "osm-download": (download_pbf, ["osm-import"], {"lock": "network"}),
    "osm-import": (run_osm2pgsql, ["osm-views"], {"lock": "cpu"}),
    "osm-views": (setup_mv_places, ["mv-brand"]),
    "atp-download": (download_atp, ["atp-extract"], {"lock": "network"}),
    "atp-extract": (extract_atp, ["atp-convert"], {"lock": "cpu"}),
    "atp-convert": (convert_atp, ["atp-split"], {"lock": "cpu"}),
    "atp-split": (split_atp, ["atp-parquet"], {"lock": "cpu"}),
    "atp-parquet": (create_parquet_atp, ["atp-import"], {"lock": "cpu"}),
    "atp-import": (import_atp, ["mv-brand"]),
    "mv-brand": (create_mv_places_brand, ["cleanup"]),
    "cleanup": (cleanup_atp, []),
}


def record_failure(step_name, exc):
    """Failure hook for the runner: persist a failing step as an error row
    holding the step name and its full stack trace, so a refresh can be
    diagnosed later.

    Opens its own connection (the step's own one may be in a broken
    transaction) and never raises — masking the original error would be worse.
    """
    comment = f"step '{step_name}' failed\n" + "".join(
        traceback.format_exception(type(exc), exc, exc.__traceback__)
    )
    # mv-brand, cleanup, … don't belong to the osm/atp branches.
    import_type = step_name.split("-", 1)[0]
    if import_type not in ("osm", "atp"):
        import_type = "pipeline"
    try:
        conn = connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO data_imports (type, date, status, comment) "
                    "VALUES (%s, NULL, 'error', %s)",
                    (import_type, comment),
                )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        logger.exception("Could not record failure for step '%s'", step_name)
