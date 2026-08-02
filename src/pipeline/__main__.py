import logging
import sys

from src.config import get_database
from src.db import set_maintenance
from src.pipeline._db import connect
from src.pipeline.dag import PIPELINE, record_failure
from src.pipeline.runner import StepFormatter, main

handler = logging.StreamHandler()
handler.setFormatter(StepFormatter(
    fmt="[%(asctime)s] %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
))
logging.root.setLevel(logging.INFO)
logging.root.addHandler(handler)

get_database()  # fail fast if the DB env vars are missing

if sys.argv[1:2] == ["list"]:  # read-only command, no DB, no maintenance flag
    main(PIPELINE, record_failure)
    raise SystemExit(0)

# Maintenance mode covers the whole run: the web app serves a 503 page instead
# of 500s while the tables are rebuilt. Cleared only on success — a crashed or
# failed run stays in maintenance until an admin relaunches the pipeline.
conn = connect()
try:
    set_maintenance(conn, True)
    main(PIPELINE, record_failure)
    set_maintenance(conn, False)
finally:
    conn.close()
