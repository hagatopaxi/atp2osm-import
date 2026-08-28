import logging
import socket
import sys

from src.config import get_database
from src.pipeline.dag import PIPELINE, record_failure
from src.pipeline.errors import PipelineIncomplete
from src.pipeline.runner import StepFormatter, main

handler = logging.StreamHandler()
handler.setFormatter(StepFormatter(
    fmt="[%(asctime)s] %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
))
logging.root.setLevel(logging.INFO)
logging.root.addHandler(handler)

get_database()  # fail fast if the DB env vars are missing

# No internet (the nightly run has hit DNS outages): stop before any step opens
# a data_imports row, so nothing is left half-done and the site stays up. The
# timer retries tomorrow.
try:
    socket.getaddrinfo("download.geofabrik.de", 443)
except OSError as exc:
    logging.error("No internet access (%s) — aborting, the timer will retry tomorrow", exc)
    sys.exit(1)

# No maintenance flag to set here: each datasource opens and closes its own
# data_imports row (see _db.start_import), which is what puts the web app in
# maintenance.
try:
    main(PIPELINE, record_failure)
except PipelineIncomplete as exc:
    # Everything else ran; exiting non-zero is what makes systemd retry in 4h.
    logging.error("Datasource unavailable (%s) — retrying in 4h", exc)
    sys.exit(1)
