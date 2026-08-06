import logging

from src.config import get_database
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

# No maintenance flag to set here: each datasource opens and closes its own
# data_imports row (see _db.start_import), which is what puts the web app in
# maintenance.
main(PIPELINE, record_failure)
