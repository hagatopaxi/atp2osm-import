# Pipeline

This package manages the data refresh pipeline: downloading fresh OSM and ATP data, importing it into PostGIS, and building the materialized views the application relies on.

## Concept

The pipeline is a directed acyclic graph (DAG) of steps. Each step knows what comes **after** it, not what came before. This is a deliberate choice: you trigger a starting point and the runner propagates forward automatically.

```
start ─┬─ osm-download → osm-import → osm-views ──────────────────────────────────────────┐
       │                                                                                  ├─ mv-brand → cleanup
       └─ atp-download → atp-extract → atp-convert → atp-split → atp-parquet → atp-import ┘
```

`start` is a virtual entry point with no logic of its own. It simply declares which steps kick off the pipeline, making the starting point immediately readable.

Running `from osm-views` executes `osm-views` then `mv-brand`, without touching the ATP branch. The runner trusts that whatever is already in the database is current.

## Running the pipeline

```bash
# Full pipeline — all steps in correct order
./run-pipeline.sh

# With explicit step
./run-pipeline.sh from atp-import

# Or run directly inside a dev environment
uv run --env-file .env python -m src.pipeline from atp-import
uv run --env-file .env python -m src.pipeline list
```

Each step self-manages its own skip logic by querying the database or checking for the presence of a downloaded file. Running the full pipeline twice in a row is safe — steps that find their data already current will exit early.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `PIPELINE_WORKERS` | `cpu_count // 2` | Number of parallel workers for CPU-bound steps (`atp-convert`, `atp-split`, `atp-parquet`). Set to a lower value on a dev machine to stay responsive, higher on a dedicated server. |

## Files

### `dag.py` — the pipeline definition

This is the only file you need to read to understand the full pipeline. It contains a single `PIPELINE` dict where each entry maps a step name to its function and successors:

```python
PIPELINE = {
    "osm-download": (download_pbf,    ["osm-import"], {"serial": True}),
    "osm-import":   (run_osm2pgsql,   ["osm-views"]),
    ...
}
```

The optional third element is a dict of step options. The only option currently supported is `serial: True`, which prevents the step from running concurrently with other steps in the same execution wave. Use it for bandwidth-heavy operations where parallelism would be counterproductive.

`dag.py` has no knowledge of the runner. It only imports from the step files.

### Step files — `osm.py`, `atp.py`, `atp2osm.py`

Each file groups the steps for one domain. A step is just a plain Python function with no arguments. It opens its own database connection, does its work, and closes it.

```
osm.py                   — download_pbf, run_osm2pgsql, setup_mv_places
atp.py                   — download_atp, extract_atp, create_parquet_atp, import_atp, cleanup_atp
ndgeojson_to_parquet.py  — convert_to_parquet, convert_atp, split_atp, convert_geojson_to_ndgeojson, split_ndgeojson
atp2osm.py               — create_mv_places_brand
```

Steps are responsible for deciding whether they need to run. A step that finds its data already up-to-date should log a message and return early rather than doing unnecessary work.

### `runner.py` — the execution engine

The runner reads the `PIPELINE` dict and handles two things:

- **Graph traversal**: given a starting point, it performs a forward BFS to collect all downstream steps, then topologically sorts them into execution waves.
- **Wave execution**: steps within the same wave that are not marked `serial` run in parallel using a thread pool. Waves execute sequentially — a wave only starts once the previous one has fully completed.

The runner knows nothing about OSM, ATP, or databases. It only calls functions and manages the execution order.

### `_db.py` — shared database helpers

Internal module (not a step file) providing `connect()`, `last_import_date()`, and `record_import()`. Used by step files to open connections and track import history in the `data_imports` table.

## Adding a step

1. Write a plain `def my_step():` function in the appropriate domain file (`osm.py`, `atp.py`, or a new file if it belongs to a new domain).
2. Import it in `dag.py`.
3. Add it to `PIPELINE` with its successors, and wire it into the graph by adding it as a successor of the step that should precede it.

No changes to the runner are needed.
