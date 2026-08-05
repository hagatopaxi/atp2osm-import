import os
import time
import logging
import requests
import random

from datetime import datetime, timedelta, timezone
from pathlib import Path


logger = logging.getLogger(__name__)


def delete_file_if_exists(file_path):
    """
    Delete a file if it exists.
    """
    if os.path.exists(file_path):
        os.remove(file_path)



def clean_debug_folder():
    for file_path in os.listdir("./data/debug"):
        os.remove(f"./data/debug/{file_path}")


print


def download_large_file(
    url: str,
    destination: str | Path,
    chunk_size: int = 8192,
    progress_interval: int = 15,
) -> None:
    """
    Stream a file from *url* to *destination* while printing a progress
    percentage roughly every ``progress_interval`` seconds.

    Parameters
    ----------
    url               : URL of the file to download.
    destination       : Local path where the file will be saved.
    chunk_size        : Number of bytes read per iteration (default 8192).
    progress_interval : Seconds between progress updates (default 15 s).
    """
    dest_path = Path(destination)
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        # ``stream=True`` gives us an iterator over the response body.
        with requests.get(url, stream=True, timeout=30) as resp:
            resp.raise_for_status()

            # Try to obtain the total size from the HTTP header.
            total_bytes = resp.headers.get("Content-Length")
            total_bytes = (
                int(total_bytes) if total_bytes and total_bytes.isdigit() else None
            )

            # If we don’t know the size we’ll fall back to a simple byte counter.
            show_percent = total_bytes is not None

            written = 0
            start = last_report = time.time()

            with open(dest_path, "wb") as out_file:
                for chunk in resp.iter_content(chunk_size=chunk_size):
                    if not chunk:  # skip keep‑alive chunks
                        continue
                    out_file.write(chunk)
                    written += len(chunk)

                    now = time.time()
                    if now - last_report >= progress_interval:
                        elapsed = now - start
                        speed = written / elapsed if elapsed > 0 else 0

                        if show_percent:
                            pct = (written / total_bytes) * 100
                            logger.info(
                                f"[{elapsed:6.1f}s] "
                                f"{pct:5.1f}% ({written:,} / {total_bytes:,} bytes) "
                                f"@ {speed / 1024:,.1f} KiB/s"
                            )
                        else:
                            # No length header → just show bytes transferred.
                            logger.info(
                                f"[{elapsed:6.1f}s] "
                                f"{written:,} bytes downloaded "
                                f"@ {speed / 1024:,.1f} KiB/s"
                            )
                        last_report = now

            # ----- final summary -------------------------------------------------
            if written == 0:
                dest_path.unlink(missing_ok=True)
                raise ValueError(f"Downloaded file is empty (0 bytes): {url}")

            total_elapsed = time.time() - start
            avg_speed = written / total_elapsed if total_elapsed > 0 else 0
            if show_percent:
                logger.info(
                    f"\nDownload complete: 100.0% ({written:,} / {total_bytes:,} bytes) "
                    f"in {total_elapsed:.1f}s ({avg_speed / 1024:,.1f} KiB/s)."
                )
            else:
                logger.info(
                    f"\nDownload complete: {written:,} bytes in "
                    f"{total_elapsed:.1f}s ({avg_speed / 1024:,.1f} KiB/s)."
                )

    except requests.exceptions.RequestException:
        dest_path.unlink(missing_ok=True)
        raise


# Le sort d'une intégration. Le détail du type d'erreur vit une ligne plus
# bas, sur le département : c'est là qu'il a un sens.
IMPORT_STATUSES = ("success", "partial", "cancelled", "error")


def status_group(status: str | None) -> str:
    """Le statut lui-même, ou 'none' pour une marque jamais intégrée."""
    return status if status in IMPORT_STATUSES else "none"


def build_filters(args, spec):
    """Build a SQL WHERE clause from the query string.

    `spec` declares which filters the page exposes, and on which columns:

        {"q":      ("brand_name", "brand_wikidata"),  # ILIKE search
         "status": "status",                          # one of IMPORT_STATUSES
         "user":   "osm_user_id",                     # integer equality
         "date":   "import_date"}                     # ?from= and ?to= bounds

    A filter missing from `spec` is ignored even when present in the URL. Column
    names always come from the code, never from the request.

    Returns (where_sql, params, active_filters).
    """
    where, params, active = [], [], {}

    if "q" in spec:
        q = args.get("q", "").strip()
        if q:
            columns = spec["q"]
            where.append("(" + " OR ".join(f"{c} ILIKE %s" for c in columns) + ")")
            params += [f"%{q}%"] * len(columns)
            active["q"] = q

    if "status" in spec:
        status = args.get("status", "")
        if status in IMPORT_STATUSES:
            where.append(f"{spec['status']} = %s")
            params.append(status)
            active["status"] = status

    if "user" in spec:
        user = args.get("user", type=int)
        if user:
            where.append(f"{spec['user']} = %s")
            params.append(user)
            active["user"] = user

    if "date" in spec:
        column = spec["date"]
        date_from = args.get("from", "").strip()
        if date_from:
            where.append(f"{column} >= %s")
            params.append(date_from)
            active["from"] = date_from

        date_to = args.get("to", "").strip()
        if date_to:
            # inclusive bound: everything dated on the given day
            where.append(f"{column} < %s::date + 1")
            params.append(date_to)
            active["to"] = date_to

    return ("WHERE " + " AND ".join(where) if where else ""), params, active


def filter_brands(rows, args):
    """Filter the brand list from the query string.

    Counterpart of build_filters() for an already in-memory list — see the comment
    in the /brands view for why. The only filter proper to this list is the
    'never imported' status.

    Returns (filtered_rows, active_filters).
    """
    active = {}

    q = args.get("q", "").strip()
    if q:
        needle = q.lower()
        rows = [
            r
            for r in rows
            if needle in r["brand"].lower() or needle in r["brand_wikidata"].lower()
        ]
        active["q"] = q

    status = args.get("status", "")
    if status:
        rows = [r for r in rows if status_group(r["last_status"]) == status]
        active["status"] = status

    date_from = args.get("from", "").strip()
    if date_from:
        rows = [
            r for r in rows if r["last_import"] and str(r["last_import"].date()) >= date_from
        ]
        active["from"] = date_from

    date_to = args.get("to", "").strip()
    if date_to:
        rows = [
            r for r in rows if r["last_import"] and str(r["last_import"].date()) <= date_to
        ]
        active["to"] = date_to

    return rows, active


# ponytail: cache mémoire par process (pseudo OSM -> date d'expiration). Un
# display_name ne bouge quasiment jamais, une semaine suffit. Si plusieurs
# workers tournent, chacun a le sien — c'est voulu, pas la peine de sortir Redis.
OSM_USER_CACHE_TTL = timedelta(weeks=1)
_osm_user_cache: dict[int, tuple[str, datetime]] = {}


def fetch_osm_users(user_ids):
    """Batch fetch user display names from the OSM API, cached one week."""
    from src.config import get_settings
    if not user_ids:
        return {}

    now = datetime.now(timezone.utc)
    cached = {}
    missing = []
    for uid in user_ids:
        entry = _osm_user_cache.get(uid)
        if entry and entry[1] > now:
            cached[uid] = entry[0]
        else:
            missing.append(uid)
    if not missing:
        return cached

    settings = get_settings()
    ids_param = ",".join(str(uid) for uid in missing)
    try:
        resp = requests.get(
            f"{settings.api_url}/api/0.6/users.json?users={ids_param}",
            timeout=5,
            headers={"User-Agent": f"atp2osm/{settings.app_version}"},
        )
        resp.raise_for_status()
        fetched = {
            u["user"]["id"]: u["user"]["display_name"]
            for u in resp.json().get("users", [])
        }
    except Exception:
        logger.exception("Failed to fetch OSM user details")
        return cached  # l'API est muette : on sert au moins ce qu'on a

    expires = now + OSM_USER_CACHE_TTL
    for uid, name in fetched.items():
        _osm_user_cache[uid] = (name, expires)
    return cached | fetched


def get_rand_items(arr: list, n: int) -> list:
    """
    Returns a new array which contains n random items.
    No duplicate
    """
    if n >= len(arr):
        return arr

    items_idx = []
    length = len(arr)
    for _ in range(n):
        rand_idx = random.randint(0, length - 1)
        max_iter = 15
        i = 0
        while rand_idx in items_idx and i < max_iter:
            rand_idx = random.randint(0, length - 1)
            i += 1

        if i != max_iter:
            items_idx.append(rand_idx)
    return [arr[idx] for idx in items_idx]


def _determine_import_status(results: list[dict]) -> str:
    """Derive the import_history status from its département rows.

    All succeeded → success ; none → error ; a mix → partial. Le type d'erreur
    (API OSM ou inattendue) reste sur la ligne du département.
    """
    statuses = {r["status"] for r in results}
    if statuses <= {"success"}:
        return "success"
    return "partial" if "success" in statuses else "error"
