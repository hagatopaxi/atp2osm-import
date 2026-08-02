import os
import time
import logging
import requests
import random

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


def fetch_osm_users(user_ids):
    """Batch fetch user display names from the OSM API."""
    from src.config import get_settings
    if not user_ids:
        return {}
    settings = get_settings()
    ids_param = ",".join(str(uid) for uid in user_ids)
    try:
        resp = requests.get(
            f"{settings.api_url}/api/0.6/users.json?users={ids_param}",
            timeout=5,
            headers={"User-Agent": f"atp2osm/{settings.app_version}"},
        )
        resp.raise_for_status()
        data = resp.json()
        return {
            u["user"]["id"]: u["user"]["display_name"] for u in data.get("users", [])
        }
    except Exception:
        logger.exception("Failed to fetch OSM user details")
        return {}


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
