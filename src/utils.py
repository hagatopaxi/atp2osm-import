import os
import time
import functools
import logging
import requests
import random

from pathlib import Path
from typing import Callable, Any, TypeVar, cast


logger = logging.getLogger(__name__)


def delete_file_if_exists(file_path):
    """
    Delete a file if it exists.
    """
    if os.path.exists(file_path):
        os.remove(file_path)


F = TypeVar("F", bound=Callable[..., Any])


def timer(func: F) -> F:
    """
    Décorateur qui chronomètre l'exécution d'une fonction.

    Exemple d’utilisation :

    @timer
    def my_job():
        data = [x**2 for x in range(10_000)]
        filtered = [x for x in data if x % 2 == 0]
        return sum(filtered)

    total = my_job()          # affichera « Execution took X seconds »
    """

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start = time.perf_counter()
        try:
            # Exécution de la fonction décorée
            result = func(*args, **kwargs)
        finally:
            # Le bloc finally garantit que le temps est affiché même si
            # la fonction lève une exception.
            end = time.perf_counter()
            duration = end - start
            if duration > 3:
                logger.info(f"{func.__name__} – Execution took {duration:.0f} seconds")
        return result

    # Cast pour que le type retourné corresponde à celui du décorateur
    return cast(F, wrapper)


def clean_debug_folder():
    for file_path in os.listdir("./data/debug"):
        os.remove(f"./data/debug/{file_path}")


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

    except requests.exceptions.RequestException as exc:
        logger.info(f"Error downloading the file: {exc}")


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
