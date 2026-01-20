import os
import time
import functools
from typing import Callable, Any, TypeVar, cast, Tuple
import logging


logger = logging.getLogger(__name__)


def limit_offset(count, step):
    """
    Generator that returns tuple of (start, end) for each step in a range for a database limit offset values.
    For example, limit_offset(14, 5) will yield (0, 5), (5, 10), (10, 14).
    """
    for i in range(0, count, step):
        yield (i, min(i + step, count))


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
    print(f"Total: {total}")
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
