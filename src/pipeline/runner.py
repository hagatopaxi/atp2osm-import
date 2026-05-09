import logging
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

_step_ctx = threading.local()


class StepFormatter(logging.Formatter):
    def format(self, record):
        base = super().format(record)
        step = getattr(_step_ctx, "name", None)
        return f"[{step}] {base}" if step else base


def _fn(entry):
    return entry[0]


def _succs(entry):
    return entry[1]


def _is_serial(entry):
    return len(entry) > 2 and entry[2].get("serial", False)


def _reachable(pipeline, start):
    visited, queue = set(), [start]
    while queue:
        node = queue.pop(0)
        if node in visited:
            continue
        visited.add(node)
        queue.extend(_succs(pipeline[node]))
    return visited



def _waves(pipeline, nodes):
    """Group nodes into sequential waves. Within a wave, non-serial steps run in parallel.
    Serial steps always get their own wave."""
    subset = set(nodes)
    nexts = {n: [s for s in _succs(pipeline[n]) if s in subset] for n in subset}
    in_degree = {n: 0 for n in subset}
    for succs in nexts.values():
        for s in succs:
            in_degree[s] += 1

    waves = []
    while in_degree:
        ready = sorted(n for n, d in in_degree.items() if d == 0)
        serial = [n for n in ready if _is_serial(pipeline[n])]
        parallel = [n for n in ready if not _is_serial(pipeline[n])]

        for name in serial:
            waves.append([name])
        if parallel:
            waves.append(parallel)

        for node in ready:
            del in_degree[node]
            for s in nexts[node]:
                in_degree[s] -= 1

    return waves


def _run_step(pipeline, name):
    fn = _fn(pipeline[name])
    if fn is None:
        return
    _step_ctx.name = name
    try:
        logger.info("▶  start")
        fn()
        logger.info("✓  done")
    finally:
        _step_ctx.name = None


def run(pipeline, nodes):
    for wave in _waves(pipeline, nodes):
        if len(wave) == 1:
            _run_step(pipeline, wave[0])
        else:
            with ThreadPoolExecutor(max_workers=len(wave)) as executor:
                futures = {executor.submit(_run_step, pipeline, name): name for name in wave}
                for future in as_completed(futures):
                    future.result()


def main(pipeline):
    args = sys.argv[1:]
    cmd = args[0] if args else "refresh"

    if cmd == "refresh":
        run(pipeline, _reachable(pipeline, "start"))

    elif cmd == "from":
        if len(args) < 2:
            _usage()
        start = args[1]
        _check(pipeline, start)
        run(pipeline, _reachable(pipeline, start))

    elif cmd == "step":
        if len(args) < 2:
            _usage()
        name = args[1]
        _check(pipeline, name)
        _run_step(pipeline, name)

    elif cmd == "list":
        for wave in _waves(pipeline, set(pipeline)):
            tag = " (serial)" if len(wave) == 1 and _is_serial(pipeline[wave[0]]) else ""
            for name in wave:
                _, succs = pipeline[name][:2]
                arrow = f"  →  {', '.join(succs)}" if succs else ""
                print(f"  {name}{arrow}{tag}")
        return

    else:
        _usage()


def _check(pipeline, name):
    if name not in pipeline:
        print(f"Unknown step '{name}'. Available: {', '.join(pipeline)}", file=sys.stderr)
        sys.exit(1)


def _usage():
    print(
        "Usage:\n"
        "  python -m src.pipeline                 — full pipeline\n"
        "  python -m src.pipeline refresh         — same\n"
        "  python -m src.pipeline from <step>     — step + all downstream\n"
        "  python -m src.pipeline step <step>     — single step only\n"
        "  python -m src.pipeline list            — print pipeline order",
        file=sys.stderr,
    )
    sys.exit(1)
