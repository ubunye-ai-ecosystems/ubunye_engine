"""A performance guard: a change must not make the engine slower than the code it changes.

It times a fixed set of engine operations on two copies of the source, the
change and its base (for a pull request, the branch it goes into), and fails
if any operation is more than 30% slower (plus 5 ms, so a timing of a few
milliseconds cannot fail on noise alone):

    python benchmarks/guard.py --base ../base

Each copy is timed in a fresh process with ``PYTHONPATH`` pointing at it, so
both use the same installed dependencies; the copies take turns for several
rounds and each keeps its best time, which damps the noise of a shared CI
machine. An operation the base cannot run (it is newer) is reported and
skipped. No Spark: this guards the engine's own overhead, the pandas backend,
CSV reading and the run record's hash, which is what the engine adds on top of
any backend.

The check that found the slow CSV reader before this guard existed took 0.78 s
of a 0.82 s read; this is the net for the next one.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Callable, Dict

TOLERANCE = 1.30
ALLOWANCE_S = 0.005
HERE = Path(__file__).resolve()

# ---------------------------------------------------------------- the timings


def _data(root: Path) -> Dict[str, Path]:
    """Files every timed operation reads: the same bytes for both copies."""
    names = ["Mokoena, Thabo", "Dlamini, Anna", "Naidoo, Priya", "van Wyk, Pieter"]
    plain = root / "plain.csv"
    with open(plain, "w", encoding="utf-8", newline="\n") as f:
        f.write("id,name,score,passed,day\n")
        for i in range(60_000):
            f.write(
                f'{i},"{names[i % 4]}",{i % 97}.5,{str(i % 3 == 0).lower()},2024-01-{i % 28 + 1:02d}\n'
            )
    messy = root / "messy.csv"
    with open(messy, "w", encoding="utf-8", newline="\n") as f:
        f.write("id,name\n")
        for i in range(20_000):
            nick = '"Mokoena, Thabo ""T"""' if i % 10 == 0 else f'"{names[i % 4]}"'
            f.write(f"{i},{nick}\n")
    task = root / "uc" / "pkg" / "copy"
    task.mkdir(parents=True)
    (task / "transformations.py").write_text(
        "from ubunye.core.interfaces import Task\n\n\n"
        "class Copy(Task):\n"
        "    def transform(self, sources):\n"
        '        return {"out": sources["src"]}\n',
        encoding="utf-8",
    )
    (task / "config.yaml").write_text(
        "MODEL: etl\n"
        'VERSION: "1.0.0"\n'
        "CONFIG:\n"
        "  inputs:\n"
        "    src:\n"
        "      format: s3\n"
        f'      path: "{plain.as_posix()}"\n'
        "      file_format: csv\n"
        "      options:\n"
        '        header: "true"\n'
        '        inferSchema: "true"\n'
        "  transform: {}\n"
        "  outputs:\n"
        "    out:\n"
        "      format: s3\n"
        f'      path: "{(root / "out").as_posix()}"\n'
        "      file_format: parquet\n"
        "      mode: overwrite\n",
        encoding="utf-8",
    )
    return {"plain": plain, "messy": messy, "task": task}


def _best(fn: Callable[[], object], repeat: int) -> float:
    fn()  # warm up: imports, caches
    best = float("inf")
    for _ in range(repeat):
        start = time.perf_counter()
        fn()
        best = min(best, time.perf_counter() - start)
    return best


def measure(repeat: int) -> Dict[str, object]:
    """Time every operation with whichever ``ubunye`` is on the path."""
    import ubunye
    from ubunye.backends.pandas_backend import PandasBackend
    from ubunye.config import load_config
    from ubunye.lineage.content_hash import fingerprint

    options = {"header": "true", "inferSchema": "true"}
    results: Dict[str, object] = {"source": str(Path(ubunye.__file__).parent)}
    with tempfile.TemporaryDirectory() as tmp:
        files = _data(Path(tmp))
        frame = PandasBackend().read_frame("csv", str(files["plain"]), options=options)
        variables = {"dt": None, "dtf": None, "mode": "DEV"}
        operations: Dict[str, Callable[[], object]] = {
            "config_load": lambda: load_config(str(files["task"]), variables=variables),
            "csv_read_plain_60k": lambda: PandasBackend().read_frame(
                "csv", str(files["plain"]), options=options
            ),
            "csv_read_spark_quotes_20k": lambda: PandasBackend().read_frame(
                "csv", str(files["messy"]), options=options
            ),
            "run_record_hash_60k": lambda: fingerprint(frame),
            "run_task_pandas_lineage": lambda: ubunye.run_task(
                str(files["task"]), backend="pandas", lineage=True
            ),
        }
        timings: Dict[str, object] = {}
        for name, fn in operations.items():
            try:
                timings[name] = _best(fn, repeat)
            except Exception as exc:  # the base may not have this yet
                timings[name] = f"cannot run: {type(exc).__name__}: {exc}"[:200]
        results["timings"] = timings
    return results


# ------------------------------------------------------------- the comparison


def _time_copy(source: Path, repeat: int) -> Dict[str, object]:
    env = dict(os.environ, PYTHONPATH=str(source))
    done = subprocess.run(
        [sys.executable, str(HERE), "--measure", "--repeat", str(repeat)],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(source),
    )
    if done.returncode != 0:
        raise SystemExit(f"timing {source} failed:\n{done.stderr[-3000:]}")
    return json.loads(done.stdout.strip().splitlines()[-1])


def compare(base: Path, head: Path, rounds: int, repeat: int) -> int:
    best: Dict[str, Dict[str, float]] = {"base": {}, "head": {}}
    notes: Dict[str, str] = {}
    for _ in range(rounds):
        for label, source in (("base", base), ("head", head)):
            result = _time_copy(source, repeat)
            for name, value in result["timings"].items():  # type: ignore[union-attr]
                if isinstance(value, str):
                    notes[f"{label}:{name}"] = value
                    continue
                best[label][name] = min(best[label].get(name, float("inf")), value)

    lines = ["| operation | base (ms) | this change (ms) | change |", "|---|---|---|---|"]
    slower = []
    for name in sorted(best["head"]):
        now = best["head"][name]
        if name not in best["base"]:
            lines.append(f"| {name} | - | {now * 1000:.1f} | new |")
            continue
        was = best["base"][name]
        change = (now - was) / was * 100 if was else 0.0
        verdict = ""
        if now > was * TOLERANCE + ALLOWANCE_S:
            slower.append(name)
            verdict = " SLOWER"
        lines.append(f"| {name} | {was * 1000:.1f} | {now * 1000:.1f} | {change:+.0f}%{verdict} |")
    for key, note in sorted(notes.items()):
        lines.append(f"\n{key}: {note}")
    report = "\n".join(lines)
    print(report)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write("### Performance against the base\n\n" + report + "\n")
    if slower:
        print(
            f"\nSlower than the base by more than {round((TOLERANCE - 1) * 100)}%: "
            + ", ".join(slower),
            file=sys.stderr,
        )
        return 1
    print(f"\nOK: no operation is more than {round((TOLERANCE - 1) * 100)}% slower than the base.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base", type=Path, help="a checkout of the code to compare with")
    parser.add_argument("--head", type=Path, default=HERE.parents[1], help="this checkout")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--measure", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.measure:
        print(json.dumps(measure(args.repeat)))
        return 0
    if args.base is None:
        parser.error("--base is required")
    return compare(args.base.resolve(), args.head.resolve(), args.rounds, args.repeat)


if __name__ == "__main__":
    sys.exit(main())
