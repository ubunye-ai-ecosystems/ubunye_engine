"""Ubunye Engine performance benchmarks.

Run with::

    python benchmarks/bench_engine.py

Each benchmark reports iterations/sec and p50/p99 latencies.
Results are printed as a table for easy before/after comparison.
"""

from __future__ import annotations

import json
import statistics
import tempfile
import time
from pathlib import Path
from typing import Callable, Dict, List


def _bench(
    name: str,
    fn: Callable[[], None],
    *,
    warmup: int = 3,
    iterations: int = 100,
) -> Dict[str, float]:
    """Run a benchmark and return timing statistics."""
    for _ in range(warmup):
        fn()

    times: List[float] = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)

    times.sort()
    total = sum(times)
    return {
        "name": name,
        "iterations": iterations,
        "total_sec": round(total, 4),
        "ops_per_sec": round(iterations / total, 1),
        "p50_ms": round(times[len(times) // 2] * 1000, 3),
        "p99_ms": round(times[int(len(times) * 0.99)] * 1000, 3),
        "mean_ms": round(statistics.mean(times) * 1000, 3),
        "stdev_ms": round(statistics.stdev(times) * 1000, 3) if len(times) > 1 else 0,
    }


# =====================================================================
# Benchmark: Config loading
# =====================================================================


def bench_config_load() -> Dict[str, float]:
    """Benchmark: load_config with a minimal ETL config."""
    from ubunye.config import load_config

    config_dir = Path(tempfile.mkdtemp())
    config_file = config_dir / "config.yaml"
    config_file.write_text(
        """
MODEL: "etl"
VERSION: "0.1.0"
ENGINE:
  spark_conf:
    spark.sql.shuffle.partitions: "1"
  profiles:
    dev:
      spark_conf:
        spark.app.name: "bench-dev"
    prod:
      spark_conf:
        spark.app.name: "bench-prod"
CONFIG:
  inputs:
    source:
      format: hive
      db_name: raw_db
      tbl_name: claims
  transform: {}
  outputs:
    sink:
      format: s3
      path: "s3a://bucket/fraud/out/"
      mode: overwrite
""",
        encoding="utf-8",
    )

    def run():
        load_config(str(config_dir))

    return _bench("config_load", run, iterations=500)


def bench_config_load_with_jinja() -> Dict[str, float]:
    """Benchmark: load_config with Jinja template variables."""
    from ubunye.config import load_config

    config_dir = Path(tempfile.mkdtemp())
    config_file = config_dir / "config.yaml"
    config_file.write_text(
        """
MODEL: "etl"
VERSION: "0.1.0"
ENGINE:
  spark_conf:
    spark.sql.shuffle.partitions: "1"
CONFIG:
  inputs:
    source:
      format: hive
      db_name: raw_db
      tbl_name: "claims_{{ dt | default('latest') }}"
  transform: {}
  outputs:
    sink:
      format: s3
      path: "s3a://bucket/fraud/{{ dt | default('latest') }}/out/"
      mode: overwrite
""",
        encoding="utf-8",
    )

    def run():
        load_config(str(config_dir), variables={"dt": "20260101"})

    return _bench("config_load_jinja", run, iterations=500)


# =====================================================================
# Benchmark: Entry-point discovery
# =====================================================================


def bench_discovery_cached() -> Dict[str, float]:
    """Benchmark: entry-point lookup (cached path)."""
    from ubunye._internal.discovery import get_lineage_backend

    get_lineage_backend("filesystem")

    def run():
        get_lineage_backend("filesystem")

    return _bench("discovery_cached", run, iterations=10000)


def bench_discovery_cold() -> Dict[str, float]:
    """Benchmark: entry-point lookup (cold cache — first call)."""
    from ubunye._internal.discovery import clear_cache, get_lineage_backend

    def run():
        clear_cache()
        get_lineage_backend("filesystem")

    return _bench("discovery_cold", run, iterations=100)


# =====================================================================
# Benchmark: Lineage I/O
# =====================================================================


def bench_lineage_write() -> Dict[str, float]:
    """Benchmark: FileSystemLineageStore._record_sync (synchronous write)."""
    from ubunye.interfaces.lineage import LineageRecord
    from ubunye.lineage.storage import FileSystemLineageStore

    base = Path(tempfile.mkdtemp()) / "lineage"
    store = FileSystemLineageStore(str(base))

    counter = [0]

    def run():
        counter[0] += 1
        record = LineageRecord(
            run_id=f"bench-{counter[0]}",
            recorded_at="2026-01-01T00:00:00Z",
            engine_version="0.2.0",
            task="bench_task",
            usecase="bench",
            pipeline="perf",
            target="dev",
            status="success",
            duration_sec=0.5,
            metadata={"profile": "dev", "key1": "value1", "key2": "value2"},
        )
        store._record_sync(record)

    return _bench("lineage_write", run, iterations=500)


def bench_lineage_read() -> Dict[str, float]:
    """Benchmark: FileSystemLineageStore.get_run (read by run_id)."""
    from ubunye.interfaces.lineage import LineageRecord
    from ubunye.lineage.storage import FileSystemLineageStore

    base = Path(tempfile.mkdtemp()) / "lineage"
    store = FileSystemLineageStore(str(base))

    record = LineageRecord(
        run_id="bench-read-target",
        recorded_at="2026-01-01T00:00:00Z",
        engine_version="0.2.0",
        task="bench_task",
        usecase="bench",
        pipeline="perf",
        target="dev",
        status="success",
    )
    store._record_sync(record)

    def run():
        store.get_run("bench-read-target")

    return _bench("lineage_read", run, iterations=500)


def bench_lineage_search() -> Dict[str, float]:
    """Benchmark: search across 100 lineage records."""
    from ubunye.interfaces.lineage import LineageRecord
    from ubunye.lineage.storage import FileSystemLineageStore

    base = Path(tempfile.mkdtemp()) / "lineage"
    store = FileSystemLineageStore(str(base))

    for i in range(100):
        store._record_sync(
            LineageRecord(
                run_id=f"search-{i}",
                recorded_at=f"2026-01-{(i % 28) + 1:02d}T00:00:00Z",
                engine_version="0.2.0",
                task="bench_task",
                usecase="bench",
                pipeline="perf",
                target="dev",
                status="success" if i % 3 != 0 else "error",
            )
        )

    def run():
        store.search(status="error")

    return _bench("lineage_search_100", run, iterations=50)


# =====================================================================
# Benchmark: Metadata worker throughput
# =====================================================================


def bench_worker_throughput() -> Dict[str, float]:
    """Benchmark: MetadataWorker submit + flush 100 records."""
    from ubunye._internal.metadata_worker import MetadataWorker

    written = []

    def fast_write(record):
        written.append(record)

    def run():
        written.clear()
        worker = MetadataWorker(fast_write, kind="bench")
        for i in range(100):
            worker.submit({"i": i}, run_id=f"bench-{i}")
        worker.flush(timeout=10.0)

    return _bench("worker_100_records", run, iterations=50)


# =====================================================================
# Benchmark: Registry operations
# =====================================================================


def bench_registry_register() -> Dict[str, float]:
    """Benchmark: FilesystemRegistryBackend.register."""
    from ubunye.models.registry import FilesystemRegistryBackend

    class _FastModel:
        def save(self, path):
            Path(path).mkdir(parents=True, exist_ok=True)
            (Path(path) / "m.bin").write_bytes(b"x")

        def metadata(self):
            return {"f": "dummy"}

    counter = [0]
    base = Path(tempfile.mkdtemp())
    backend = FilesystemRegistryBackend(str(base))

    def run():
        counter[0] += 1
        backend.register(
            use_case="bench",
            model_name="M",
            version=f"1.0.{counter[0]}",
            model=_FastModel(),
            metrics={"acc": 0.95},
        )

    return _bench("registry_register", run, iterations=200)


# =====================================================================
# Runner
# =====================================================================


def main():
    benchmarks = [
        bench_config_load,
        bench_config_load_with_jinja,
        bench_discovery_cached,
        bench_discovery_cold,
        bench_lineage_write,
        bench_lineage_read,
        bench_lineage_search,
        bench_worker_throughput,
        bench_registry_register,
    ]

    results = []
    for bench_fn in benchmarks:
        print(f"  Running {bench_fn.__name__}...", flush=True)
        result = bench_fn()
        results.append(result)

    print()
    print(f"{'Benchmark':<25} {'ops/s':>8} {'p50 ms':>9} {'p99 ms':>9} {'mean ms':>9} {'stdev':>8}")
    print("-" * 75)
    for r in results:
        print(
            f"{r['name']:<25} {r['ops_per_sec']:>8.1f} {r['p50_ms']:>9.3f} "
            f"{r['p99_ms']:>9.3f} {r['mean_ms']:>9.3f} {r['stdev_ms']:>8.3f}"
        )

    results_path = Path("benchmarks") / "results.json"
    results_path.parent.mkdir(parents=True, exist_ok=True)
    results_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"\nResults saved to {results_path}")


if __name__ == "__main__":
    main()
