"""E-10 option 1: keep the F-038 helper processes alive across the tables of one run.

Prototype only (none of the engine's safety: no deadline, no source check, no
budget). A helper is started once, loads content_hash.py by its path as the
engine's helper does, and then answers one Arrow IPC stream after another on the
same stdin: two lane sums per stream. The caller cuts each table into one run per
helper, as _run_helpers does.

Measured: the E-06 run's two large tables (events, then detail) hashed
(a) as the engine does now (fresh helpers per table), (b) with helpers kept alive
(started once, before the first table, start included in the total), and the bare
cost of starting one helper to its first answer. The digests must agree.

    python keepalive.py ROWS [REPEATS]
"""

from __future__ import annotations

import json
import os
import statistics
import subprocess
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import bench  # noqa: E402
import candidates as C  # noqa: E402

BOOT = (
    "import sys, json\n"
    "if sys.path and sys.path[0] == '':\n"
    "    del sys.path[0]\n"
    "import importlib.util as u\n"
    "s = u.spec_from_file_location('_ubunye_rows_v1', sys.argv[1])\n"
    "m = u.module_from_spec(s)\n"
    "sys.modules[s.name] = m\n"
    "s.loader.exec_module(m)\n"
    "import pyarrow as pa\n"
    "pa.set_cpu_count(1)\n"
    "src, out = sys.stdin.buffer, sys.stdout.buffer\n"
    "while True:\n"
    "    try:\n"
    "        reader = pa.ipc.open_stream(src)\n"
    "    except Exception:\n"
    "        break\n"
    "    kinds = dict(json.loads(reader.schema.metadata[b'k'].decode()))\n"
    "    names = list(reader.schema.names)\n"
    "    a = b = 0\n"
    "    for batch in reader:\n"
    "        for start in range(0, batch.num_rows, m._SLICE_ROWS):\n"
    "            x, y = m._slice_lanes(names, kinds, batch.slice(start, m._SLICE_ROWS))\n"
    "            a += x\n"
    "            b += y\n"
    "    out.write(('%d %d\\n' % (a & m._MASK, b & m._MASK)).encode('ascii'))\n"
    "    out.flush()\n"
)


class Pool:
    def __init__(self, ch, workers):
        self.ch = ch
        env = dict(os.environ, UBUNYE_HASH_WORKERS="1")
        self.procs = [
            subprocess.Popen(
                [sys.executable, "-c", BOOT, ch.__file__],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                env=env,
            )
            for _ in range(workers)
        ]

    def lanes(self, table):
        import pyarrow as pa

        ch = self.ch
        names = sorted(table.column_names)
        schema = [(f.name, ch.arrow_kind(f.type)) for f in table.schema]
        sub = table.select(names).replace_schema_metadata(
            {b"k": json.dumps(sorted([list(p) for p in schema])).encode()}
        )
        k = len(self.procs)
        step = -(-sub.num_rows // k)

        def feed(proc, part):
            w = pa.ipc.new_stream(proc.stdin, part.schema)
            for batch in part.to_batches(max_chunksize=ch._SLICE_ROWS):
                w.write_batch(batch)
            w.close()
            proc.stdin.flush()

        threads = [
            threading.Thread(target=feed, args=(p, sub.slice(i * step, step)))
            for i, p in enumerate(self.procs)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        a = b = 0
        for p in self.procs:
            x, y = p.stdout.readline().split()
            a += int(x)
            b += int(y)
        return ch.data_hash(schema, table.num_rows, (a, b))

    def close(self):
        for p in self.procs:
            p.stdin.close()
            p.wait(timeout=30)


def main(rows, repeats=3):
    ch = C.load_content_hash()
    events = bench.gen_events(rows)
    detail = bench.gen_detail(rows)
    tables = [events, detail]
    out = {"rows": rows, "tables": [t.num_rows for t in tables]}

    os.environ["UBUNYE_HASH_WORKERS"] = "4"
    fresh, kept, starts = [], [], []
    for _ in range(repeats):
        t = time.perf_counter()
        d_fresh = [ch.fingerprint_arrow(x).data_hash for x in tables]
        fresh.append(time.perf_counter() - t)

        t = time.perf_counter()
        pool = Pool(ch, 4)
        d_kept = [pool.lanes(x) for x in tables]
        kept.append(time.perf_counter() - t)
        pool.close()
        assert d_fresh == d_kept, (d_fresh, d_kept)

        # Start to first answer of one helper, on a one row table.
        t = time.perf_counter()
        pool = Pool(ch, 1)
        pool.lanes(events.slice(0, 1))
        starts.append(time.perf_counter() - t)
        pool.close()
    out.update(
        {
            "fresh_helpers_s": round(statistics.median(fresh), 3),
            "kept_alive_s": round(statistics.median(kept), 3),
            "saved_s": round(statistics.median(fresh) - statistics.median(kept), 3),
            "one_helper_start_to_answer_s": round(statistics.median(starts), 3),
            "runs": {
                "fresh": [round(x, 3) for x in fresh],
                "kept": [round(x, 3) for x in kept],
                "start": [round(x, 3) for x in starts],
            },
            "digests": d_fresh,
        }
    )
    print(json.dumps(out))


if __name__ == "__main__":
    main(int(sys.argv[1]), int(sys.argv[2]) if len(sys.argv) > 2 else 3)
