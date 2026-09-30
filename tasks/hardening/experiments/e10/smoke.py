"""Quick checks: C SHA-256 (both paths) against hashlib; every candidate on one slice."""

import ctypes
import hashlib
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import candidates as C  # noqa: E402
import pyarrow as pa  # noqa: E402

lib = C.c_dll()
print("sha-ni on this cpu:", lib.rows_v1_has_shani())
rng = random.Random(7)
for mode in (0, 1):
    print("mode", mode, "->", lib.rows_v1_set_mode(mode))
    bad = 0
    for ln in list(range(0, 300)) + [rng.randrange(300, 5000) for _ in range(200)]:
        for _ in range(3):
            b = bytes(rng.randrange(256) for _ in range(ln))
            out = ctypes.create_string_buffer(32)
            lib.rows_v1_sha256(b, len(b), out)
            if out.raw != hashlib.sha256(b).digest():
                bad += 1
    print("  mismatches:", bad)
lib.rows_v1_set_mode(-1)

ch = C.load_content_hash()
lines = pa.array(
    ['{"a":1,"s":"x"}', "{}", '{"é":"ü\\u0001"}', "x" * 200, "y" * 55, "z" * 56, "w" * 64, ""]
    + [f'{{"id":{i},"cat":"c_{i % 7}"}}' for i in range(5000)],
    pa.string(),
).slice(3)
ref = ch._arrow_lanes(lines)
print("ref", ref)
for name, fn in list(C.CANDIDATES.items()) + [("numba_parallel", C.lanes_numba_parallel)]:
    t = time.perf_counter()
    got = fn(lines)
    print(
        f"{name:15s} {'OK ' if tuple(x & C.MASK for x in got) == ref else 'BAD'} "
        f"{time.perf_counter() - t:.3f}s"
    )
