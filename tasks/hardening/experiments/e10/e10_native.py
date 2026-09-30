"""E-10 prototype: rows-v1 on threads with a native lane kernel (not engine code).

The shape a fix could take. The canonical lines are built by the engine's own
_slice_lines (unchanged); each slice's lines are hashed by the C kernel
(rows_v1_lanes.dll / .so, loaded with ctypes, which releases the GIL). Arrow's
compute kernels release the GIL too, so the slices run on a thread pool in this
process: no helper processes, no IPC, no start-up per table.

If the kernel cannot be loaded, the engine's function is used unchanged.

    fingerprint_arrow_native(table, threads=4)
    install()   # rebinds ubunye.lineage.content_hash.fingerprint_arrow (for E-06 runs)
"""

from __future__ import annotations

import ctypes
import os
from concurrent.futures import ThreadPoolExecutor

_lib = None


def _kernel():
    global _lib
    if _lib is None:
        path = os.environ.get("E10_DLL") or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "rows_v1_lanes.dll"
        )
        lib = ctypes.CDLL(path)
        lib.rows_v1_lanes.argtypes = [
            ctypes.c_void_p,
            ctypes.c_int64,
            ctypes.c_void_p,
            ctypes.c_void_p,
        ]
        lib.rows_v1_lanes.restype = None
        _lib = lib
    return _lib


def native_lanes(lines):
    import numpy as np
    import pyarrow as pa

    lib = _kernel()
    n = len(lines)
    buffers = lines.buffers()
    offsets = np.frombuffer(buffers[1], dtype=np.int32, count=n + 1, offset=lines.offset * 4)
    data = buffers[2] if buffers[2] is not None else pa.py_buffer(bytes(1))
    out = (ctypes.c_uint64 * 2)(0, 0)
    lib.rows_v1_lanes(offsets.ctypes.data, n, data.address, ctypes.addressof(out))
    return int(out[0]), int(out[1])


def fingerprint_arrow_native(table, threads=None, ch=None):
    if ch is None:
        from ubunye.lineage import content_hash as ch
    threads = threads or int(os.environ.get("E10_THREADS", "4"))
    schema = [(f.name, ch.arrow_kind(f.type)) for f in table.schema]
    kinds = dict(schema)
    names = sorted(table.column_names)
    a = b = 0
    if names:

        def one(piece):
            lines = ch._slice_lines(names, kinds, piece)
            if lines is not None:
                return native_lanes(lines)
            columns = [
                ch._members(name, kinds.get(name, ""), piece.column(i).to_pylist())
                for i, name in enumerate(names)
            ]
            return ch._python_lanes(columns)

        with ThreadPoolExecutor(threads) as pool:
            for x, y in pool.map(one, ch._slices(table, names)):
                a += x
                b += y
    return ch.Fingerprint(
        schema_hash=ch.schema_hash(schema),
        data_hash=ch.data_hash(schema, table.num_rows, (a, b)),
        row_count=table.num_rows,
    )


def install():
    from ubunye.lineage import content_hash as ch

    try:
        _kernel()
    except OSError:
        return False
    ch.fingerprint_arrow = lambda table: fingerprint_arrow_native(table, ch=ch)
    return True
