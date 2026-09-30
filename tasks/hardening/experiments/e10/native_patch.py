"""E-10: a copy of the engine's content_hash.py whose _arrow_lanes is the native kernel.

Nothing in the engine changes. The copy is the engine file, byte for byte, with
one block appended that rebinds _arrow_lanes to the C kernel (loaded with
ctypes from E10_DLL). Helper processes load the copy by its path, so they hash
with the kernel too: this is how "native + helpers" is measured. The copy's own
SHA-256 travels to the helpers as before, so the source check still holds.
"""

from __future__ import annotations

import sys

BLOCK = """

# --------------------------------------------------------------------------- #
# E-10 prototype (appended): the per-row SHA-256 and lane sums in native code.
# --------------------------------------------------------------------------- #
def _e10_native_lanes() -> Callable[..., Any]:
    import ctypes

    import numpy as np

    lib = ctypes.CDLL(os.environ["E10_DLL"])
    lib.rows_v1_lanes.argtypes = [ctypes.c_void_p, ctypes.c_int64, ctypes.c_void_p,
                                  ctypes.c_void_p]
    lib.rows_v1_lanes.restype = None

    def lanes_native(lines: Any) -> Tuple[int, int]:
        n = len(lines)
        buffers = lines.buffers()
        offsets = np.frombuffer(buffers[1], dtype=np.int32, count=n + 1, offset=lines.offset * 4)
        if buffers[2] is None:
            import pyarrow as pa

            data = pa.py_buffer(bytes(1))
        else:
            data = buffers[2]
        out = (ctypes.c_uint64 * 2)(0, 0)
        lib.rows_v1_lanes(offsets.ctypes.data, n, data.address, ctypes.addressof(out))
        return int(out[0]), int(out[1])

    return lanes_native


_arrow_lanes = _e10_native_lanes()
"""


def make(src: str, dst: str) -> str:
    with open(src, "r", encoding="utf-8", newline="") as fh:
        text = fh.read()
    with open(dst, "w", encoding="utf-8", newline="") as fh:
        fh.write(text + BLOCK)
    return dst


if __name__ == "__main__":
    print(make(sys.argv[1], sys.argv[2]))
