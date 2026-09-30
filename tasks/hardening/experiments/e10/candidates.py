"""E-10: candidate lane functions for rows-v1, each a drop-in for content_hash._arrow_lanes.

Every candidate takes an Arrow string array of canonical lines (built by the
engine's own _slice_lines, unchanged) and returns the two lane sums: the first
and second 8 bytes of each line's SHA-256, big endian, summed modulo 2**64.
Only the per-row SHA-256, the lane extraction and the sums are replaced.
"""

from __future__ import annotations

import ctypes
import importlib.util
import os
import sys

import numpy as np
import pyarrow as pa

HERE = os.path.dirname(os.path.abspath(__file__))
MASK = (1 << 64) - 1


def load_content_hash(path=None):
    """The engine's content_hash.py loaded by path (as a helper loads it)."""
    path = path or os.environ.get("E10_CONTENT_HASH")
    if path is None:
        root = os.environ["E10_WORKTREE"]
        path = os.path.join(root, "ubunye", "lineage", "content_hash.py")
    name = "_e10_ch_" + str(abs(hash(path)))
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def _offsets_data(lines):
    n = len(lines)
    bufs = lines.buffers()
    offsets = np.frombuffer(bufs[1], dtype=np.int32, count=n + 1, offset=lines.offset * 4)
    data = bufs[2] if bufs[2] is not None else pa.py_buffer(b"\0")
    return n, offsets, data


# ----------------------------------------------------------------------------- C (ctypes)
_dll = None


def c_dll(path=None):
    global _dll
    if _dll is None:
        path = path or os.environ.get("E10_DLL") or os.path.join(HERE, "rows_v1_lanes.dll")
        lib = ctypes.CDLL(path)
        lib.rows_v1_lanes.argtypes = [
            ctypes.c_void_p,
            ctypes.c_int64,
            ctypes.c_void_p,
            ctypes.c_void_p,
        ]
        lib.rows_v1_lanes.restype = None
        lib.rows_v1_sha256.argtypes = [ctypes.c_char_p, ctypes.c_int64, ctypes.c_char_p]
        lib.rows_v1_set_mode.argtypes = [ctypes.c_int]
        lib.rows_v1_set_mode.restype = ctypes.c_int
        lib.rows_v1_has_shani.restype = ctypes.c_int
        _dll = lib
    return _dll


def lanes_c(lines):
    lib = c_dll()
    n, offsets, data = _offsets_data(lines)
    out = (ctypes.c_uint64 * 2)(0, 0)
    lib.rows_v1_lanes(offsets.ctypes.data, n, data.address, ctypes.addressof(out))
    return int(out[0]), int(out[1])


# ----------------------------------------------------------------------------- DuckDB
_duck = {}


def duck_con(threads=1):
    import duckdb

    con = _duck.get(threads)
    if con is None:
        con = duckdb.connect()
        con.execute(f"SET threads={int(threads)}")
        _duck[threads] = con
    return con


_DUCK_SQL = (
    "select count(*), "
    "sum(('0x' || substr(h, 1, 8))::UBIGINT), sum(('0x' || substr(h, 9, 8))::UBIGINT), "
    "sum(('0x' || substr(h, 17, 8))::UBIGINT), sum(('0x' || substr(h, 25, 8))::UBIGINT) "
    "from (select sha256(l) as h from e10_lines)"
)


def _join32(hi, lo):
    return ((int(hi or 0) << 32) + int(lo or 0)) & MASK


def lanes_duckdb(lines, threads=1):
    con = duck_con(threads)
    e10_lines = pa.table({"l": lines})  # noqa: F841  (DuckDB's replacement scan)
    con.register("e10_lines", e10_lines)
    try:
        n, a_hi, a_lo, b_hi, b_lo = con.execute(_DUCK_SQL).fetchone()
    finally:
        con.unregister("e10_lines")
    assert n == len(lines)
    return _join32(a_hi, a_lo), _join32(b_hi, b_lo)


# ----------------------------------------------------------------------------- polars-hash
def lanes_polars_hash(lines):
    import polars as pl
    import polars_hash  # noqa: F401  (registers .chash)

    s = pl.from_arrow(lines)
    df = pl.DataFrame({"l": s}).select(pl.col("l").chash.sha2_256().alias("h"))
    h = pl.col("h")
    out = df.select(
        [
            h.str.slice(i, 8).str.to_integer(base=16).cast(pl.Int64).sum().alias(f"s{i}")
            for i in (0, 8, 16, 24)
        ]
    ).row(0)
    return _join32(out[0], out[1]), _join32(out[2], out[3])


# ----------------------------------------------------------------------------- DataFusion
_df_ctx = None


def lanes_datafusion(lines):
    global _df_ctx
    import datafusion
    from datafusion import col
    from datafusion import functions as f

    if _df_ctx is None:
        _df_ctx = datafusion.SessionContext()
    frame = _df_ctx.from_arrow(pa.table({"l": lines}))
    got = frame.select(f.sha256(col("l")).alias("h")).to_arrow_table().column("h")
    a = b = 0
    for chunk in got.chunks:
        n = len(chunk)
        if n == 0:
            continue
        bufs = chunk.buffers()
        if pa.types.is_fixed_size_binary(chunk.type):
            words = np.frombuffer(
                bufs[1], dtype=">u8", count=4 * n, offset=chunk.offset * 32
            ).reshape(n, 4)
        else:
            width = 8 if pa.types.is_large_binary(chunk.type) else 4
            dt_ = np.int64 if width == 8 else np.int32
            off = np.frombuffer(bufs[1], dtype=dt_, count=n + 1, offset=chunk.offset * width)
            assert (np.diff(off) == 32).all()
            words = np.frombuffer(bufs[2], dtype=">u8", count=4 * n, offset=int(off[0])).reshape(
                n, 4
            )
        a += int(words[:, 0].astype(np.uint64).sum(dtype=np.uint64))
        b += int(words[:, 1].astype(np.uint64).sum(dtype=np.uint64))
    return a & MASK, b & MASK


# ----------------------------------------------------------------------------- numba
_nb = {}


def _numba_kernels():
    if _nb:
        return _nb
    import numba
    from numba import njit, prange, uint32, uint64

    K = np.array(
        [
            0x428A2F98,
            0x71374491,
            0xB5C0FBCF,
            0xE9B5DBA5,
            0x3956C25B,
            0x59F111F1,
            0x923F82A4,
            0xAB1C5ED5,
            0xD807AA98,
            0x12835B01,
            0x243185BE,
            0x550C7DC3,
            0x72BE5D74,
            0x80DEB1FE,
            0x9BDC06A7,
            0xC19BF174,
            0xE49B69C1,
            0xEFBE4786,
            0x0FC19DC6,
            0x240CA1CC,
            0x2DE92C6F,
            0x4A7484AA,
            0x5CB0A9DC,
            0x76F988DA,
            0x983E5152,
            0xA831C66D,
            0xB00327C8,
            0xBF597FC7,
            0xC6E00BF3,
            0xD5A79147,
            0x06CA6351,
            0x14292967,
            0x27B70A85,
            0x2E1B2138,
            0x4D2C6DFC,
            0x53380D13,
            0x650A7354,
            0x766A0ABB,
            0x81C2C92E,
            0x92722C85,
            0xA2BFE8A1,
            0xA81A664B,
            0xC24B8B70,
            0xC76C51A3,
            0xD192E819,
            0xD6990624,
            0xF40E3585,
            0x106AA070,
            0x19A4C116,
            0x1E376C08,
            0x2748774C,
            0x34B0BCB5,
            0x391C0CB3,
            0x4ED8AA4A,
            0x5B9CCA4F,
            0x682E6FF3,
            0x748F82EE,
            0x78A5636F,
            0x84C87814,
            0x8CC70208,
            0x90BEFFFA,
            0xA4506CEB,
            0xBEF9A3F7,
            0xC67178F2,
        ],
        dtype=np.uint32,
    )
    H = np.array(
        [
            0x6A09E667,
            0xBB67AE85,
            0x3C6EF372,
            0xA54FF53A,
            0x510E527F,
            0x9B05688C,
            0x1F83D9AB,
            0x5BE0CD19,
        ],
        dtype=np.uint32,
    )

    @njit(inline="always")
    def ror(x, n):
        return uint32((x >> uint32(n)) | (x << uint32(32 - n)))

    @njit
    def compress(st, blk, w):
        for t in range(16):
            w[t] = (
                (uint32(blk[4 * t]) << uint32(24))
                | (uint32(blk[4 * t + 1]) << uint32(16))
                | (uint32(blk[4 * t + 2]) << uint32(8))
                | uint32(blk[4 * t + 3])
            )
        for t in range(16, 64):
            x = w[t - 15]
            y = w[t - 2]
            s0 = ror(x, 7) ^ ror(x, 18) ^ (x >> uint32(3))
            s1 = ror(y, 17) ^ ror(y, 19) ^ (y >> uint32(10))
            w[t] = uint32(w[t - 16] + s0 + w[t - 7] + s1)
        a, b, c, d, e, f, g, h = st[0], st[1], st[2], st[3], st[4], st[5], st[6], st[7]
        for t in range(64):
            S1 = ror(e, 6) ^ ror(e, 11) ^ ror(e, 25)
            ch = (e & f) ^ (~e & g)
            t1 = uint32(h + S1 + ch + K[t] + w[t])
            S0 = ror(a, 2) ^ ror(a, 13) ^ ror(a, 22)
            mj = (a & b) ^ (a & c) ^ (b & c)
            t2 = uint32(S0 + mj)
            h = g
            g = f
            f = e
            e = uint32(d + t1)
            d = c
            c = b
            b = a
            a = uint32(t1 + t2)
        st[0] += a
        st[1] += b
        st[2] += c
        st[3] += d
        st[4] += e
        st[5] += f
        st[6] += g
        st[7] += h

    @njit
    def lanes_range(off, data, lo, hi):
        st = np.empty(8, np.uint32)
        w = np.empty(64, np.uint32)
        buf = np.empty(128, np.uint8)
        sa = uint64(0)
        sb = uint64(0)
        for i in range(lo, hi):
            p = off[i]
            ln = off[i + 1] - p
            for j in range(8):
                st[j] = H[j]
            full = ln - (ln % 64)
            k = 0
            while k < full:
                compress(st, data[p + k : p + k + 64], w)
                k += 64
            rem = ln - full
            for j in range(rem):
                buf[j] = data[p + full + j]
            buf[rem] = 0x80
            tot = 64 if rem + 9 <= 64 else 128
            for j in range(rem + 1, tot - 8):
                buf[j] = 0
            bits = uint64(ln) * uint64(8)
            for j in range(8):
                buf[tot - 1 - j] = uint64((bits >> uint64(8 * j)) & uint64(0xFF))
            compress(st, buf[0:64], w)
            if tot == 128:
                compress(st, buf[64:128], w)
            sa += (uint64(st[0]) << uint64(32)) | uint64(st[1])
            sb += (uint64(st[2]) << uint64(32)) | uint64(st[3])
        return sa, sb

    @njit(nogil=True)
    def lanes_serial(off, data):
        return lanes_range(off, data, 0, len(off) - 1)

    @njit(parallel=True, nogil=True)
    def lanes_parallel(off, data, parts):
        n = len(off) - 1
        outa = np.zeros(parts, np.uint64)
        outb = np.zeros(parts, np.uint64)
        step = (n + parts - 1) // parts
        for q in prange(parts):
            lo = q * step
            hi = min(n, lo + step)
            if lo < hi:
                a, b = lanes_range(off, data, lo, hi)
                outa[q] = a
                outb[q] = b
        return outa.sum(), outb.sum()

    _nb["serial"] = lanes_serial
    _nb["parallel"] = lanes_parallel
    _nb["numba"] = numba
    return _nb


def lanes_numba(lines):
    k = _numba_kernels()
    n, offsets, data = _offsets_data(lines)
    arr = np.frombuffer(data, dtype=np.uint8)
    a, b = k["serial"](offsets, arr)
    return int(a), int(b)


def lanes_numba_parallel(lines, parts=4):
    k = _numba_kernels()
    n, offsets, data = _offsets_data(lines)
    arr = np.frombuffer(data, dtype=np.uint8)
    a, b = k["parallel"](offsets, arr, parts)
    return int(a), int(b)


CANDIDATES = {
    "c": lanes_c,
    "duckdb": lanes_duckdb,
    "polars_hash": lanes_polars_hash,
    "datafusion": lanes_datafusion,
    "numba": lanes_numba,
}
