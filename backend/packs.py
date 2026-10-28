"""Compact 'pack' format used by the app for fast loading (identical to the NSE engine's convert.py, format version 1).

One stock = one zlib block: dates and prices delta-coded as 32-bit integers, then volume and delivery as float64.
"""
import struct
import zlib

import numpy as np

PACK_VERSION = 1


def encode_pack_block(dates, o, h, l, c, v, dq):
    n = len(dates)
    days = np.array(dates, dtype="datetime64[D]").astype("int64")
    arrs = [np.asarray(a, dtype="float64") for a in (o, h, l, c)]
    scale = 0
    for sc in (100, 10000):
        cents = [np.rint(a * sc) for a in arrs]
        if all(np.all(np.abs(a * sc - x) < 1e-5) and np.all(np.abs(x) < 2 ** 30) for a, x in zip(arrs, cents)):
            scale = sc
            break

    def delta(a):
        d = np.empty(len(a), dtype="int64")
        d[0] = a[0]
        d[1:] = np.diff(a)
        return d

    parts = [struct.pack("<2sBBIi", b"AB", PACK_VERSION, 1 if scale else 2, n, scale),
             delta(days).astype("<i4").tobytes()]
    if scale:
        oc, hc, lc, cc = cents
        parts.append(delta(cc).astype("<i4").tobytes())
        for x in (oc, hc, lc):
            parts.append((x - cc).astype("<i4").tobytes())
    else:
        for a in (arrs[3], arrs[0], arrs[1], arrs[2]):
            parts.append(a.astype("<f8").tobytes())
    parts.append(b"\0" * ((-sum(len(x) for x in parts)) % 8))
    parts.append(np.asarray(v, dtype="<f8").tobytes())
    parts.append(np.asarray(dq, dtype="<f8").tobytes())
    return zlib.compress(b"".join(parts), 9)


def pack_group(g):
    return encode_pack_block(g["date"].tolist(), g["open"].values, g["high"].values, g["low"].values,
                             g["close"].values, g["volume"].values.astype("float64"),
                             g["delivery"].values.astype("float64"))


def decode_pack_block(blob):
    """Inverse of encode_pack_block (used by the tests). Returns a dict of numpy arrays."""
    raw = zlib.decompress(blob)
    magic, ver, mode, n, scale = struct.unpack_from("<2sBBIi", raw, 0)
    assert magic == b"AB" and ver == PACK_VERSION
    off = struct.calcsize("<2sBBIi")

    def take(dt, cnt):
        nonlocal off
        a = np.frombuffer(raw, dtype=dt, count=cnt, offset=off)
        off += a.nbytes
        return a
    days = np.cumsum(take("<i4", n).astype("int64"))
    if mode == 1:
        cc = np.cumsum(take("<i4", n).astype("int64"))
        oc, hc, lc = (take("<i4", n).astype("int64") + cc for _ in range(3))
        o, h, l, c = (x / scale for x in (oc, hc, lc, cc))
    else:
        c, o, h, l = (take("<f8", n) for _ in range(4))
    off += (-off) % 8
    v = take("<f8", n)
    dq = take("<f8", n)
    return {"dates": days.astype("datetime64[D]").astype(str), "open": o, "high": h, "low": l, "close": c, "volume": v, "delivery": dq}
