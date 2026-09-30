"""E-10: install size, licence and platform wheels of each candidate package.

Size: the installed files of the distribution (and what it pulls in that the
engine does not already need), from this venv's metadata. Wheels: the current
release's files on PyPI, by platform and CPython version.
"""

from __future__ import annotations

import json
import os
import urllib.request
from importlib import metadata

PKGS = {
    "duckdb": ["duckdb"],
    "polars-hash": ["polars-hash", "polars", "polars-runtime-32"],
    "datafusion": ["datafusion"],
    "numba": ["numba", "llvmlite"],
}


def installed_mb(dist):
    d = metadata.distribution(dist)
    total = 0
    for f in d.files or []:
        p = d.locate_file(f)
        try:
            total += os.path.getsize(p)
        except OSError:
            pass
    return total / 2**20


def licence(dist):
    m = metadata.metadata(dist)
    return (
        m.get("License-Expression")
        or m.get("License")
        or ";".join(
            c.split("::")[-1].strip() for c in m.get_all("Classifier") or [] if "License" in c
        )
    )


def wheels(dist, version):
    url = f"https://pypi.org/pypi/{dist}/{version}/json"
    with urllib.request.urlopen(url, timeout=30) as r:
        data = json.load(r)
    tags = [u["filename"] for u in data["urls"] if u["filename"].endswith(".whl")]
    plats = {
        "win_amd64": False,
        "win_arm64": False,
        "manylinux x86_64": False,
        "manylinux aarch64": False,
        "musllinux": False,
        "macosx x86_64": False,
        "macosx arm64": False,
    }
    pys = set()
    for t in tags:
        py = t.split("-")[-3]
        pys.add(py)
        if "win_amd64" in t:
            plats["win_amd64"] = True
        if "win_arm64" in t:
            plats["win_arm64"] = True
        if "manylinux" in t and "x86_64" in t:
            plats["manylinux x86_64"] = True
        if "manylinux" in t and "aarch64" in t:
            plats["manylinux aarch64"] = True
        if "musllinux" in t:
            plats["musllinux"] = True
        if "macosx" in t and ("x86_64" in t or "universal2" in t):
            plats["macosx x86_64"] = True
        if "macosx" in t and ("arm64" in t or "universal2" in t):
            plats["macosx arm64"] = True
    return {
        "wheels": len(tags),
        "python_tags": sorted(pys),
        "platforms": plats,
        "requires_python": data["info"].get("requires_python"),
        "largest_wheel_mb": round(
            max([u["size"] for u in data["urls"] if u["filename"].endswith(".whl")] or [0]) / 2**20,
            1,
        ),
    }


out = {}
for key, dists in PKGS.items():
    row = {"dists": {}}
    for dist in dists:
        v = metadata.version(dist)
        row["dists"][dist] = {
            "version": v,
            "installed_mb": round(installed_mb(dist), 1),
            "licence": licence(dist),
        }
        try:
            row["dists"][dist]["pypi"] = wheels(dist, v)
        except Exception as exc:  # noqa: BLE001
            row["dists"][dist]["pypi"] = f"{type(exc).__name__}: {exc}"
    row["total_installed_mb"] = round(sum(d["installed_mb"] for d in row["dists"].values()), 1)
    out[key] = row
dll = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rows_v1_lanes.dll")
out["c (this prototype)"] = {"total_installed_mb": round(os.path.getsize(dll) / 2**20, 3)}
print(json.dumps(out, indent=1))
