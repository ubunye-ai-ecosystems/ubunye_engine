"""Compare every observation of a workload with a reference, from the records alone."""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence

from ubunye.proving.evidence import Observation

PASS, FAIL, PARTIAL = "PASS", "FAIL", "PARTIAL"
NOT_RECORDED, NOT_RUN, UNSUPPORTED = "NOT RECORDED", "NOT RUN", "UNSUPPORTED"
VERDICTS = (PASS, FAIL, PARTIAL, NOT_RECORDED, NOT_RUN, UNSUPPORTED)
DIMENSIONS = ("execute", "identity", "inputs", "data", "schema", "rows")


def _steps(record: Dict[str, Any], side: str) -> Dict[str, Dict[str, Any]]:
    return {s.get("name"): s for s in (record.get(side) or []) if s.get("name")}


def _digest(record: Dict[str, Any]) -> Optional[str]:
    """One short hash for the table: every output's data hash, by name."""
    outs = _steps(record, "outputs")
    if not outs or any(not s.get("data_hash") for s in outs.values()):
        return None
    joined = ";".join(f"{n}={outs[n]['data_hash']}" for n in sorted(outs))
    return hashlib.sha256(joined.encode()).hexdigest()[:12]


def _compare_steps(ref: Dict[str, Any], cand: Dict[str, Any], side: str, key: str) -> str:
    """PASS when every step of the reference has the same ``key`` in the candidate."""
    ref_steps, cand_steps = _steps(ref, side), _steps(cand, side)
    if not ref_steps:
        return NOT_RECORDED
    results = []
    for name, rstep in ref_steps.items():
        cstep = cand_steps.get(name)
        rval = rstep.get(key)
        cval = None if cstep is None else cstep.get(key)
        if cstep is None:
            results.append(FAIL)  # an output the reference wrote is missing
        elif rval in (None, "", -1) or cval in (None, "", -1):
            results.append(NOT_RECORDED)
        elif key == "data_hash" and rstep.get("hash_method") != cstep.get("hash_method"):
            results.append(PARTIAL)  # hashed by different methods: not comparable
        else:
            results.append(PASS if rval == cval else FAIL)
    if FAIL in results:
        return FAIL
    if all(r == PASS for r in results):
        return PASS
    if all(r == NOT_RECORDED for r in results):
        return NOT_RECORDED
    return PARTIAL


def _dimensions(ref: Observation, obs: Observation) -> Dict[str, str]:
    if obs.status in ("not_run", "unsupported"):
        verdict = NOT_RUN if obs.status == "not_run" else UNSUPPORTED
        return {d: verdict for d in DIMENSIONS}
    if obs.status == "failed_to_launch":
        return {"execute": FAIL, **{d: NOT_RUN for d in DIMENSIONS[1:]}}
    rec = obs.record or {}
    execute = PASS if rec.get("status") == "success" else FAIL
    if execute == FAIL or ref.status != "executed":
        return {"execute": execute, **{d: NOT_RUN for d in DIMENSIONS[1:]}}
    rrec = ref.record or {}
    same_outputs = set(_steps(rrec, "outputs")) == set(_steps(rec, "outputs"))
    code_r, code_c = rrec.get("code_hash"), rec.get("code_hash")
    zone_r, zone_c = rrec.get("time_zone"), rec.get("time_zone")
    if not same_outputs or (code_r and code_c and code_r != code_c):
        identity = FAIL
    elif zone_r and zone_c and not same_zone(zone_r, zone_c):
        identity = FAIL  # the same code, set to cut time differently (ADR 007)
    elif code_r and code_c:
        identity = PASS
    else:
        identity = PARTIAL  # the outputs match but a record has no code hash
    return {
        "execute": execute,
        "identity": identity,
        "inputs": _compare_steps(rrec, rec, "inputs", "data_hash"),
        "data": _compare_steps(rrec, rec, "outputs", "data_hash"),
        "schema": _compare_steps(rrec, rec, "outputs", "schema_hash"),
        "rows": _compare_steps(rrec, rec, "outputs", "row_count"),
    }


_UTC_NAMES = frozenset(
    {
        "utc",
        "etc/utc",
        "uct",
        "etc/uct",
        "universal",
        "etc/universal",
        "zulu",
        "etc/zulu",
        "gmt",
        "etc/gmt",
        "gmt0",
        "gmt+0",
        "gmt-0",
        "etc/gmt0",
        "etc/gmt+0",
        "etc/gmt-0",
        "greenwich",
        "etc/greenwich",
        "z",
        "+00:00",
        "-00:00",
        "00:00",
    }
)


def same_zone(a: str, b: str) -> bool:
    """Whether two time zone names cut time the same way.

    Platforms spell one zone differently (Databricks says ``Etc/UTC``, Spark and the
    pandas backend ``UTC``; finding F-025). Names are equal after the UTC spellings are
    folded together, or both zones give the same UTC offset at every 15 days from 1990
    to 2040. A zone that cannot be resolved counts as different: when in doubt, not the
    same run.
    """
    fold_a, fold_b = str(a).strip().casefold(), str(b).strip().casefold()
    fold_a = "utc" if fold_a in _UTC_NAMES else fold_a
    fold_b = "utc" if fold_b in _UTC_NAMES else fold_b
    if fold_a == fold_b:
        return True
    try:
        from zoneinfo import ZoneInfo

        za = ZoneInfo("UTC" if fold_a == "utc" else str(a).strip())
        zb = ZoneInfo("UTC" if fold_b == "utc" else str(b).strip())
    except Exception:  # noqa: BLE001 (an unknown name: not provably the same)
        return False
    start = datetime(1990, 1, 1, tzinfo=timezone.utc)
    return all(
        (start + timedelta(days=15 * i)).astimezone(za).utcoffset()
        == (start + timedelta(days=15 * i)).astimezone(zb).utcoffset()
        for i in range(1218)
    )


def _setting_note(ref: Observation, obs: Observation) -> str:
    """Why an otherwise identical run is not the same run: its settings differ."""
    zr = (ref.record or {}).get("time_zone")
    zc = (obs.record or {}).get("time_zone")
    if zr and zc and not same_zone(zr, zc):
        return f"session time zone {zc}, reference {zr}: day and hour values can differ"
    return ""


def _overall(dims: Dict[str, str]) -> str:
    values = list(dims.values())
    if dims["execute"] in (NOT_RUN, UNSUPPORTED):
        return dims["execute"]
    if FAIL in values:
        return FAIL
    # Inputs are optional evidence (hashing inputs is a choice); the rest is required.
    required = [dims[d] for d in ("execute", "identity", "data", "schema", "rows")]
    if all(v == PASS for v in required) and dims["inputs"] in (PASS, NOT_RECORDED):
        return PASS
    return PARTIAL


def compare(
    observations: Iterable[Observation],
    *,
    reference: str,
    expect: Sequence[str] = (),
) -> Dict[str, Any]:
    """The machine-readable matrix for one workload.

    ``reference`` names the environment the others are compared with; it must have
    executed successfully. ``expect`` lists environments that should appear: any
    without an observation is reported NOT RUN.
    """
    obs = {o.environment: o for o in observations}
    workloads = {o.workload for o in obs.values()}
    if len(workloads) > 1:
        raise ValueError(f"observations of more than one workload: {sorted(workloads)}")
    if reference not in obs:
        raise ValueError(f"no observation for the reference environment {reference!r}")
    ref = obs[reference]
    if ref.status != "executed" or (ref.record or {}).get("status") != "success":
        raise ValueError(f"the reference {reference!r} did not run successfully")
    workload = ref.workload
    for env in expect:
        if env not in obs:
            obs[env] = Observation(
                workload=workload,
                environment=env,
                status="not_run",
                reason="expected, but no observation was collected",
            )

    rows: Dict[str, Any] = {}
    for env in sorted(obs, key=lambda e: (e != reference, e)):
        o = obs[env]
        rec = o.record or {}
        dims = _dimensions(ref, o)
        env_versions = (rec.get("environment") or {}).get("packages") or {}
        rows[env] = {
            "verdict": _overall(dims),
            "dimensions": dims,
            "status": o.status,
            "reason": o.reason or (rec.get("error") or "") or _setting_note(ref, o),
            "digest": _digest(rec) if o.status == "executed" else None,
            "outputs": {
                n: {k: s.get(k) for k in ("data_hash", "schema_hash", "row_count", "hash_method")}
                for n, s in _steps(rec, "outputs").items()
            },
            "run_id": rec.get("run_id"),
            "engine_version": rec.get("engine_version"),
            "backend": rec.get("backend"),
            "engine_packages": {
                k: v for k, v in env_versions.items() if k in ("pyspark", "pandas", "pyarrow")
            },
            "config_hash": rec.get("config_hash"),
            "duration_seconds": rec.get("duration_sec"),
            "platform": o.platform,
            "cost": o.cost,
            "provenance": o.provenance,
        }
    return {
        "ubunye_matrix": 1,
        "workload": workload,
        "reference": reference,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dimensions": list(DIMENSIONS),
        "environments": rows,
        "summary": {v: sum(1 for r in rows.values() if r["verdict"] == v) for v in VERDICTS},
    }


def _cost_text(cost: Dict[str, Any]) -> str:
    basis = (cost or {}).get("basis", "unknown")
    amount = (cost or {}).get("amount")
    if basis == "local":
        return "local"
    if amount is None:
        return "unknown"
    currency = (cost or {}).get("currency", "USD")
    label = {"actual": "billed", "provider_estimate": "provider est.", "ubunye_estimate": "est."}
    return f"{amount:g} {currency} ({label.get(basis, basis)})"


def render_markdown(matrix: Dict[str, Any]) -> str:
    """The human-readable table, generated from the matrix; never edit it by hand."""
    lines = [
        f"## Workload `{matrix['workload']}`",
        "",
        f"Reference: `{matrix['reference']}`. Generated {matrix['generated_at']} from run "
        "records; an environment without evidence is NOT RUN.",
        "",
        "| Environment | Verdict | Execute | Identity | Inputs | Data | Schema | Rows "
        "| Digest | Time | Cost | Engine |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for env, r in matrix["environments"].items():
        d = r["dimensions"]
        secs = r["duration_seconds"]
        engine = r["engine_version"] or ""
        backend = r["backend"] or ""
        lines.append(
            f"| {env} | **{r['verdict']}** | {d['execute']} | {d['identity']} | {d['inputs']} "
            f"| {d['data']} | {d['schema']} | {d['rows']} | {r['digest'] or '-'} "
            f"| {'-' if secs is None else f'{secs:.1f}s'} | {_cost_text(r['cost'])} "
            f"| {engine} {backend}".rstrip() + " |"
        )
    notes = [
        f"- **{env}**: {r['reason']}"
        for env, r in matrix["environments"].items()
        if r["verdict"] in (NOT_RUN, UNSUPPORTED, FAIL) and r["reason"]
    ]
    if notes:
        lines += ["", "Why:", "", *notes]
    lines += [
        "",
        "Digest: the first 12 hex of the SHA-256 over every output's `rows-v1` data hash. "
        "Time: the run record's task duration. Cost says where the figure came from; an "
        "estimate is never a bill.",
    ]
    return "\n".join(lines) + "\n"


def render_many(matrices: List[Dict[str, Any]]) -> str:
    return "# Proving ground\n\n" + "\n".join(render_markdown(m) for m in matrices)
