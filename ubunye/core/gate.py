"""The gate: does a run's receipt regress against a baseline?

Compares two run records (v2, see ADR 006) and says, rule by rule, whether the
candidate may pass: in CI, the baseline is the run on the base branch and the
candidate the run on the pull request.

What fails:

- the candidate run failed, or broke a ``fail`` expectation;
- an output's data or schema changed while the task's ``VERSION`` did not (a
  deliberate change bumps the version, or is allowed with ``allow_data_change``);
- an output disappeared;
- a limit set in the policy: slower by more than ``max_slowdown``, longer than
  ``max_seconds``, or a row count moved by more than ``max_row_change``.

Every changed output also says what else changed between the runs (config,
code, environment, inputs), and a change with none of them is called out as
nondeterminism. Warnings (a ``warn`` expectation broken, rows quarantined, a hash
that cannot be compared, a new output) never fail the gate.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional

from ubunye.lineage.context import RunContext, StepRecord

OK, WARN, FAIL = "ok", "warn", "fail"

#: Durations under this are noise; a slowdown rule ignores runs this short.
NOISE_FLOOR_SECONDS = 1.0


@dataclass
class Policy:
    """What the gate allows beyond "same outputs, or a version bump"."""

    allow_data_change: bool = False
    #: e.g. 0.5: fail when the candidate is more than 50% slower than the baseline.
    max_slowdown: Optional[float] = None
    #: Fail when the candidate took longer than this, in seconds.
    max_seconds: Optional[float] = None
    #: e.g. 0.1: fail when an output's row count moved by more than 10%.
    max_row_change: Optional[float] = None


@dataclass
class Finding:
    """One line of the gate's verdict."""

    rule: str
    status: str
    detail: str
    output: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _what_else_changed(base: RunContext, cand: RunContext) -> List[str]:
    moved = []
    if base.config_hash != cand.config_hash:
        moved.append("config")
    if base.code_hash and cand.code_hash and base.code_hash != cand.code_hash:
        moved.append("code")
    if (
        base.environment_hash
        and cand.environment_hash
        and (base.environment_hash != cand.environment_hash)
    ):
        moved.append("environment")
    b_in = {s.name: s.data_hash for s in base.inputs}
    c_in = {s.name: s.data_hash for s in cand.inputs}
    changed_inputs = sorted(
        n for n in set(b_in) | set(c_in) if b_in.get(n) and c_in.get(n) and b_in[n] != c_in[n]
    )
    if changed_inputs:
        moved.append("input " + ", ".join(changed_inputs))
    return moved


def _comparable(a: StepRecord, b: StepRecord) -> bool:
    return bool(a.data_hash and b.data_hash and a.hash_method and a.hash_method == b.hash_method)


def evaluate(base: RunContext, cand: RunContext, policy: Optional[Policy] = None) -> List[Finding]:
    """The gate's findings for ``cand`` against ``base``, most important first."""
    policy = policy or Policy()
    found: List[Finding] = []
    bumped = bool(base.version and cand.version and base.version != cand.version)

    # --- the run itself ---------------------------------------------------------------
    if cand.status != "success":
        found.append(Finding("run", FAIL, f"the run ended '{cand.status}': {cand.error or '-'}"))
    else:
        found.append(Finding("run", OK, "succeeded"))

    for e in cand.expectations:
        if e.get("passed"):
            continue
        where = f"{e['rule']} broken by {e['failed']} of {e['total']} rows"
        severity = e.get("severity")
        if severity == "fail":
            found.append(Finding("expectation", FAIL, where, e.get("output")))
        elif severity == "quarantine":
            found.append(Finding("expectation", WARN, where + " (quarantined)", e.get("output")))
        else:
            found.append(Finding("expectation", WARN, where, e.get("output")))

    # --- the outputs ------------------------------------------------------------------
    moved = _what_else_changed(base, cand)
    because = (
        ("; changed: " + ", ".join(moved))
        if moved
        else (
            "; nothing else changed (same config, code, environment and inputs), so the "
            "transform is not deterministic"
        )
    )
    b_out = {s.name: s for s in base.outputs}
    c_out = {s.name: s for s in cand.outputs}
    for name in sorted(set(b_out) | set(c_out)):
        b, c = b_out.get(name), c_out.get(name)
        if c is None:
            found.append(Finding("output", FAIL, "missing from the candidate run", name))
            continue
        if b is None:
            found.append(Finding("output", WARN, "new output, nothing to compare", name))
            continue
        if not _comparable(b, c):
            why = c.hash_error or b.hash_error or "hashes missing or made differently"
            found.append(Finding("data", WARN, f"cannot compare ({why})", name))
        elif b.data_hash == c.data_hash:
            found.append(Finding("data", OK, "identical", name))
        elif bumped:
            found.append(
                Finding("data", OK, f"changed with VERSION {base.version} -> {cand.version}", name)
            )
        elif policy.allow_data_change:
            found.append(Finding("data", WARN, "changed (allowed)" + because, name))
        else:
            found.append(
                Finding(
                    "data",
                    FAIL,
                    f"changed without a VERSION bump (still {cand.version or '-'})" + because,
                    name,
                )
            )
        if b.schema_hash and c.schema_hash and b.schema_hash != c.schema_hash:
            status = OK if bumped else (WARN if policy.allow_data_change else FAIL)
            found.append(Finding("schema", status, "columns or types changed" + because, name))
        if (
            policy.max_row_change is not None
            and b.row_count is not None
            and c.row_count is not None
            and b.row_count > 0
        ):
            change = abs(c.row_count - b.row_count) / b.row_count
            if change > policy.max_row_change:
                found.append(
                    Finding(
                        "rows",
                        FAIL,
                        f"{b.row_count} -> {c.row_count} rows ({change:.1%}), more than "
                        f"{policy.max_row_change:.1%}",
                        name,
                    )
                )

    # --- time ---------------------------------------------------------------------------
    b_s, c_s = base.duration_sec, cand.duration_sec
    if policy.max_seconds is not None and c_s is not None and c_s > policy.max_seconds:
        found.append(Finding("time", FAIL, f"{c_s:.1f}s, more than {policy.max_seconds:.1f}s"))
    if (
        policy.max_slowdown is not None
        and b_s
        and c_s is not None
        and c_s > NOISE_FLOOR_SECONDS
        and c_s > b_s * (1 + policy.max_slowdown)
    ):
        found.append(
            Finding(
                "time",
                FAIL,
                f"{b_s:.1f}s -> {c_s:.1f}s ({c_s / b_s - 1:.0%} slower), more than "
                f"{policy.max_slowdown:.0%}",
            )
        )
    return found


def passed(findings: List[Finding]) -> bool:
    return not any(f.status == FAIL for f in findings)


def markdown(findings: List[Finding], base: RunContext, cand: RunContext) -> str:
    """A table for a pull request or a CI job summary."""
    verdict = "passes" if passed(findings) else "fails"
    lines = [
        f"### Ubunye gate {verdict}: `{cand.task_path}`",
        "",
        f"Baseline `{base.run_id[:8]}` (VERSION {base.version or '-'}) against "
        f"candidate `{cand.run_id[:8]}` (VERSION {cand.version or '-'}).",
        "",
        "| | rule | output | detail |",
        "|---|---|---|---|",
    ]
    mark = {OK: "✅", WARN: "⚠️", FAIL: "❌"}
    for f in findings:
        lines.append(f"| {mark[f.status]} | {f.rule} | {f.output or ''} | {f.detail} |")
    return "\n".join(lines) + "\n"
