"""``CONFIG.expectations``: checks on outputs, run before anything is written.

The rules are declared in the config, not coded in the transform, so they can be
read without reading any Spark, and they run the same way on every backend: they
are evaluated with Narwhals on whatever frame the transform returned (Spark or
pandas). Three principles, from the data contract example they replace:

- **Quarantine, do not drop.** A row breaking a ``quarantine`` rule goes to the
  named quarantine output with the rules it broke attached, so somebody can fix
  the source. A dropped row is silent data loss.
- **Fail before writing.** Every output is checked before any is written, so a
  ``fail`` rule leaves no half-written run behind. More than
  ``max_quarantine_rate`` of the rows quarantined is also a failure: that is a
  source that changed, not a few bad rows.
- **Report every rule, passed or not.** The results list carries a zero for a
  rule nobody broke today, which is how you notice it breaking tomorrow.

A null passes every rule except ``not_null``, as in SQL.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from functools import reduce
from typing import Any, Dict, List, Optional, Tuple

from ubunye.config.schema import ExpectationRule, ExpectationSet
from ubunye.core.errors import ExpectationError, TransformOutputError

log = logging.getLogger(__name__)

#: The column added to quarantined rows: the names of the rules each row broke.
FAILED_RULES_COLUMN = "_ubunye_failed_rules"


@dataclass
class RuleResult:
    """What one rule found on one output."""

    output: str
    rule: str
    kind: str
    severity: str
    column: Optional[str]
    failed: int  # rows breaking it (duplicate rows for unique; 1 or 0 for row_count)
    total: int  # rows checked
    passed: bool

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _nw() -> Any:
    try:
        import narwhals as nw
    except ImportError as exc:  # pragma: no cover - narwhals is a dependency
        raise ExpectationError(
            "Expectations need the 'narwhals' package, which is not installed.",
            hint="pip install narwhals",
        ) from exc
    return nw


def _breaks(nw: Any, rule: ExpectationRule) -> Any:
    """A boolean expression, True where the row breaks a row-level rule."""
    if rule.not_null is not None:
        return nw.col(rule.not_null).is_null()
    if rule.between is not None:
        col = nw.col(rule.between.column)
        parts = []
        if rule.between.min is not None:
            parts.append(col < rule.between.min)
        if rule.between.max is not None:
            parts.append(col > rule.between.max)
        return reduce(lambda a, b: a | b, parts).fill_null(False)
    if rule.one_of is not None:
        col = nw.col(rule.one_of.column)
        return (~col.is_in(rule.one_of.values)) & ~col.is_null()
    if rule.matches is not None:
        col = nw.col(rule.matches.column)
        # A null passes, and engines disagree on what contains() gives for one:
        # pandas 3 says False, a pandas 2.2 object column says None (and there ~None
        # raises, while ~True is -2, which sums to a negative count). So fill, make it
        # a real boolean before negating, and rule the nulls out explicitly.
        matched = col.str.contains(rule.matches.pattern).fill_null(True).cast(nw.Boolean)
        return ~matched & ~col.is_null()
    raise ValueError(f"'{rule.kind}' is not a row-level rule")  # pragma: no cover


def _collect(frame: Any) -> Any:
    return frame.collect() if hasattr(frame, "collect") else frame


def _scalars(frame: Any, exprs: List[Any]) -> Dict[str, Any]:
    """One pass over the data for every count, as a {name: value} dict."""
    row = _collect(frame.select(*exprs)).rows(named=True)[0]
    return {k: (0 if v is None else v) for k, v in row.items()}


def check_output(
    name: str, frame: Any, spec: ExpectationSet
) -> Tuple[Any, Optional[Any], List[RuleResult]]:
    """Check one output: (clean frame, quarantined frame or None, results)."""
    nw = _nw()
    df = nw.from_native(frame)

    row_rules = [r for r in spec.rules if r.kind in ("not_null", "between", "one_of", "matches")]
    counts = _scalars(
        df,
        [nw.len().alias("__total")]
        + [_breaks(nw, r).cast(nw.Int64).sum().alias(f"r{i}") for i, r in enumerate(row_rules)],
    )
    total = int(counts["__total"])
    failed: Dict[str, int] = {r.name: int(counts[f"r{i}"]) for i, r in enumerate(row_rules)}

    for rule in spec.rules:
        if rule.unique is not None:
            dupes = (
                df.group_by(rule.unique)
                .agg(nw.len().alias("__n"))
                .filter(nw.col("__n") > 1)
                .select(nw.col("__n").sum().alias("__dupes"))
            )
            failed[rule.name] = int(_scalars(dupes, [nw.col("__dupes")])["__dupes"])
        elif rule.row_count is not None:
            low, high = rule.row_count.min, rule.row_count.max
            outside = (low is not None and total < low) or (high is not None and total > high)
            failed[rule.name] = 1 if outside else 0

    results = [
        RuleResult(
            output=name,
            rule=r.name,
            kind=r.kind,
            severity=r.severity,
            column=r.column,
            failed=failed[r.name],
            total=total,
            passed=failed[r.name] == 0,
        )
        for r in spec.rules
    ]

    quarantine_rules = [r for r in row_rules if r.severity == "quarantine"]
    if not quarantine_rules or not any(failed[r.name] for r in quarantine_rules):
        return frame, None, results

    breaks_any = reduce(lambda a, b: a | b, [_breaks(nw, r) for r in quarantine_rules])
    clean = df.filter(~breaks_any)
    reasons = nw.concat_str(
        [nw.when(_breaks(nw, r)).then(nw.lit(r.name)) for r in quarantine_rules],
        separator=",",
        ignore_nulls=True,
    )
    quarantined = df.filter(breaks_any).with_columns(reasons.alias(FAILED_RULES_COLUMN))
    return nw.to_native(clean), nw.to_native(quarantined), results


def apply(
    outputs: Dict[str, Any], expectations: Dict[str, ExpectationSet]
) -> Tuple[Dict[str, Any], List[RuleResult]]:
    """Check every output that has expectations; nothing is written here.

    Returns the outputs to write (clean frames, plus quarantined rows under their
    quarantine output) and every rule's result. Raises :class:`ExpectationError`
    if any ``fail`` rule is broken or a quarantine rate is exceeded.
    """
    if not expectations:
        return outputs, []
    outputs = dict(outputs)
    results: List[RuleResult] = []
    problems: List[str] = []

    for name in sorted(expectations):
        spec = expectations[name]
        if name not in outputs:
            continue  # the writer reports a missing output with its own message
        if spec.quarantine and spec.quarantine in outputs:
            raise TransformOutputError(
                f"The transform returned '{spec.quarantine}', which is the quarantine "
                f"output for '{name}'; the engine fills it.",
                context={"Output": name, "Quarantine": spec.quarantine},
                hint=f"Stop returning '{spec.quarantine}' from transform().",
            )
        clean, quarantined, found = check_output(name, outputs[name], spec)
        results += found
        outputs[name] = clean
        if spec.quarantine:
            if quarantined is not None:
                outputs[spec.quarantine] = quarantined
            else:
                outputs[spec.quarantine] = _empty_quarantine(clean)

        for r in found:
            if r.passed:
                continue
            line = f"{name}: {r.rule} ({r.kind}) broken by {r.failed} of {r.total} rows"
            if r.severity == "fail":
                problems.append(line)
            elif r.severity == "warn":
                log.warning("expectation warning: %s", line)
            else:
                log.info("quarantined: %s", line)

        total = found[0].total if found else 0
        if spec.max_quarantine_rate is not None and quarantined is not None and total:
            n = _row_count(quarantined)
            if n / total > spec.max_quarantine_rate:
                problems.append(
                    f"{name}: {n} of {total} rows quarantined ({n / total:.1%}), more than "
                    f"max_quarantine_rate {spec.max_quarantine_rate:.1%}"
                )

    if problems:
        raise ExpectationError(
            "Expectations failed, so nothing was written:\n  " + "\n  ".join(problems),
            results=[r.as_dict() for r in results],
            hint="Fix the data or the source, or change the rule's severity to "
            "quarantine or warn if this is expected.",
        )
    return outputs, results


def _row_count(frame: Any) -> int:
    nw = _nw()
    return int(_scalars(nw.from_native(frame), [nw.len().alias("n")])["n"])


def _empty_quarantine(frame: Any) -> Any:
    """No row was quarantined: an empty frame of the same shape, so the output exists."""
    nw = _nw()
    df = nw.from_native(frame)
    return nw.to_native(df.head(0).with_columns(nw.lit("").alias(FAILED_RULES_COLUMN)))


def summary(results: List[RuleResult]) -> str:
    """A short human line per rule, for logs and the CLI."""
    width = max((len(r.rule) for r in results), default=0)
    return "\n".join(
        f"{'ok  ' if r.passed else r.severity:10} {r.output}.{r.rule:<{width}}  "
        f"{r.failed}/{r.total}"
        for r in results
    )
