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

A null passes every rule except ``not_null``, as in SQL. In a float column NaN counts
as missing too, on every backend (F-045): pandas stores a missing float as NaN, so it
cannot tell the two apart, and a rule must not pass on one backend and fail on the
other. So ``not_null`` breaks on NaN, and ``between`` and ``one_of`` let it pass.
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


def _missing(nw: Any, column: str, floats: frozenset) -> Any:
    """True where a value is missing: null, or NaN in a float column (F-045)."""
    col = nw.col(column)
    return (col.is_null() | col.is_nan()) if column in floats else col.is_null()


def _breaks(nw: Any, rule: ExpectationRule, floats: frozenset = frozenset()) -> Any:
    """A boolean expression, True where the row breaks a row-level rule.

    ``floats`` names the float columns, where NaN counts as missing like null.
    """
    if rule.not_null is not None:
        return _missing(nw, rule.not_null, floats)
    if rule.between is not None:
        col = nw.col(rule.between.column)
        parts = []
        if rule.between.min is not None:
            parts.append(col < rule.between.min)
        if rule.between.max is not None:
            parts.append(col > rule.between.max)
        # Spark orders NaN above every number, so NaN > max; rule it out as missing.
        outside = reduce(lambda a, b: a | b, parts).fill_null(False)
        if rule.between.column in floats:
            outside = outside & ~_missing(nw, rule.between.column, floats)
        return outside
    if rule.one_of is not None:
        col = nw.col(rule.one_of.column)
        return (~col.is_in(rule.one_of.values)) & ~_missing(nw, rule.one_of.column, floats)
    if rule.matches is not None:
        col = nw.col(rule.matches.column)
        # A null passes, and engines disagree on what contains() gives for one:
        # pandas 3 says False, a pandas 2.2 object column says None (and there ~None
        # raises, while ~True is -2, which sums to a negative count). So fill, make it
        # a real boolean before negating, and rule the nulls out explicitly.
        matched = col.str.contains(rule.matches.pattern).fill_null(True).cast(nw.Boolean)
        return ~matched & ~col.is_null()
    raise ValueError(f"'{rule.kind}' is not a row-level rule")  # pragma: no cover


def _columns_of(rule: ExpectationRule) -> List[str]:
    spec = getattr(rule, rule.kind)
    if isinstance(spec, str):
        return [spec]
    if isinstance(spec, list):
        return list(spec)
    column = getattr(spec, "column", None)
    return [column] if column else []


# A numeric column read as text is most often a CSV parse problem, so say so.
_TEXT_NUMBER_HINT = (
    "If '{column}' should be a number and comes from a CSV file: a value somewhere "
    "did not parse, so the whole column stayed text. For a CSV written by pandas, "
    "Excel or most databases (quotes doubled inside quoted text), set "
    "options.escape: '\"' on the input; Spark's default escape is a backslash. "
    "Or cast the column in transformations.py."
)


def _check_columns(nw: Any, name: str, df: Any, spec: ExpectationSet) -> None:
    """Every rule names a column the output has, of a type the rule can check.

    Checked before any counting, so a wrong column gives one clear error, not an
    engine traceback from deep inside the comparison.
    """
    schema = df.collect_schema() if hasattr(df, "collect_schema") else df.schema
    available = list(schema.names())
    for rule in spec.rules:
        for column in _columns_of(rule):
            if column not in available:
                raise ExpectationError(
                    f"{name}: rule '{rule.name}' ({rule.kind}) names column '{column}', "
                    "which the output does not have.",
                    context={"Output": name, "Rule": rule.name, "Columns": ", ".join(available)},
                    hint="Check the column name in CONFIG.expectations, or the transform's output.",
                )
            dtype = schema[column]
            if rule.kind == "between" and not dtype.is_numeric():
                raise ExpectationError(
                    f"{name}: rule '{rule.name}' (between) needs a numeric column, but "
                    f"'{column}' is {dtype}.",
                    context={"Output": name, "Rule": rule.name, "Column": column},
                    hint=_TEXT_NUMBER_HINT.format(column=column),
                )
            if rule.kind == "matches" and dtype not in (nw.String, nw.Categorical):
                raise ExpectationError(
                    f"{name}: rule '{rule.name}' (matches) needs a text column, but "
                    f"'{column}' is {dtype}.",
                    context={"Output": name, "Rule": rule.name, "Column": column},
                    hint="Cast the column to text in transformations.py, or drop the rule.",
                )


def _collect(frame: Any) -> Any:
    return frame.collect() if hasattr(frame, "collect") else frame


def _scalars(frame: Any, exprs: List[Any]) -> Dict[str, Any]:
    """One pass over the data for every count, as a {name: value} dict."""
    selected = frame.select(*exprs)
    native = _nw().to_native(selected)
    if hasattr(native, "sparkSession") and hasattr(native, "collect"):
        # A Spark frame gives its one row itself. Narwhals would collect it through
        # Arrow, and a Spark image need not have pyarrow (F-036).
        row = native.collect()[0].asDict()
    else:
        row = _collect(selected).rows(named=True)[0]
    return {k: (0 if v is None else v) for k, v in row.items()}


def check_output(
    name: str, frame: Any, spec: ExpectationSet
) -> Tuple[Any, Optional[Any], List[RuleResult]]:
    """Check one output: (clean frame, quarantined frame or None, results)."""
    nw = _nw()
    df = nw.from_native(frame)
    _check_columns(nw, name, df, spec)

    row_rules = [r for r in spec.rules if r.kind in ("not_null", "between", "one_of", "matches")]
    schema = df.collect_schema()
    floats = frozenset(c for c, t in schema.items() if t in (nw.Float32, nw.Float64))
    counts = _scalars(
        df,
        [nw.len().alias("__total")]
        + [
            _breaks(nw, r, floats).cast(nw.Int64).sum().alias(f"r{i}")
            for i, r in enumerate(row_rules)
        ],
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

    clean, quarantined = _split(nw, frame, df, row_rules, floats, failed)
    return clean, quarantined, results


def _split(
    nw: Any,
    frame: Any,
    df: Any,
    row_rules: List[ExpectationRule],
    floats: frozenset,
    failed: Dict[str, int],
) -> Tuple[Any, Optional[Any]]:
    """The clean rows and the quarantined rows (None when no quarantine rule broke)."""
    quarantine_rules = [r for r in row_rules if r.severity == "quarantine"]
    if not quarantine_rules or not any(failed.get(r.name) for r in quarantine_rules):
        return frame, None

    breaks_any = reduce(lambda a, b: a | b, [_breaks(nw, r, floats) for r in quarantine_rules])
    clean = df.filter(~breaks_any)
    reasons = nw.concat_str(
        [nw.when(_breaks(nw, r, floats)).then(nw.lit(r.name)) for r in quarantine_rules],
        separator=",",
        ignore_nulls=True,
    )
    quarantined = df.filter(breaks_any).with_columns(reasons.alias(FAILED_RULES_COLUMN))
    return nw.to_native(clean), nw.to_native(quarantined)


def cut(
    outputs: Dict[str, Any],
    expectations: Dict[str, ExpectationSet],
    results: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Cut outputs into clean and quarantined rows by rule results already found.

    Nothing is counted and nothing is checked: ``results`` (``RuleResult.as_dict``)
    say which quarantine rules broke. The engine uses this to hand the caller the
    same cut of the transform's own frames when the checked frames were held and
    are freed at task end (ADR 009). Returns the clean frames, and the quarantine
    outputs, by name.
    """
    nw = _nw()
    cut_frames: Dict[str, Any] = {}
    for name in sorted(expectations):
        if name not in outputs:
            continue
        spec = expectations[name]
        df = nw.from_native(outputs[name])
        schema = df.collect_schema()
        floats = frozenset(c for c, t in schema.items() if t in (nw.Float32, nw.Float64))
        row_rules = [
            r for r in spec.rules if r.kind in ("not_null", "between", "one_of", "matches")
        ]
        failed = {
            str(r.get("rule")): int(r.get("failed") or 0)
            for r in results
            if r.get("output") == name
        }
        clean, quarantined = _split(nw, outputs[name], df, row_rules, floats, failed)
        cut_frames[name] = clean
        if spec.quarantine:
            cut_frames[spec.quarantine] = (
                quarantined if quarantined is not None else _empty_quarantine(clean)
            )
    return cut_frames


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
