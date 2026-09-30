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
- **Nothing is lost silently.** A ``reconcile`` compares an output with an input
  the transform received: its row count, and a column's sum, within a tolerance
  (F-017). Quarantined rows count as carried over; they were set aside, not lost.
- **Check the source before trusting it.** Expectations may also name an input:
  its rules run right after it is read, before the transform (F-018). The
  ``columns`` rule checks each column's type by the run record's names.

A null passes every rule except ``not_null``, as in SQL. In a float column NaN counts
as missing too, on every backend (F-045): pandas stores a missing float as NaN, so it
cannot tell the two apart, and a rule must not pass on one backend and fail on the
other. So ``not_null`` breaks on NaN, and ``between`` and ``one_of`` let it pass.
"""

from __future__ import annotations

import decimal
import logging
import math
import numbers
from dataclasses import asdict, dataclass
from functools import reduce
from typing import Any, Dict, List, Optional, Tuple

from ubunye.config.schema import ExpectationRule, ExpectationSet, allowed
from ubunye.core.errors import ExpectationError, TransformOutputError

log = logging.getLogger(__name__)

#: The column added to quarantined rows: the names of the rules each row broke.
FAILED_RULES_COLUMN = "_ubunye_failed_rules"


@dataclass
class RuleResult:
    """What one rule found on one output, or on one input (``side``)."""

    output: str  # the output's name, or the input's when side is "input"
    rule: str
    kind: str
    severity: str
    column: Optional[str]
    failed: int  # rows breaking it (duplicate rows for unique; 1 or 0 for row_count)
    total: int  # rows checked (input rows for a reconcile of rows)
    passed: bool
    #: What was found, in words, where a count alone does not say it (reconcile,
    #: columns).
    detail: Optional[str] = None
    #: "input" for an input contract, checked before the transform (F-018).
    side: str = "output"

    def as_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        # Left out when they say nothing new, so records stay as they were.
        if d["detail"] is None:
            del d["detail"]
        if d["side"] == "output":
            del d["side"]
        return d


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
    if isinstance(spec, dict):  # columns: the rule itself reports a missing column
        return []
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


def _check_columns(nw: Any, name: str, df: Any, spec: ExpectationSet, side: str = "output") -> None:
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
                    f"which the {side} does not have.",
                    context={
                        side.title(): name,
                        "Rule": rule.name,
                        "Columns": ", ".join(available),
                    },
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


def _sum(nw: Any, column: str, floats: frozenset) -> Any:
    """A column's sum, leaving out missing values (null, and NaN in a float column).

    Spark's sum of a column holding NaN is NaN and pandas skips it; NaN counts as
    missing on every backend here, as it does for the rules (F-045). Used only
    for a frame that is neither Spark nor pandas nor Arrow (see :func:`_exact_sums`).
    """
    if column in floats:
        return nw.when(~_missing(nw, column, floats)).then(nw.col(column)).sum()
    return nw.col(column).sum()


def _exact_sums(frame: Any, columns: List[str]) -> Tuple[int, Dict[str, Any]]:
    """Row count and the sums of ``columns``: exact for integers and decimals.

    Spark, pandas and Arrow frames are summed natively (the sums a reconcile
    compares must not wrap at 2**63, nor round a decimal through a float).
    Anything else is summed by Narwhals.
    """
    from ubunye.adapters.sums import exact_sums

    native = exact_sums(frame, columns)
    if native is not None:
        return native
    nw = _nw()
    df = nw.from_native(frame)
    schema = df.collect_schema()
    floats = frozenset(c for c, t in schema.items() if t in (nw.Float32, nw.Float64))
    got = _scalars(
        df,
        [nw.len().alias("__rows")]
        + [_sum(nw, c, floats).alias(f"s{i}") for i, c in enumerate(columns)],
    )
    return int(got["__rows"]), {c: got[f"s{i}"] for i, c in enumerate(columns)}


def _numeric_column(nw: Any, where: str, df: Any, column: str, rule: str) -> None:
    """``column`` exists in ``df`` and is a number, or an ExpectationError says which."""
    schema = df.collect_schema()
    available = list(schema.names())
    if column not in available:
        raise ExpectationError(
            f"{where}: {rule} names column '{column}', which it does not have.",
            context={"Frame": where, "Rule": rule, "Columns": ", ".join(available)},
            hint="Check the column name in CONFIG.expectations.",
        )
    if not schema[column].is_numeric():
        raise ExpectationError(
            f"{where}: {rule} sums column '{column}', which is {schema[column]}, not a number.",
            context={"Frame": where, "Rule": rule, "Column": column},
            hint=_TEXT_NUMBER_HINT.format(column=column),
        )


def measure_inputs(
    inputs: Optional[Dict[str, Any]], expectations: Dict[str, ExpectationSet]
) -> Dict[str, Dict[str, Any]]:
    """Row count and asked-for sums of every input a ``reconcile`` names.

    Taken **before the transform runs**: a pandas transform may change its
    input in place (drop rows, overwrite a column), and counting afterwards
    would hide exactly the loss a reconcile is for. One pass over each such
    input. On a lazy backend (Spark) that pass reads the source: inputs are not
    held (ADR 009). Returns ``{input: {"rows": n, "sums": {column: value}}}``.
    """
    wanted: Dict[str, List[str]] = {}
    for spec in expectations.values():
        for check in spec.reconcile:
            columns = wanted.setdefault(check.input, [])
            if check.sum is not None:
                column = check.sum.input_column or check.sum.column
                if column not in columns:
                    columns.append(column)
    if not wanted:
        return {}
    missing = sorted(n for n in wanted if not inputs or n not in inputs)
    if missing:
        raise ExpectationError(
            f"reconcile needs the input frames the transform received, and "
            f"{', '.join(repr(m) for m in missing)} was not given.",
            context={"Missing inputs": ", ".join(missing)},
            hint="In a notebook, call read() or transform() before write(). With the "
            "Engine, call read_inputs() or apply_transforms() first, or pass inputs= to "
            "write_outputs().",
        )
    nw = _nw()
    measured: Dict[str, Dict[str, Any]] = {}
    for name in sorted(wanted):
        frame = inputs[name]  # type: ignore[index]
        df = nw.from_native(frame)
        for column in wanted[name]:
            _numeric_column(nw, f"input {name}", df, column, "reconcile")
        rows, sums = _exact_sums(frame, wanted[name])
        measured[name] = {"rows": rows, "sums": sums}
    return measured


def _text(value: Any) -> str:
    """A number written exactly: an int as is, a Decimal in full, a float round trip."""
    if isinstance(value, numbers.Integral):
        return str(int(value))
    if isinstance(value, decimal.Decimal):
        return format(value, "f")
    value = float(value)
    if math.isfinite(value) and value.is_integer() and abs(value) < 1e16:
        return str(int(value))
    return repr(value)


def _difference(before: Any, after: Any) -> Any:
    """after - before, exactly: ints as ints, Decimals as Decimals, else floats.

    Two equal totals match, infinities included; two NaN totals (each side
    holding +inf and -inf) match too.
    """
    if isinstance(before, float) or isinstance(after, float):
        b, a = float(before), float(after)
        if a == b or (math.isnan(a) and math.isnan(b)):
            return 0.0
        return a - b
    if isinstance(before, decimal.Decimal) or isinstance(after, decimal.Decimal):
        return _as_decimal(after) - _as_decimal(before)
    return int(after) - int(before)


def _as_decimal(value: Any) -> decimal.Decimal:
    if isinstance(value, decimal.Decimal):
        return value
    return decimal.Decimal(int(value))


def _limit_text(value: Any) -> str:
    return value if isinstance(value, str) else _text(value)


def _reconcile(
    name: str,
    spec: ExpectationSet,
    total: int,
    sums: Dict[str, Any],
    measured: Dict[str, Dict[str, Any]],
) -> List[RuleResult]:
    """The reconcile results for one output, from counts already taken."""
    results: List[RuleResult] = []
    for check in spec.reconcile:
        got = measured[check.input]
        if check.rows is not None:
            read = int(got["rows"])
            lost, gained = max(read - total, 0), max(total - read, 0)
            ok = True
            limits = []
            for label, bound, moved in (
                ("lost", check.rows.max_lost, lost),
                ("gained", check.rows.max_gained, gained),
            ):
                if bound is not None:
                    ok = ok and moved <= allowed(bound, read)
                    limits.append(f"at most {_limit_text(bound)} {label}")
            results.append(
                RuleResult(
                    output=name,
                    rule=check.rows_name,
                    kind="reconcile",
                    severity=check.severity,
                    column=None,
                    failed=lost or gained,
                    total=read,
                    passed=ok,
                    detail=f"{read} rows read from {check.input}, {total} reached {name}: "
                    f"{lost} lost, {gained} gained ({', '.join(limits)})",
                )
            )
        if check.sum is not None:
            source = check.sum.input_column or check.sum.column
            before, after = got["sums"][source], sums[check.sum.column]
            floaty = isinstance(before, float) or isinstance(after, float)
            base = float(before) if floaty else before
            with decimal.localcontext() as ctx:
                ctx.prec = 100  # a decimal256 total has up to 76 digits
                diff = _difference(before, after)
                ok = abs(diff) <= allowed(check.sum.tolerance, base)
            results.append(
                RuleResult(
                    output=name,
                    rule=check.sum_name,
                    kind="reconcile",
                    severity=check.severity,
                    column=check.sum.column,
                    failed=0 if ok else 1,
                    total=1,
                    passed=ok,
                    detail=f"sum of {source} in {check.input} {_text(before)}, of "
                    f"{check.sum.column} in {name} {_text(after)}: difference "
                    f"{_text(diff)} (at most {_limit_text(check.sum.tolerance)})",
                )
            )
    return results


def _shape(name: str, frame: Any, spec: ExpectationSet, side: str) -> Dict[str, RuleResult]:
    """The ``columns`` rules' results, from the frame's schema (no row is read).

    Types are compared by the run record's names (ADR 006), exactly: ``int32``
    is not ``int64``. List the types a column may have to accept more than one.
    Nulls are not part of the type.
    """
    rules = [r for r in spec.rules if r.columns is not None]
    if not rules:
        return {}
    from ubunye.lineage.content_hash import frame_kinds

    found = frame_kinds(frame)
    results: Dict[str, RuleResult] = {}
    for rule in rules:
        wanted = rule.columns or {}
        problems = []
        for column, types in wanted.items():
            if column not in found:
                problems.append(f"{column}: expected {' or '.join(types)}, missing")
            elif found[column] not in types:
                problems.append(f"{column}: expected {' or '.join(types)}, found {found[column]}")
        checked = len(wanted)
        if rule.extra == "forbid":
            extra = [c for c in found if c not in wanted]
            problems += [f"{c}: not expected (extra: forbid), found {found[c]}" for c in extra]
            checked += len(extra)
        results[rule.name] = RuleResult(
            output=name,
            rule=rule.name,
            kind="columns",
            severity=rule.severity,
            column=None,
            failed=len(problems),
            total=checked,
            passed=not problems,
            detail="; ".join(problems) if problems else None,
            side=side,
        )
    return results


def check_output(
    name: str,
    frame: Any,
    spec: ExpectationSet,
    measured: Optional[Dict[str, Dict[str, Any]]] = None,
    side: str = "output",
) -> Tuple[Any, Optional[Any], List[RuleResult]]:
    """Check one output: (clean frame, quarantined frame or None, results).

    ``measured`` is :func:`measure_inputs` for the inputs the output's
    ``reconcile`` names. Their checks count the output before any row is
    quarantined, so a quarantined row counts as carried over. ``side`` is
    "input" for an input contract (F-018).

    ``columns`` rules run first, from the schema alone. If one with severity
    fail breaks, the other rules are not run: they would check a frame of the
    wrong shape. A set of nothing but ``columns`` rules never reads a row.
    """
    nw = _nw()
    shape = _shape(name, frame, spec, side)
    wrong_shape = any(not r.passed and r.severity == "fail" for r in shape.values())
    only_shape = all(r.kind == "columns" for r in spec.rules) and not spec.reconcile
    if wrong_shape or only_shape:
        return frame, None, [shape[r.name] for r in spec.rules if r.name in shape]
    df = nw.from_native(frame)
    _check_columns(nw, name, df, spec, side)
    measured = measured if measured is not None else measure_inputs(None, {name: spec})
    sum_columns: List[str] = []
    for check in spec.reconcile:
        if check.sum is not None and check.sum.column not in sum_columns:
            _numeric_column(nw, name, df, check.sum.column, "reconcile")
            sum_columns.append(check.sum.column)

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
        shape.get(r.name)
        or RuleResult(
            output=name,
            rule=r.name,
            kind=r.kind,
            severity=r.severity,
            column=r.column,
            failed=failed[r.name],
            total=total,
            passed=failed[r.name] == 0,
            side=side,
        )
        for r in spec.rules
    ]
    # Sums are exact (no wrap at 2**63, no decimal through a float), so they are
    # taken natively, in a pass of their own; the output is held (ADR 009).
    sums = _exact_sums(frame, sum_columns)[1] if sum_columns else {}
    results += _reconcile(name, spec, total, sums, measured)

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
    outputs: Dict[str, Any],
    expectations: Dict[str, ExpectationSet],
    inputs: Optional[Dict[str, Any]] = None,
    measured: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Tuple[Dict[str, Any], List[RuleResult]]:
    """Check every output that has expectations; nothing is written here.

    Returns the outputs to write (clean frames, plus quarantined rows under their
    quarantine output) and every rule's result. Raises :class:`ExpectationError`
    if any ``fail`` rule is broken or a quarantine rate is exceeded. A
    ``reconcile`` compares with ``measured`` (:func:`measure_inputs`, taken
    before the transform ran), or else measures ``inputs`` now.
    """
    if not expectations:
        return outputs, []
    outputs = dict(outputs)
    results: List[RuleResult] = []
    problems: List[str] = []
    reconciled = {n: s for n, s in expectations.items() if n in outputs and s.reconcile}
    measured = dict(measured or {})
    unmeasured = {
        n: s for n, s in reconciled.items() if any(c.input not in measured for c in s.reconcile)
    }
    if unmeasured:
        measured.update(measure_inputs(inputs, unmeasured))

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
        clean, quarantined, found = check_output(name, outputs[name], spec, measured)
        results += found
        outputs[name] = clean
        if spec.quarantine:
            if quarantined is not None:
                outputs[spec.quarantine] = quarantined
            else:
                outputs[spec.quarantine] = _empty_quarantine(clean)

        problems += _report(name, found)
        total = next((r.total for r in found if r.kind not in ("columns", "reconcile")), 0)
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


def _report(name: str, found: List[RuleResult]) -> List[str]:
    """Log each broken warn or quarantine rule; return a line per broken fail rule."""
    problems: List[str] = []
    for r in found:
        if r.passed:
            continue
        if r.detail:
            line = f"{name}: {r.rule} ({r.kind}): {r.detail}"
        else:
            line = f"{name}: {r.rule} ({r.kind}) broken by {r.failed} of {r.total} rows"
        if r.severity == "fail":
            problems.append(line)
        elif r.severity == "warn":
            log.warning("expectation warning: %s", line)
        else:
            log.info("quarantined: %s", line)
    return problems


def check_inputs(
    inputs: Dict[str, Any], expectations: Dict[str, ExpectationSet]
) -> List[RuleResult]:
    """Check every input that has expectations, before the transform (F-018).

    ``expectations`` holds the input contracts only (keyed by input name). A
    ``columns`` rule reads the schema alone; any other rule costs one pass over
    the input (on Spark, a read of the source: inputs are not held, ADR 009).
    Raises :class:`ExpectationError` if a ``fail`` rule is broken: the transform
    does not run and nothing is written.
    """
    results: List[RuleResult] = []
    problems: List[str] = []
    for name in sorted(expectations):
        if name not in inputs:
            continue
        _, _, found = check_output(name, inputs[name], expectations[name], side="input")
        results += found
        problems += _report(name, found)
    if problems:
        raise ExpectationError(
            "An input broke its expectations, so the transform did not run and nothing "
            "was written:\n  " + "\n  ".join(problems),
            results=[r.as_dict() for r in results],
            hint="The source has changed. Fix it, or change the input's expectations in "
            "CONFIG.expectations if the change is meant.",
        )
    return results


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
