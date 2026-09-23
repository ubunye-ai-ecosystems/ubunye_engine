"""pandas behind the engine's data port.

Thirty lines, as the design notes promised. This is what ports and adapters buys:
adding a data engine to the system is a shim, not a fork.

What this unlocks (from blog.md, verbatim intent):

* Engine tests on REAL data with no SparkSession, no JVM, no Java on the runner —
  instead of mock objects with hard-coded return values.
* The road to ``--backend pandas``: run a task on a laptop with nothing but Python.
"""

from __future__ import annotations

from typing import Any, Dict, List


class PandasDataFrameAdapter:
    """Make a pandas DataFrame satisfy :class:`ubunye.core.ports.DataFramePort`.

    pandas has the *concepts* but different spellings — ``len(df)`` not ``count()``,
    ``to_dict("records")`` not ``collect()``, and ``df.count()`` exists but means
    something else entirely (non-null counts per column). The adapter translates;
    nothing is copied until ``collect()`` is called.
    """

    def __init__(self, df: Any) -> None:
        self._df = df

    @property
    def native(self) -> Any:
        """The wrapped pandas DataFrame, for code that needs the real thing."""
        return self._df

    @property
    def schema(self) -> Dict[str, str]:
        return {column: str(dtype) for column, dtype in self._df.dtypes.items()}

    def count(self) -> int:
        return len(self._df)

    def collect(self) -> List[Dict[str, Any]]:
        return self._df.to_dict("records")

    # ------------------------------------------------------------------
    # Row-bounding, spelled the way the engine already asks for it.
    #
    # These are not on DataFramePort, and they should not be: a Spark
    # DataFrame satisfies that port natively and adding methods to it would
    # break that. They exist because the engine asks every frame it holds to
    # bound itself in two places, and pandas answered neither:
    #
    #   * lineage fingerprinting calls .sample(fraction=, seed=).limit(n)
    #   * `run --sample N` calls .limit(n)
    #
    # Without them the fingerprint's AttributeError was swallowed and lineage
    # recorded the SCHEMA hash as the data hash, so two completely different
    # frames produced the same "data" hash and `lineage compare` called two
    # unrelated runs identical.
    # ------------------------------------------------------------------

    def limit(self, num: int) -> "PandasDataFrameAdapter":
        """The first ``num`` rows, wrapped again so it stays a port."""
        return PandasDataFrameAdapter(self._df.head(max(int(num), 0)))

    def sample(
        self, fraction: float = 1.0, seed: int = 42, withReplacement: bool = False
    ) -> "PandasDataFrameAdapter":
        """A deterministic row sample, in Spark's spelling.

        pandas calls the argument ``frac`` and the seed ``random_state``. The
        translation is the whole point of an adapter.
        """
        frac = min(max(float(fraction), 0.0), 1.0)
        sampled = self._df.sample(
            frac=frac, random_state=int(seed), replace=bool(withReplacement)
        )
        return PandasDataFrameAdapter(sampled)
