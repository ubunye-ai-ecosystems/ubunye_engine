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
