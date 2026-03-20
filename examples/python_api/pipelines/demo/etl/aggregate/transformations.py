from typing import Any, Dict

from ubunye.core.interfaces import Task


class Aggregate(Task):
    """Group clean events and count occurrences."""

    def transform(self, sources: Dict[str, Any]) -> Dict[str, Any]:
        df = sources["clean_events"]
        # Example: count rows per partition (adjust columns to your data)
        summary = df.groupBy().count()
        return {"summary": summary}
