from typing import Any, Dict

from ubunye.core.interfaces import Task


class CleanData(Task):
    """Drop nulls and duplicates from raw events."""

    def transform(self, sources: Dict[str, Any]) -> Dict[str, Any]:
        df = sources["raw_events"]
        cleaned = df.dropna().dropDuplicates()
        return {"clean_events": cleaned}
