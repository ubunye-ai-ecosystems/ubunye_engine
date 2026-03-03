from typing import Any, Dict

from ubunye.core.interfaces import Task


class FeatureEng2(Task):
    """User-defined Spark transformation task."""

    def setup(self) -> None:
        pass

    def transform(self, sources: Dict[str, Any]) -> Dict[str, Any]:
        # Replace with your pure DataFrame transformations.
        df = sources.get("tx_data")
        return {"output_features": df}
