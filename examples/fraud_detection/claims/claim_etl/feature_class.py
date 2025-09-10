"""User-defined Spark transformations for the 'claim_etl' task."""
from typing import Dict
from ubunye.core.interfaces import Task

class ClaimEtl(Task):
    """Example task that simply forwards sources to outputs.

    Replace with your business logic using PySpark DataFrames.
    """

    def setup(self) -> None:
        """Optional: prepare runtime variables before transform."""
        # e.g., self.today = datetime.date.today().isoformat()
        pass

    def transform(self, sources: Dict[str, object]) -> Dict[str, object]:
        """Apply transformations.

        Parameters
        ----------
        sources: Dict[str, object]
            Mapping of input name to Spark DataFrame.

        Returns
        -------
        Dict[str, object]
            Mapping of output name to Spark DataFrame.
        """
        return {"bronze": sources["raw_claims"]}
