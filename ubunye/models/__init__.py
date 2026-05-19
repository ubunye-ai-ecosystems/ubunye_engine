"""ubunye.models — library-independent model registry and lifecycle management."""

from ubunye.models.base import UbunyeModel
from ubunye.models.gates import GateResult, PromotionGate
from ubunye.models.loader import load_model_class
from ubunye.models.mlflow_registry import MLflowRegistryBackend
from ubunye.models.registry import (
    FilesystemRegistryBackend,
    ModelRecord,
    ModelRegistry,
    ModelStage,
    ModelVersion,
)

__all__ = [
    "UbunyeModel",
    "load_model_class",
    "FilesystemRegistryBackend",
    "MLflowRegistryBackend",
    "ModelRegistry",
    "ModelVersion",
    "ModelRecord",
    "ModelStage",
    "PromotionGate",
    "GateResult",
]
