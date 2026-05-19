"""Formal interfaces (typing.Protocol) for Ubunye's pluggable seams.

Each protocol defines the structural contract that backends must satisfy.
Implementations are discovered at runtime via entry-point groups and
auto-detect classmethods.

Cross-boundary dataclasses accompany each protocol so that callers never
depend on a concrete backend's internal types.
"""

from ubunye.interfaces.auth import AuthBackend, Credentials
from ubunye.interfaces.deploy import DeployAdapter, DeployContext, DeployResult
from ubunye.interfaces.lineage import LineageBackend, LineageRecord
from ubunye.interfaces.registry import ModelVersionInfo, RegistryBackend

__all__ = [
    "AuthBackend",
    "Credentials",
    "DeployAdapter",
    "DeployContext",
    "DeployResult",
    "LineageBackend",
    "LineageRecord",
    "ModelVersionInfo",
    "RegistryBackend",
]
