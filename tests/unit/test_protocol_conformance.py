"""The control-plane Protocols are contracts now, not documentation.

An audit found that the four Protocols in ubunye/interfaces/ (AuthBackend,
DeployAdapter, LineageBackend, RegistryBackend) were referenced only by docstrings.
No implementation inherited them, nothing checked against them. They were decorative:
a contract nobody had ever signed.

The design philosophy is structural typing, so the fix is NOT inheritance. It is this
file: every shipped implementation is checked against its Protocol, member by member.
If an implementation drifts from the contract, or someone adds a Protocol method
without teaching the implementations, this fails.
"""

from __future__ import annotations

import pytest

from ubunye.interfaces import AuthBackend, DeployAdapter, LineageBackend, RegistryBackend


def _load_cases():
    from ubunye.deploy.databricks import DatabricksDeployAdapter
    from ubunye.deploy.databricks.auth import ServicePrincipalAuthBackend, TokenAuthBackend
    from ubunye.lineage.delta_store import DeltaLineageBackend
    from ubunye.lineage.storage import FileSystemLineageStore
    from ubunye.models.registry import FilesystemRegistryBackend

    return [
        (TokenAuthBackend, AuthBackend),
        (ServicePrincipalAuthBackend, AuthBackend),
        (DatabricksDeployAdapter, DeployAdapter),
        (DeltaLineageBackend, LineageBackend),
        (FileSystemLineageStore, LineageBackend),
        (FilesystemRegistryBackend, RegistryBackend),
    ]


@pytest.mark.parametrize("impl,proto", _load_cases(), ids=lambda x: getattr(x, "__name__", str(x)))
def test_every_shipped_implementation_signs_its_contract(impl, proto):
    missing = [m for m in dir(proto) if not m.startswith("_") and not hasattr(impl, m)]
    assert not missing, (
        f"{impl.__name__} is registered as a {proto.__name__} but lacks: {missing}. "
        "Either implement the members or change the Protocol on purpose."
    )
