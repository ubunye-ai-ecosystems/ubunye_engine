"""Spark adapter — the Spark-specific mechanism that lives outside the hexagon.

The engine core owns the *contract and the decision* (``ubunye.core.ports``,
``ubunye.core.interfaces``, and the write-mode grammar in
``ubunye.core.write_modes``). This package owns the *Spark mechanism* that
honours those decisions: how a resolved write mode actually becomes a
``df.write`` chain, a ``MERGE INTO`` statement, or a ``USE CATALOG``.

Nothing in ``ubunye.core`` may import from here — that is the whole point of
moving it (issue #37). Backends and Spark writer plugins do.
"""
