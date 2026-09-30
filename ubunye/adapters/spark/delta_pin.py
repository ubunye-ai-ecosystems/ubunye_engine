"""Pin a Delta read to one version, so every consumer of the frame sees one snapshot.

A lazy Delta frame reads the table's latest version on every action. So one input
read once could feed two outputs from two versions (10 rows and 13 rows, when another
job appended between them), and the run record's input hash could read a third
(F-046). A read that names no version is pinned here to the version the table is at
when it is read (``versionAsOf``). A read that names one (``version_as_of``,
``timestamp_as_of``, or ``versionAsOf`` / ``timestampAsOf`` in ``options``, any case)
is left as the user wrote it.

The frame carries the pin (``ATTR``) so the run record can say the engine made it.
If the version cannot be read (no Delta SQL extension, no permission), the read is
left unpinned, as before.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

#: The attribute on a pinned frame: {"version", "timestamp", "pinned_by": "engine"}.
ATTR = "ubunye_source_pin"

_PIN_KEYS = ("versionasof", "timestampasof")


def user_pin(cfg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """The version the config pins the read to, or None. Options match in any case."""
    options = {str(k).lower(): v for k, v in (cfg.get("options") or {}).items()}
    version = cfg.get("version_as_of")
    if version is None:
        version = options.get("versionasof")
    if version is not None:
        return {"version": int(version)}
    stamp = cfg.get("timestamp_as_of")
    if stamp is None:
        stamp = options.get("timestampasof")
    if stamp is not None:
        return {"timestamp_as_of": str(stamp)}
    return None


def latest(spark: Any, table: str) -> Dict[str, Any]:
    """The table's latest version and its time, from the Delta log (no data read)."""
    row = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").collect()[0]
    stamp = row["timestamp"]
    return {
        "version": int(row["version"]),
        "timestamp": stamp.isoformat() if hasattr(stamp, "isoformat") else str(stamp),
    }


def pin_options(
    spark: Any, table: str, cfg: Dict[str, Any], options: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """Add ``versionAsOf`` to ``options`` unless the config pins the read already.

    Returns the engine's pin, or None when the user pinned it or it could not be read.
    """
    if user_pin(cfg) is not None or any(str(k).lower() in _PIN_KEYS for k in options):
        return None
    try:
        pin = latest(spark, table)
    except Exception:  # noqa: BLE001 - no version to pin to: read as before
        return None
    options["versionAsOf"] = pin["version"]
    return dict(pin, pinned_by="engine")


def mark(frame: Any, pin: Optional[Dict[str, Any]]) -> Any:
    """Put the engine's pin on the frame, for the run record. Never fails the read."""
    if pin is not None:
        try:
            setattr(frame, ATTR, pin)
        except Exception:  # noqa: BLE001
            pass
    return frame
