"""E-10: when E10_PATCH=1, the run's rows-v1 hash uses the threaded native prototype."""

import os

if os.environ.get("E10_PATCH") == "1":
    import e10_native

    if not e10_native.install():
        raise SystemExit("E-10: the native kernel did not load")
