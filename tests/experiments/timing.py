"""Where does a 13 s run spend its time? Time the phases of one `ubunye run`."""

import os
import subprocess
import sys
import time

ubunye, root = sys.argv[1], sys.argv[2]
base = [ubunye, "run", "-d", root, "-u", "uc", "-p", "pkg", "-t", "t", "--backend", "pandas"]


def timed(label, cmd, env=None):
    t0 = time.perf_counter()
    subprocess.run(cmd, capture_output=True, env=env)
    print(f"{label:<40} {time.perf_counter() - t0:6.2f} s")


timed("ubunye version", [ubunye, "version"])
timed("run, no lineage", base + ["-dt", "5"])
timed("run, --lineage", base + ["--lineage", "-dt", "6"])
env = dict(os.environ, UBUNYE_TELEMETRY="0")
timed("run, --lineage, UBUNYE_TELEMETRY=0", base + ["--lineage", "-dt", "7"], env)
