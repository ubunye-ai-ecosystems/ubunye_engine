"""E-04: can a secret reach anything a run leaves behind?

A unique marker is put in every place a user might put a secret, the run is made to
use them all, and every artifact is searched for every marker: run and lineage
records, OpenLineage events, stdout and stderr of `run`, `run --json`, `plan --json`,
`lineage show`, and every file under the task and output folders.

A local HTTP server plays the API and records what it received, so the test also
proves each secret was really used (a secret that was never sent proves nothing).

Usage: python tests/experiments/e04_secrets.py <ubunye executable> [spark|pandas]
"""

from __future__ import annotations

import base64
import json
import os
import shutil
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

UBUNYE = sys.argv[1]
BACKEND = sys.argv[2] if len(sys.argv) > 2 else "spark"
HERE = os.path.dirname(os.path.abspath(__file__))

MARKERS = {
    "secret_ref_bearer": "MKR-A-bearer-7f3a9",  # auth.token: secret://env/...
    "jinja_env_header": "MKR-B-header-51c2e",  # headers: "{{ env.X }}"
    "secret_ref_param": "MKR-C-param-9d0b1",  # params: secret://env/...
    "jinja_env_param": "MKR-D-query-2e8f4",  # params: "{{ env.X }}"
    "cli_var": "MKR-E-var-66a1c",  # --var token=...
    "basic_password": "MKR-F-basic-c0ffe",  # auth basic password: secret://env/...
}

received = []


def forms(label: str, marker: str) -> list:
    """The marker as sent: basic auth carries base64("user:password")."""
    out = [marker]
    if label == "basic_password":
        out.append(base64.b64encode(f"svc:{marker}".encode()).decode())
    return out


class Api(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        received.append({"path": self.path, "headers": dict(self.headers)})
        body = json.dumps({"data": [{"id": 1, "city": "jhb"}, {"id": 2, "city": "cpt"}]})
        data = body.encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


TRANSFORM = """\
from ubunye.core.interfaces import Task


class Pull(Task):
    def transform(self, sources):
        return {"out_a": sources["api_a"], "out_b": sources["api_b"]}
"""


def config(url: str, root: str) -> str:
    root = root.replace("\\", "/")
    return f"""\
MODEL: etl
VERSION: "1.0.0"
CONFIG:
  inputs:
    api_a:
      format: rest_api
      url: "{url}/a"
      headers:
        X-Api-Key: "{{{{ env.E04_HEADER }}}}"
      params:
        key: "secret://env/E04_PARAM"
        other: "{{{{ env.E04_QUERY }}}}"
        who: "{{{{ token }}}}"
      auth:
        type: bearer
        token: "secret://env/E04_BEARER"
      response:
        root_key: data
    api_b:
      format: rest_api
      url: "{url}/b"
      auth:
        type: basic
        username: "svc"
        password: "secret://env/E04_BASIC"
      response:
        root_key: data
  transform: {{}}
  outputs:
    out_a:
      format: s3
      path: "{root}/out/a"
      file_format: parquet
      mode: overwrite
    out_b:
      format: s3
      path: "{root}/out/b"
      file_format: parquet
      mode: overwrite
"""


def main() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), Api)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_address[1]}"

    work = os.path.join(HERE, "work_e04")
    shutil.rmtree(work, ignore_errors=True)
    task = os.path.join(work, "uc", "pkg", "pull")
    os.makedirs(task)
    with open(os.path.join(task, "transformations.py"), "w") as fh:
        fh.write(TRANSFORM)
    with open(os.path.join(task, "config.yaml"), "w") as fh:
        fh.write(config(url, work))

    env = dict(
        os.environ,
        E04_BEARER=MARKERS["secret_ref_bearer"],
        E04_HEADER=MARKERS["jinja_env_header"],
        E04_PARAM=MARKERS["secret_ref_param"],
        E04_QUERY=MARKERS["jinja_env_param"],
        E04_BASIC=MARKERS["basic_password"],
        UBUNYE_OPENLINEAGE_FILE=os.path.join(work, "openlineage.jsonl"),
        OPENLINEAGE_DISABLED="false",
    )
    where = ["-d", work, "-u", "uc", "-p", "pkg", "-t", "pull", "--backend", BACKEND]
    var = ["--var", f"token={MARKERS['cli_var']}"]
    outputs = {}
    for name, args in {
        "plan --json": ["plan", *where, *var, "--json"],
        "run": ["run", *where, *var, "--lineage"],
        "lineage list": ["lineage", "list", "-d", work, "-u", "uc", "-p", "pkg", "-t", "pull"],
    }.items():
        proc = subprocess.run([UBUNYE, *args], capture_output=True, text=True, env=env)
        outputs[name] = (proc.returncode, proc.stdout + proc.stderr)
        print(f"{name:<14} exit {proc.returncode}")
    server.shutdown()

    # Were the secrets really sent?
    sent = json.dumps(received)
    print("\nused by the run (seen by the API):")
    for label, marker in MARKERS.items():
        used = any(f in sent for f in forms(label, marker))
        print(f"  {label:<18} {'yes' if used else 'NO'}")

    print("\nfound in artifacts (must be none):")
    leaks = []
    for name, (_, text) in outputs.items():
        for label, marker in MARKERS.items():
            if any(f in text for f in forms(label, marker)):
                leaks.append((f"output of `{name}`", label))
    for folder, _, files in os.walk(work):
        for f in files:
            path = os.path.join(folder, f)
            try:
                with open(path, "rb") as fh:
                    data = fh.read()
            except OSError:
                continue
            for label, marker in MARKERS.items():
                if any(f.encode() in data for f in forms(label, marker)):
                    leaks.append((os.path.relpath(path, work), label))
    for where_, label in sorted(set(leaks)):
        print(f"  LEAK {label:<18} in {where_}")
    if not leaks:
        print("  none")
    with open(os.path.join(HERE, "e04_outputs.json"), "w") as fh:
        json.dump({k: v[1][-4000:] for k, v in outputs.items()}, fh, indent=1)


if __name__ == "__main__":
    main()
