# E-04: Can a password or token reach a run record, lineage record, OpenLineage event or log?

**Status:** answered (2026-09-28): passes for secret:// and env; fails for --var
**Why:** Spline (AbsaOSS) captured plaintext JDBC passwords in lineage (spline-spark-agent issue 69) and had to add a redaction filter.

## Method
Configs with secrets in every place a user might put one: secret:// refs, env vars, JDBC URLs with user and password, REST headers, options. Search every artifact the run leaves.

## Pass means
No secret value appears in any artifact.

## Result
Harness: tests/experiments/e04_secrets.py, Spark backend (the REST connector needs
Spark, F-015). Six markers, each confirmed sent to a local API (basic auth checked in
its base64 form), then every artifact searched: the run and lineage records, the
OpenLineage events file, `plan --json`, `run` and `lineage list` output, every file
under the work folder.

| Where the secret was put | Used | Leaked |
|---|---|---|
| `auth.token: secret://env/...` (bearer) | yes | no |
| `auth.password: secret://env/...` (basic) | yes | no |
| `params: secret://env/...` | yes | no |
| `headers: "{{ env.X }}"` | yes | no |
| `params: "{{ env.X }}"` | yes | no |
| `--var token=...` | yes | yes: lineage record, OpenLineage events, plan output (F-016) |

The environment block of the record holds only Python, platform and package versions,
never environment variables.

Not yet covered: JDBC URLs with a password inside the URL string, Databricks and cloud
secret providers, logs at DEBUG level, error messages from a failed call.
