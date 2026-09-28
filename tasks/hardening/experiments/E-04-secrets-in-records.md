# E-04: Can a password or token reach a run record, lineage record, OpenLineage event or log?

**Status:** planned
**Why:** Spline (AbsaOSS) captured plaintext JDBC passwords in lineage (spline-spark-agent issue 69) and had to add a redaction filter.

## Method
Configs with secrets in every place a user might put one: secret:// refs, env vars, JDBC URLs with user and password, REST headers, options. Search every artifact the run leaves.

## Pass means
No secret value appears in any artifact.

## Result
Not run yet.
