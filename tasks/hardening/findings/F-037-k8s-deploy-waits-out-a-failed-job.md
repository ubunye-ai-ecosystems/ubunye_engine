# F-037: `ubunye deploy k8s` sat out the whole timeout after the Job had failed

**Status:** fixed on feat/prove-r1 (2026-09-29), not yet merged
**Severity:** minor (time and, on a paid cluster, money; never a wrong verdict)
**Source:** proving R1 on Kubernetes (kind), ubunye-infra prove-r1 run 36576881079
**Promise:** a failed cloud run fails fast and says why

## What happens
The deploy followed the Job with `kubectl wait --for=condition=complete
--timeout=1800s`. A Job that fails never becomes Complete, so the command waited the
full 30 minutes. In run 36576881079 the same image failed on Container Apps within a
minute (F-036, no pyarrow), while the kind step sat from 13:44 until the workflow was
cancelled at 14:00, printing nothing (the log is shown only at the end).

## Repro
`tests/unit/deploy/test_container_deploy.py::test_a_k8s_job_is_followed_to_failed_as_well_as_complete`
runs the deploy's Kubernetes program against a fake kubectl whose Job reaches
`Failed`. On the old code the fake clock passes the 1,800 second timeout.

## Fix
The program polls the Job's true conditions every 5 seconds and stops at `Complete`
(success) or `Failed` (exit 1), then prints the Job's log as before, so a failed run's
record and error still come back. The timeout still bounds a Job that never ends.

## Follow-up (skeptic review)
The poll ignored `kubectl get`'s exit code, so a Job deleted by hand, an RBAC refusal
or an expired token still waited the whole timeout and hid the error. Now three failed
reads in a row stop the deploy (exit 1) with kubectl's own error. Test:
`test_a_k8s_job_that_kubectl_cannot_read_fails_fast_and_says_why` (fails on the old code).
