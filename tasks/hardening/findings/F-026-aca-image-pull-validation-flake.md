# F-026: creating the Container Apps job once failed to validate its image pull

**Status:** open (watching)
**Severity:** minor
**Source:** proving ground, prove-c01 run 36496707929 (2026-09-28), Azure column
**Promise:** none; a platform flake, not a Ubunye result

## What happens
`ubunye deploy container-apps` (which calls `az containerapp job create` with
`--registry-identity`) failed: the Azure CLI tried to create an AcrPull role assignment
(the workflow's identity may not; the job identity already has AcrPull on the registry),
then the job create was rejected: "unable to pull image using Managed identity". Rerun
of the same job, same inputs: success, no such messages. Three earlier runs never showed
it. Likely a newer containerapp CLI extension (installed dynamically) or Azure-side
propagation after the job was deleted and recreated.

## Next
If it recurs: retry the create once on this specific error in `ubunye deploy
container-apps`, or keep one long-lived job and update it in place.
