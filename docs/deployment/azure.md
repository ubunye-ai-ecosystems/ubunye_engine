# Deploying on Azure

This page shows how to run a task on Azure Container Apps with one command. It
is for anyone with an Azure subscription. Azure Container Apps is
[proven](../deployment.md#where-you-can-deploy-today): the
[proving ground](../proving-ground/latest.md) runs its workloads there and gets
the same data as local Spark.

**The problem:** you want a task to run on Azure, on a schedule or on demand,
with no Spark cluster to manage.
**When to use it:** any task that fits on one machine. The job runs Spark in
local mode inside one container.
**The command:**

```bash
ubunye deploy container-apps -u shop -p orders -t clean \
  --image myacr.azurecr.io/ubunye-jobs:1 -g my-rg --environment my-env \
  --registry-server myacr.azurecr.io \
  --registry-identity /subscriptions/<sub id>/resourceGroups/my-rg/providers/Microsoft.ManagedIdentity/userAssignedIdentities/pull \
  --record-out aca.json
```

Azure Container Apps runs containers for you. A Container Apps **job** is a
container that runs to the end and stops, rather than a web app that stays up.

## Before you start

1. **The Azure CLI, signed in:** `az login`. The command calls `az containerapp`
   and `az monitor log-analytics`. Add the Container Apps extension once:
   `az extension add --name containerapp`.
2. **Docker**, to build the image.
3. **In Azure:**

    | You need | Why |
    |---|---|
    | a resource group (`-g`) | holds the job |
    | a Container Apps environment (`--environment`) with a Log Analytics workspace | the job runs in it; its logs are where the run record is read back from |
    | a container registry, such as Azure Container Registry (`--registry-server`) | holds your image |
    | a user-assigned managed identity with the `AcrPull` role on that registry (`--registry-identity`) | lets the job pull the image with no password |

    Your own sign-in needs rights to create and start Container Apps jobs in
    the resource group, and to query the Log Analytics workspace.

4. **Where the data goes.** The container's disk is gone when the job ends.
   Several tasks in one launch can share it (see
   [Several tasks in one launch](serverless.md#several-tasks-in-one-launch)), but
   anything you want to keep must go to storage that outlives the container.
   Point your task's outputs there, for example with `--env`.

## 1. Build and push the image

This target builds its image with the engine's own `ubunye deploy dockerfile`
(not the companion repo's Dockerfiles). Run it in the folder that holds your
`pipelines/` folder:

```bash
ubunye deploy dockerfile container --pipelines pipelines --out Dockerfile
docker build -t myacr.azurecr.io/ubunye-jobs:1 .
az acr login -n myacr
docker push myacr.azurecr.io/ubunye-jobs:1
```

The first command prints `[OK] wrote Dockerfile and ubunye_entry.py`. The
image carries Java 17, Spark 3.5 in local mode, Delta, the engine and your
pipelines, copied to `/app/pipelines`. That is why `container-apps` takes no
`-d`: the pipelines are already inside. Rebuild and push the image when a task
changes.

## 2. Preview with `--dry-run`

```bash
ubunye deploy container-apps -u shop -p orders -t clean \
  --image myacr.azurecr.io/ubunye-jobs:1 -g my-rg --environment my-env \
  --registry-server myacr.azurecr.io \
  --registry-identity /subscriptions/0000/resourceGroups/my-rg/providers/Microsoft.ManagedIdentity/userAssignedIdentities/pull \
  --dry-run
```

Real output, with the launcher script cut to `...`:

```
{
  "platform": "container-apps",
  "job": "ubunye-clean",
  "uploads": [],
  "commands": [
    [
      "python",
      "-c",
      "...create or update the job, start it, wait, read the logs...",
      "{\"job\": \"ubunye-clean\", \"resource_group\": \"my-rg\", \"environment\": \"my-env\", \"image\": \"myacr.azurecr.io/ubunye-jobs:1\", \"cpu\": \"2\", \"memory\": \"4Gi\", \"timeout\": 1800, \"registry_server\": \"myacr.azurecr.io\", \"registry_identity\": \"/subscriptions/0000/resourceGroups/my-rg/providers/Microsoft.ManagedIdentity/userAssignedIdentities/pull\", \"env\": [\"UBUNYE_ENTRY_ARGS=[\\\"--task\\\", \\\"shop/orders/clean\\\", \\\"--mode\\\", \\\"PROD\\\"]\"], \"wait\": true}"
    ]
  ]
}

[DRY RUN] container-apps plan for 'ubunye-clean'. Nothing ran.
```

## 3. Run it

Drop `--dry-run` and add `--record-out aca.json`. The command:

1. creates the job the first time (manual trigger, no retries, one replica),
   or updates its image, size and environment after that;
2. starts one run and prints `execution <name>`;
3. with `--wait` (the default), prints the run state every 20 seconds until
   it ends;
4. reads the job's output from Log Analytics. Output reaches Log Analytics a
   few minutes after the job ends, so the command keeps asking for up to about
   13 minutes, until the whole run record is there.

| Flag | Default | What it does |
|---|---|---|
| `--job` | from the task name | the Container Apps job name |
| `--cpu` | `2` | CPU for the replica |
| `--memory` | `4Gi` | memory for the replica |
| `--timeout` | `1800` | seconds a replica may run, for all tasks together |
| `--no-wait` | | start the run and return |

Plus the shared flags `-m`, `-dt`, `--env`, `--var`, `--record-out` and
`--dry-run`: see [One command to the cloud](serverless.md#how-it-works) and the
[CLI reference](../cli.md#deploy-container-apps).

## What success looks like

The last lines name the task and each output, with its row count and data hash:

```
[OK] container-apps run success: shop/orders/clean
  <output name>: <rows> rows, <data hash>
  run record: aca.json
```

`aca.json` is a normal run record. Compare it with a local run, or another
cloud, to check the data is the same:

```bash
ubunye gate --baseline local.json --candidate aca.json
```

In the Azure portal the job is under your resource group, named
`ubunye-clean`; **Execution history** lists each run and links to its logs.

If the run fails, the line says `[FAIL]` and the command exits with a non-zero
code.

!!! note "A rare first-create failure"
    Once, creating the job failed with "unable to pull image using Managed
    identity", and the same command passed when run again. If you see it,
    check the identity has `AcrPull` on the registry, then run the command
    again.

## Scheduling

`ubunye deploy container-apps` creates a job with a manual trigger: it runs
when this command (or someone in Azure) starts it. Running it again updates
the job and starts a new run. To run on a schedule, start the command from a
scheduler you already use, such as a CI workflow on a timer. The engine does
not set a schedule on the Azure job.
