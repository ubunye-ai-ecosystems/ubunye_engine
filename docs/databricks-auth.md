# Databricks Authentication (GitHub Actions)

How to wire a Databricks workspace to this repo's CI so the
`databricks bundle` workflows can deploy and run the production examples.

Two auth flows are supported. Pick one per workspace.

| Flow | When to use | Secrets |
|---|---|---|
| **Personal Access Token (PAT)** | Free Edition, single-user workspace, quick experiments | `DATABRICKS_HOST`, `DATABRICKS_TOKEN` |
| **Service Principal + OAuth** *(recommended for paid workspaces)* | Production, shared workspace, audit-friendly, rotates via UI | `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` |

The Databricks CLI auto-detects which flow to use: if `DATABRICKS_CLIENT_ID`
and `DATABRICKS_CLIENT_SECRET` are both set, it uses OAuth M2M (machine-to-
machine). Otherwise it falls back to PAT. You don't select a flow in the
workflow — you select it by which secrets you configure.

---

## Option A — Personal Access Token (Free Edition)

Free Edition has no service principals, so PAT is the only option there.

1. In the workspace UI: **User Settings → Developer → Access tokens → Generate new token**.
2. In the GitHub repo: **Settings → Secrets and variables → Actions → New repository secret**:
    - `DATABRICKS_HOST` — e.g. `https://dbc-xxxxxxxx-xxxx.cloud.databricks.com`
    - `DATABRICKS_TOKEN` — the token from step 1.
3. Leave `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` unset.

---

## Option B — Service Principal + OAuth *(recommended)*

On paid workspaces, use a service principal so deploys are not tied to a
human user's PAT. The principal is a durable workspace identity that can be
granted only the permissions the CI needs.

### 1. Create a service principal

**Account console** (`https://accounts.cloud.databricks.com`) **→ User
management → Service principals → Add service principal**.

Note the **Application ID** that appears after creation — that is your
`DATABRICKS_CLIENT_ID`. It is a UUID, not a name.

### 2. Add the principal to the workspace

**Account console → Workspaces → *your workspace* → Permissions → Add**.
Grant the principal **User** access. (Admin access is not required for
`bundle deploy` — see step 4 for the grants you actually need.)

### 3. Generate an OAuth secret

**Workspace UI → Settings → Identity and access → Service principals →
*your principal* → Secrets → Generate secret**.

Copy the secret once — it is not shown again. That is your
`DATABRICKS_CLIENT_SECRET`.

### 4. Grant the principal workspace access

The principal needs to be able to:

- Write to the workspace files root used by DABs
  (`/Workspace/Users/<principal>/.bundle/...`).
- Deploy and run jobs.
- Write to the Unity Catalog schema and volumes the examples create.

On paid workspaces with Unity Catalog, the minimum grants are:

```sql
-- Replace <sp_id> with the service principal's Application ID.
GRANT USE CATALOG ON CATALOG main TO `<sp_id>`;
GRANT CREATE SCHEMA ON CATALOG main TO `<sp_id>`;
GRANT USE SCHEMA, CREATE TABLE, CREATE VOLUME ON SCHEMA main.titanic TO `<sp_id>`;
```

(Replace `main` / `titanic` with whatever `titanic_catalog` / `titanic_schema`
you deploy against — the example workflows default to `workspace` / `titanic`.)

### 5. Add the three secrets to GitHub

**Settings → Secrets and variables → Actions → New repository secret**:

| Secret | Value |
|---|---|
| `DATABRICKS_HOST` | e.g. `https://adb-1234567890.12.azuredatabricks.net` |
| `DATABRICKS_CLIENT_ID` | The service principal's Application ID (UUID). |
| `DATABRICKS_CLIENT_SECRET` | The OAuth secret from step 3. |

Do **not** also set `DATABRICKS_TOKEN` — if both are present, the CLI
picks OAuth, but leaving a stale PAT around defeats the point of moving off it.

---

## How the workflows consume these secrets

Every Databricks workflow in `.github/workflows/*_databricks*.yml` exports
the same env block:

```yaml
env:
  DATABRICKS_HOST:          ${{ secrets.DATABRICKS_HOST }}
  DATABRICKS_TOKEN:         ${{ secrets.DATABRICKS_TOKEN }}
  DATABRICKS_CLIENT_ID:     ${{ secrets.DATABRICKS_CLIENT_ID }}
  DATABRICKS_CLIENT_SECRET: ${{ secrets.DATABRICKS_CLIENT_SECRET }}
```

The `Check for Databricks secrets` step gates the deploy on *either* flow
being wired up:

[% raw %]
```bash
if [[ -z "${DATABRICKS_HOST}" ]]; then
  has_secrets=false
elif [[ -n "${DATABRICKS_TOKEN}" ]] || \
     [[ -n "${DATABRICKS_CLIENT_ID}" && -n "${DATABRICKS_CLIENT_SECRET}" ]]; then
  has_secrets=true
else
  has_secrets=false
fi
```
[% endraw %]

If the gate is false, the workflow runs the unit tests and portability diff
but skips `bundle validate` / `deploy` / `run`. This keeps PRs from forks
green.

---

## Verifying the wiring

1. Push a trivial change under `examples/production/titanic_databricks/`
   (e.g. edit its README) and merge it.
2. Watch the `examples/titanic_databricks` workflow. The
   `databricks bundle deploy` step logs a line like
   `Uploading bundle files to /Workspace/Users/<principal>/.bundle/...` —
   if `<principal>` is the service principal's Application ID, OAuth is
   active. If it's a human email address, the CLI fell back to PAT.
3. In the workspace UI: **Workflows → *the deployed job* → Run as** should
   show the service principal, not a human user.

---

## Rotating the OAuth secret

OAuth secrets expire (default 1 year). To rotate:

1. Generate a new secret for the same principal (step 3 above).
2. Update `DATABRICKS_CLIENT_SECRET` in GitHub.
3. Delete the old secret in the workspace UI.

The `DATABRICKS_CLIENT_ID` does not change — it's the principal's identity.

---

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `Error: default auth: cannot configure default credentials` | None of the three secrets are set, or `DATABRICKS_HOST` is blank. Confirm all three are populated under **Settings → Secrets**. |
| `401 Unauthorized` on `bundle deploy` | Secret is correct but the principal lacks workspace access. Re-check step 2 / step 4. |
| `PERMISSION_DENIED: User is not allowed to write to path /Workspace/...` | Principal has workspace access but not write access to the bundle root. Grant `WRITE` on `/Workspace/Users/<sp_id>/` or let the CLI pick a different root via `bundle deploy --var="root_path=..."`. |
| `SCHEMA_NOT_FOUND` when the job runs | Principal can deploy but not create the UC schema. Add `CREATE SCHEMA ON CATALOG <cat>` grant. |
| Deploy succeeds on Free Edition but OAuth fails | Free Edition does not support service principals. Use PAT (Option A). |
