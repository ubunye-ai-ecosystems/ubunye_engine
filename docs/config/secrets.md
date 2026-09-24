# Secrets

A config names a secret; it never holds one.

```yaml
CONFIG:
  inputs:
    orders:
      format: jdbc
      url: "jdbc:postgresql://db.internal:5432/shop"
      table: public.orders
      user: reporting
      password: "secret://aws-sm/prod/shop-db#password"
```

The value is fetched only at the moment a connector reads or writes, and only
into the copy of the config that connector receives. Everywhere else the
reference stays a reference: the config hash, `ubunye plan`, `ubunye config`,
run records and logs. So a secret cannot leak through a printed plan or a run
record, and rotating a password does not change the config hash.

## Providers

| Reference | Where the secret lives | Install |
|---|---|---|
| `secret://env/NAME` | an environment variable | nothing |
| `secret://file/PATH` | a file, e.g. a mounted secret: `secret://file//run/secrets/db` | nothing |
| `secret://databricks/SCOPE/KEY` | a Databricks secret scope (`dbutils` on a cluster, the Databricks SDK elsewhere) | `ubunye-engine[databricks]` off-cluster |
| `secret://aws-sm/NAME` | AWS Secrets Manager | `ubunye-engine[aws]` |
| `secret://gcp-sm/PROJECT/SECRET[/VERSION]` | Google Secret Manager (latest version by default) | `ubunye-engine[gcp]` |
| `secret://azure-kv/VAULT/SECRET[/VERSION]` | Azure Key Vault | `ubunye-engine[azure]` |

For a JSON secret, `#field` picks one field: `secret://aws-sm/prod/shop-db#password`.

The cloud providers log in the way each cloud's own SDK does (a profile, a role,
workload identity, a managed identity, environment variables). The engine never
takes a credential itself.

The whole value must be the reference; a secret is not spliced into a longer
string. Put a password in its own field (`password:`), not inside a URL.

## Checked before a run, fetched during it

`ubunye validate` refuses a reference to a provider that is not installed, with
the closest name (`secret://aws-smm/...`: did you mean `aws-sm`?). Nothing is
fetched at validation.

`ubunye doctor -d ... -t TASK` lists each provider a task uses and whether its
package is installed. It does not read the secrets: the login they need may only
exist where the task really runs.

A secret that cannot be read stops the run with the reference and the provider's
error, never the value:

```text
SecretError: Could not read secret secret://aws-sm/prod/shop-db: ClientError: AccessDeniedException ...
  Provider:  aws-sm
  Reference: prod/shop-db
  Hint: pip install 'ubunye-engine[aws]'; the login is the AWS SDK's (profile, role, env).
```

## Writing a provider

A provider is a class with `get(ref) -> str`, registered under the
`ubunye.secrets` entry point group; its entry point name is the provider name in
`secret://NAME/...`. Declare `REQUIRES_MODULES = (("module", "pip-package"), ...)`
so `ubunye doctor` can say what is missing.

```toml
[project.entry-points."ubunye.secrets"]
vault = "my_package.secrets:HashiCorpVault"
```

A notebook that calls a connector itself resolves the config first:
`from ubunye.core.secrets import resolve`, then `reader.read(resolve(cfg), backend)`.
