# Security Policy

## Supported versions

Security fixes go into the newest release only. Upgrade to get them.

| Version | Supported |
|---------|-----------|
| 0.8.x   | Yes       |
| < 0.8   | No        |

## Reporting a vulnerability

Please do not open a public issue for a security problem.

1. Report it privately through GitHub: open the repository's **Security** tab and
   choose **Report a vulnerability**
   ([how private reporting works](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-security-advisories/privately-reporting-a-security-vulnerability)).
2. If that is not possible, email **uaie@gmail.com**.
3. Tell us:
   - the steps to reproduce it,
   - the Ubunye version, Python version and platform (laptop, Spark cluster,
     Databricks, a cloud),
   - what an attacker could do with it.

We will acknowledge your report within 48 hours and give you a status update
within 5 business days. We will agree a disclosure date with you, and credit you
in the changelog unless you ask us not to.

## What counts

Report it if Ubunye itself can be made to:

- reveal a secret, for example in a log, a plan, a run record or an error message
  (a config should only ever hold a `secret://` reference, see
  [Secrets](docs/config/secrets.md));
- run code or read files that the config and the task folder did not ask for;
- write outside the outputs a config declares;
- let an LLM step or the MCP server do more than its configuration allows.

These are not Ubunye vulnerabilities:

- A `transformations.py` doing something harmful. A task folder is code, and
  running it runs that code. Only run task folders you trust.
- A problem in Spark, pandas, a cloud SDK or another dependency. Report it to that
  project. Tell us too if Ubunye makes it worse.

## Keeping your own pipelines safe

- Run the newest release: `pip install -U ubunye-engine`.
- Keep Python, Java and Spark patched.
- Never put a password or token in a config. Use a `secret://` reference to an
  environment variable, a mounted file or a secret manager.
- Give a pipeline's credentials only the tables and buckets it needs.
- Turn on telemetry hooks only where your logs and metrics are stored safely.
