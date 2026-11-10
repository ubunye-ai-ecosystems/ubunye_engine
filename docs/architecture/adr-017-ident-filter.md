# ADR 017: A SQL name from a variable is quoted by one built-in filter

**Status:** accepted, 0.9.2

## Context

Databricks catalogs are often named with hyphens (`aws-x-catalog`). In SQL such a
name must be in backticks, or `aws-x-catalog.raw.trips` is read as a subtraction.
Teams pass the catalog as a job parameter or an environment variable and template
it into `config.yaml`. Some parameters carry the backticks, some do not, so a
config that writes the backticks itself breaks on the first kind (two sets of
backticks) and a config that does not breaks on the second. Teams worked around it
with chains of `replace` filters, written differently in every task.

## Decision

The config resolver registers one filter of its own, `ident`, next to Jinja's
built-in ones. It takes a name with or without backticks, drops surrounding
whitespace, splits it on dots (a dot inside backticks belongs to the name), and
quotes each part once, writing a backtick inside a name doubled the way Spark SQL
reads it. Quoting a quoted name changes nothing. An empty name or an empty part
(`a..b`) raises when the config loads, naming the value.

It lives in `ubunye/config/resolver.py`, not in the core: it is a template
helper, and it uses Spark SQL's quoting (backticks), which Databricks, Spark and
Hive share. A connector whose SQL quotes with double quotes (JDBC to Postgres)
should not use it; that would be a second filter if anyone needs it.

`ubunye validate` and `ubunye plan` print each SQL input after templating, so the
quoted name can be checked before a run.

## Consequences

- One spelling in every config: `{{ env.NAME | ident }}`, documented on the
  Jinja page, the Unity connector page and the Databricks deployment page.
- The filter is pure and has no state; it is tested with hyphens, with and without
  backticks, dotted names, whitespace and the refused cases.
- `validate --json` gained a `sql` key per task. A key was added, none changed.
