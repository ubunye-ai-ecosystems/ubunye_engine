# Databricks Community Edition

Most of the Databricks examples target **Community Edition**, so they run on a free
workspace with no cloud spend. That choice costs something, and this page is the
honest accounting of it: what CE forces you to do differently, and what to undo when
you move to a paid workspace.

The content below is the single source of truth — it is included straight from
[`examples/production/README.md`](https://github.com/ubunye-ai-ecosystems/ubunye_engine/blob/main/examples/production/README.md),
so it cannot drift from the examples it describes.

## Which examples run on CE?

| Example | Community Edition | Paid workspace |
|---|---|---|
| [Titanic (Databricks)](titanic-databricks.md) | :material-check: yes | :material-check: yes |
| [Titanic ML (Databricks)](titanic-ml-databricks.md) | :material-check: yes | :material-check: yes |
| [Titanic Multi-Task (Databricks)](titanic-multitask-databricks.md) | :material-check: yes | :material-check: yes |
| [JHB Weather (Databricks)](jhb-weather.md) | :material-check: yes | :material-check: yes |
| [Flood Risk (Databricks)](flood-risk.md) | :material-close: **no** — reads existing UC tables | :material-check: yes |
| [Device Mapping ETL (Databricks)](device-mapping.md) | :material-close: **no** — reads 3 existing UC tables | :material-check: yes |
| [Titanic (Local)](titanic-local.md) | n/a — no Databricks needed | n/a |

---

--8<-- "examples/production/README.md:community-edition"
