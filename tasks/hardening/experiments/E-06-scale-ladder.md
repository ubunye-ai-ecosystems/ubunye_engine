# E-06: Does Ubunye's overhead stay small as data grows, and does anything pull data to one machine?

**Status:** planned
**Why:** The owner's first requirement: scalable, efficient, distributed. The run record's content hash is the first suspect (pandas: about 0.78 s per 60,000 rows).

## Method
The same job through Ubunye and in plain Spark or pandas at growing sizes on free compute (GitHub Actions, Kaggle notebooks, Databricks Free Edition), on the 2018-2022 flights and generated TPC-H style data. Three repeats each.

## Pass means
Overhead under an agreed bound at every size, flat or falling with size; no step whose time or memory grows on one machine while the backend is distributed.

## Result
Not run yet.
