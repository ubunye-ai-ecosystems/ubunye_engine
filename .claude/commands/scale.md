---
description: Run one step of the scale ladder next to a plain baseline
argument-hint: <pipeline> <data size> [where: actions|kaggle|databricks]
---

Use the `scale-runner` agent for: $ARGUMENTS

Free compute only unless the owner approved a budget for this step. The result is not a
result without the plain baseline (the same job in plain Spark or pandas, same data,
same machine, three repeats). Record it in `tasks/hardening/SCOREBOARD.md`; file a
finding for anything that pulls data to one machine, runs out of memory, or grows
Ubunye's overhead with size.
