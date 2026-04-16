# task-10 — Write a research/experience paper on Ubunye Engine

**Priority:** low (do after 0.1.7 ships and fire-test war stories exist)
**Blocked by:** `todo/task-00.md` (fire-test sprint) — paper needs real
production data to cite, not speculation.

## Framing

Not a "new algorithm" paper. Frame as **systems / experience report** or
**tool demo**. Plausible venues:

- SoftwareX (open-access, accepts research software papers)
- EDBT / ICDE demo track
- SIGMOD industry / experience track
- arXiv preprint for early visibility

Do **not** pitch as "yet another ETL framework" — Kedro, Flyte, Metaflow,
Dagster, Mage, dbt all occupy that space. The contribution has to be
sharper.

## Candidate anchors (what's actually novel-ish)

1. **CI-enforced portability contract** — byte-identical
   `transformations.py` across runtimes as a build-time invariant, not a
   documentation promise. Surveyed frameworks don't formalise this.
2. **Unified Task/Transform dispatch** through a single Engine + Hook
   observability abstraction. Collapses the interactive-vs-batch split
   most frameworks carry.
3. **Config-first + Jinja + Pydantic + plugin entry points** as a design
   stack — worth analysing the trade-offs vs. code-first (Dagster,
   Prefect) and template-first (dbt).
4. **Empirical**: fire-test sprint data — what broke when taken to
   production on CE, serverless, UC, REST, ML lifecycle. The bug
   catalogue + root causes is the paper's evidence base.

## Work plan (when unblocked)

1. Run the fire-test sprint (`todo/task-00.md`). Keep a bug catalogue
   with: symptom, root cause, fix SHA, runtime where it surfaced.
2. Related-work survey — Kedro, Flyte, Metaflow, Dagster, Prefect, dbt,
   Mage, Apache Beam. Which occupy which quadrant of
   config-first/code-first × portable/runtime-locked.
3. Draft outline: motivation → contract design → architecture →
   empirical study → limitations → related work.
4. Pull writing-style sample from the user's MSc thesis at
   <https://wiredspace.wits.ac.za/items/2c23f3d9-05fd-410e-ad52-31ecffbbf643>
   to build a voice sheet before drafting.
5. Draft in LaTeX (MiKTeX is installed on the machine at
   `C:\Users\Administrator\AppData\Local\Programs\MiKTeX\miktex\bin\x64`).

## Non-goals

- Don't claim novelty in the config-driven ETL paradigm itself.
- Don't benchmark against other frameworks on raw throughput — that's
  Spark's job, not Ubunye's, and the comparison will mislead reviewers.
- Don't start drafting before the fire-test sprint is done.
