---
description: Record a finding in the hardening ledger, with evidence
argument-hint: <one-line summary>
---

Record a finding: $ARGUMENTS

1. Next free number: the highest `F-NNN` in `tasks/hardening/findings/` plus one.
2. Create `tasks/hardening/findings/F-NNN-<short-slug>.md` from the template in
   `tasks/hardening/README.md`. Fill every field. The repro must be something another
   person can run; the evidence is exact output, not a paraphrase.
3. Severity: `blocker` (wrong data, lost data, a broken promise), `major` (a newcomer
   cannot proceed without reading source), `minor` (friction with a clear workaround).
4. Say which promise it touches, if any (see the list in `tasks/hardening/README.md`).
5. Do not fix it in the same step. Filing and fixing are separate.
