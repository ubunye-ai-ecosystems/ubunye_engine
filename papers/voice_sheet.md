# Voice Sheet — Thabang L. Mashinini

Extracted from MSc thesis: "Learning Level Set Method by Echo State
Network for Image Segmentation" (Wits, 2022).

## Sentence structure

- **Medium-length sentences.** Typically 15–30 words. Avoids both
  terse bullet-style and dense multi-clause academic prose.
- **Parenthetical clarifications** are common: "temporal (i.e. time
  dependent)", "feedback connections (i.e. loops)". Uses "(i.e. ...)"
  liberally to bridge jargon.
- **Passive voice for methods/results**, active voice for motivations
  and aims: "RNNs were proposed..." vs. "We compare the ESN's
  performance..."

## Transitions and connectives

- Favours explicit signposting: "This issue was solved through...",
  "The advantage of X compared to Y is...", "It is in this context
  that..."
- Section conclusions use "In summary" or restate the section's
  contribution directly.
- Lists are introduced with a colon and use bullet points (not
  numbered), often with bold lead-ins ("BPTT:", "RTRL:").

## Formality register

- **Formal academic** but accessible. Does not use contractions in
  the body (uses them in acknowledgements: "I'm", "I'd").
- Defines acronyms on first use, then uses them consistently.
- Occasionally quotes other authors directly (block-quoted) to anchor
  claims in prior work.

## Hedging and certainty

- Moderate hedging: "may", "could be able to", "remains promising".
- Stronger claims when backed by direct comparison: "The advantage
  of RNNs compared to FNNs is their ability to..."
- Does not overclaim — frames contributions as objectives ("to
  compare", "to investigate") rather than declarations.

## Preferred section structures

- **Introduction**: problem statement → motivation → existing
  approaches → their limitations → proposed approach → aims →
  objectives → research questions → hypothesis → contributions.
- **Literature review**: per-topic subsections, each with
  Introduction → Formalism → Conclusion.
- **Methodology**: data generation → data representation → model
  architecture → evaluation metrics → training procedure.
- **Results**: per-dataset subsections with Introduction → Results
  and discussion → Conclusion.

## Voice characteristics to preserve

1. Explain jargon inline with "(i.e. ...)" rather than assuming the
   reader knows.
2. State the problem before the solution — always motivate first.
3. Use comparative framing: "unlike X, Y does..." / "the advantage
   of X compared to Y".
4. Ground claims in specific metrics and direct comparisons.
5. Keep paragraphs focused — one idea per paragraph, rarely more
   than 6 sentences.
6. Formal but warm — the acknowledgements show personality; the
   body is professional but never cold.

## Adaptation notes for a systems paper

- The MSc is a neural-networks thesis; the Ubunye paper is a
  software-engineering experience report. Adapt:
  - "Formalism" → "Architecture" or "Design"
  - "Evaluation metrics" → "Bug catalogue" / "Empirical study"
  - "Results and discussion" per dataset → per production example
- Preserve the (i.e. ...) habit, the motivate-first pattern, and
  the comparative framing against other frameworks.
