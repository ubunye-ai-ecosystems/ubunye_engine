"""Label each product review positive or negative with a language model.

The model is reached through ``ubunye.llm``, so every call is in the run record,
can be capped, and can be replayed. In CI the calls replay from the committed
``llm-replay.jsonl`` next to this file: no key, no network, the same rows every
time, on every operating system.

The committed answers were recorded from a stub model (``scripts/stub_model.py``),
not a real one, so the example proves the replay machinery, not a model's labels.
Point ``LLM_BASE_URL`` and ``LLM_MODEL`` at a real OpenAI-compatible server and run
once with ``UBUNYE_LLM_MODE=record`` to record real answers.
"""

import os

from ubunye import llm
from ubunye.core.interfaces import Task

PROMPT = "Is this product review positive or negative? Answer with one word.\n\n{review}"


class LabelReviews(Task):
    def setup(self):
        self.model = llm.port(
            "openai_compatible",
            model=os.environ.get("LLM_MODEL", "stub-labeller"),
            # Nothing listens here: a replayed run never calls out.
            base_url=os.environ.get("LLM_BASE_URL", "http://127.0.0.1:9/v1"),
        )

    def transform(self, sources):
        reviews = sources["reviews"].copy()
        prompts = [PROMPT.format(review=text) for text in reviews["review"]]
        answers = self.model.complete_many(prompts, max_tokens=5, temperature=0)
        reviews["label"] = [a.text.strip().lower() for a in answers]
        return {"labelled": reviews}
