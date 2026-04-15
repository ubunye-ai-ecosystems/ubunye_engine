"""Engine observation hooks.

A ``Hook`` observes a task run without participating in its logic. The engine
wraps each task and each step in the hook's context managers; hooks see
start, end, and any exception that propagated out of the block.

Hooks must not raise in ``__exit__`` — catch your own errors. The engine does
not swallow hook exceptions.
"""

from __future__ import annotations

from contextlib import ExitStack, contextmanager
from typing import TYPE_CHECKING, Any, Dict, Iterable, Iterator, List, Optional

if TYPE_CHECKING:
    from ubunye.core.runtime import EngineContext


class Hook:
    """Base class for engine hooks. Default methods are no-ops.

    Subclasses override :meth:`task` and/or :meth:`step` to observe runs.
    Both are context managers:

    - ``__enter__`` runs before the operation.
    - ``__exit__`` runs after; if the operation raised, ``exc_type`` is set.

    The ``state`` dict passed to :meth:`task` is a shared scratchpad the engine
    populates during the run (e.g. ``state['outputs']``). Hooks that don't
    need it can ignore the argument.
    """

    @contextmanager
    def task(
        self,
        ctx: "EngineContext",
        cfg: Dict[str, Any],
        state: Dict[str, Any],
    ) -> Iterator[None]:
        yield

    @contextmanager
    def step(
        self,
        ctx: "EngineContext",
        name: str,
        meta: Optional[Dict[str, Any]] = None,
    ) -> Iterator[None]:
        yield


class HookChain:
    """Combine many hooks into one. Enters in order, exits in reverse order."""

    def __init__(self, hooks: Iterable[Hook]) -> None:
        self.hooks: List[Hook] = list(hooks)

    @contextmanager
    def task(
        self,
        ctx: "EngineContext",
        cfg: Dict[str, Any],
        state: Dict[str, Any],
    ) -> Iterator[None]:
        with ExitStack() as stack:
            for h in self.hooks:
                try:
                    stack.enter_context(h.task(ctx, cfg, state))
                except Exception:
                    # Broken hook must not break the run
                    pass
            yield

    @contextmanager
    def step(
        self,
        ctx: "EngineContext",
        name: str,
        meta: Optional[Dict[str, Any]] = None,
    ) -> Iterator[None]:
        with ExitStack() as stack:
            for h in self.hooks:
                try:
                    stack.enter_context(h.step(ctx, name, meta))
                except Exception:
                    pass
            yield
