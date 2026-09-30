"""Arceus plugin — wires the token-offload worker into Hermes' native
plugin surface (see the design note in ``tools/arceus.py``).

Registers:
  - the ``arceus_delegate`` model tool (``tools/arceus_tool.py``)
  - the ``/arceus-status`` slash command
  - restart reconciliation (runs once, synchronously, at plugin load —
    covers both CLI and gateway processes) and the background completion
    watchdog (the secondary reconciliation path required alongside process
    exit as the primary completion signal)
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_STARTUP_DONE = False


def _run_startup_reconciliation() -> None:
    global _STARTUP_DONE
    if _STARTUP_DONE:
        return
    _STARTUP_DONE = True
    try:
        from tools.arceus import get_engine

        engine = get_engine()
        if not engine._enabled():  # noqa: SLF001 — internal, read-only check
            return
        engine.reconcile_on_startup()
        engine.start_watchdog()
    except Exception:
        logger.warning("Arceus startup reconciliation/watchdog failed to start", exc_info=True)


def _handle_status_command(raw_args: str) -> str:  # noqa: ARG001
    from tools.arceus import format_status_report, status_snapshot

    try:
        return format_status_report(status_snapshot())
    except Exception as exc:
        return f"ARCEUS STATUS\n\nCould not read Arceus state: {exc}"


async def _on_gateway_startup(event_type: str, context: dict) -> None:  # noqa: ARG001
    _run_startup_reconciliation()


def register(ctx) -> None:
    try:
        import tools.arceus_tool as arceus_tool

        arceus_tool.register(ctx)
    except Exception:
        logger.warning("Arceus tool registration failed", exc_info=True)

    ctx.register_command(
        "arceus-status",
        handler=_handle_status_command,
        description="Show Arceus (Claude Code CLI worker) status: enabled, running/queued jobs, watchdog, last result.",
    )
    ctx.register_hook("gateway:startup", _on_gateway_startup)

    # Also reconcile immediately at plugin load — covers the CLI, which has
    # no "gateway:startup" event, and makes gateway startup reconciliation
    # happen as early as possible rather than waiting on the full platform
    # connect sequence.
    _run_startup_reconciliation()


def _reset_for_tests() -> None:
    """Test-only: allow re-running startup reconciliation in-process."""
    global _STARTUP_DONE
    _STARTUP_DONE = False
