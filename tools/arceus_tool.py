#!/usr/bin/env python3
"""
Model-facing tool + deterministic routing helpers for Arceus.

``arceus_delegate`` is the tool the primary model ("Luna") calls to hand a
task brief to the local ``claude`` CLI instead of doing the heavy work (or
deep repository exploration) itself. The tool dispatches and returns
immediately — the actual result re-enters the conversation later as a new
turn once the ``claude`` subprocess exits (see ``tools/arceus.py``).

This module also exposes small, pure, dependency-free classifier functions
(``classify_task_for_arceus`` / ``detect_user_override``) used both by the
tool description (steering the model toward early delegation) and directly
by tests, so the routing rules are independently verifiable without needing
a live model.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional

from tools.registry import tool_error

# ---------------------------------------------------------------------------
# Deterministic routing helpers
# ---------------------------------------------------------------------------

# Task types that MUST route to Arceus per the token-offload spec: all
# artifact/document generation, plus substantial coding work.
DOCUMENT_TASK_TYPES = {
    "document", "docx", "pdf", "xlsx", "spreadsheet", "pptx", "presentation",
    "artifact", "csv",
}
CODING_TASK_TYPES = {"complex_coding", "repository_heavy_work"}

_OVERRIDE_USE_PHRASES = (
    "use arceus", "use claude", "have claude code handle this",
    "have arceus handle this", "delegate this to arceus", "delegate this to claude",
)
_OVERRIDE_SKIP_PHRASES = (
    "keep this on luna", "don't use arceus", "do not use arceus",
    "don't use claude", "do not use claude", "no arceus", "stay on luna",
)

# Keyword families used for early (pre-investigation) routing hints. These
# intentionally err toward recall for artifact/document requests (section 7
# of the spec: ALL artifact/document creation goes to Arceus) and toward
# precision for coding (avoid yanking trivial one-line fixes off Luna).
_DOCUMENT_KEYWORDS = (
    "docx", "word document", ".pdf", "pdf report", "xlsx", "spreadsheet",
    "pptx", "slide deck", "presentation deck", "proposal", "quotation",
    "quote for", "estimate for", "scope of work", "bid sheet", "invoice",
    "calculation sheet", "customer deliverable", "convert this file",
)
_CODING_KEYWORDS = (
    "implement", "refactor", "migrate", "migration", "fix the bug",
    "debug why", "add a feature", "build a feature", "write tests for",
    "multi-file", "across the codebase", "architecture change",
    "integrate with", "database schema", "performance investigation",
)


def detect_user_override(text: str) -> Optional[bool]:
    """Return True (force Arceus), False (force Luna/prohibit Arceus), or
    None (no explicit override) based on user phrasing. Native routing
    consults this before any keyword/task-type heuristic."""
    if not text:
        return None
    lowered = text.lower()
    if any(p in lowered for p in _OVERRIDE_SKIP_PHRASES):
        return False
    if any(p in lowered for p in _OVERRIDE_USE_PHRASES):
        return True
    return None


def classify_task_for_arceus(
    text: str, *, task_type: Optional[str] = None,
) -> Optional[str]:
    """Return a normalized Arceus task_type string, or None to leave routing
    to the model's own judgment.

    Order of precedence: explicit user override > explicit task_type >
    document keywords > coding keywords > None.
    """
    override = detect_user_override(text or "")
    if override is False:
        return None
    if task_type:
        normalized = task_type.strip().lower()
        if normalized in DOCUMENT_TASK_TYPES or normalized in CODING_TASK_TYPES:
            return normalized

    lowered = (text or "").lower()
    if any(k in lowered for k in _DOCUMENT_KEYWORDS):
        return "document"
    if override is True or any(k in lowered for k in _CODING_KEYWORDS):
        return "complex_coding"
    return None


# ---------------------------------------------------------------------------
# Task-brief -> claude CLI prompt
# ---------------------------------------------------------------------------
_ENVELOPE_INSTRUCTIONS = """
When you finish, end your final message with a compact machine-readable
status block so the caller does not need to re-read your full output:

STATUS: PASS or FAIL
CHANGED: <short list of changed files, or "none">
VALIDATION: <one line per check you ran, e.g. "tests PASS", "typecheck PASS">
ARTIFACTS: <paths of any created/modified deliverable files, or "none">
SUMMARY: <1-3 sentences>

If — and only if — you cannot proceed without more information from the
user, do NOT guess. Instead end your ENTIRE response with exactly one line
in this form (nothing after it):

ARCEUS_NEEDS_INPUT: <the precise question you need answered>

Run the relevant tests/lint/typecheck/build yourself and fix failures
before returning where reasonable (bounded correction effort — do not loop
forever). Do not ask the caller clarifying questions unless truly blocked.
""".strip()


def build_task_prompt(
    task_brief: str,
    *,
    task_type: str = "complex_coding",
    working_directory: Optional[str] = None,
) -> str:
    """Compose the full non-interactive ``claude -p`` prompt for one job."""
    parts = [task_brief.strip()]
    if working_directory:
        parts.append(f"\nWork inside: {working_directory}")
    parts.append("\n---\n" + _ENVELOPE_INSTRUCTIONS)
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Session-context capture (mirrors tools/delegate_tool.py's background path)
# ---------------------------------------------------------------------------
def _capture_routing_context(parent_agent: Any) -> Dict[str, Any]:
    session_key = ""
    origin_ui_session_id = ""
    origin_session_id = ""
    try:
        from tools.approval import get_current_session_key

        session_key = get_current_session_key(default="")
    except Exception:
        session_key = ""
    try:
        from gateway.session_context import get_session_env

        source = get_session_env("HERMES_SESSION_SOURCE", "")
        origin_ui_session_id = get_session_env("HERMES_UI_SESSION_ID", "")
        if source == "tui":
            agent_session_id = str(getattr(parent_agent, "session_id", "") or "")
            if agent_session_id:
                session_key = agent_session_id
        if source == "api_server":
            origin_session_id = get_session_env("HERMES_SESSION_CHAT_ID", "") or ""
    except Exception:
        pass
    if not session_key:
        agent_session_id = str(getattr(parent_agent, "session_id", "") or "")
        if agent_session_id:
            session_key = agent_session_id
    parent_session_id = getattr(parent_agent, "session_id", None)
    return {
        "session_key": session_key,
        "origin_ui_session_id": origin_ui_session_id,
        "origin_session_id": origin_session_id,
        "parent_session_id": parent_session_id,
    }


# ---------------------------------------------------------------------------
# Tool schema + handler
# ---------------------------------------------------------------------------
ARCEUS_DELEGATE_SCHEMA = {
    "name": "arceus_delegate",
    "description": (
        "Hand a substantial coding task or ANY document/artifact creation "
        "(DOCX, PDF, XLSX, PPTX, CSV, proposals, reports, spreadsheets, "
        "presentations, conversions) to Arceus — the local, subscription-"
        "authenticated Claude Code CLI running as a background worker. "
        "Call this EARLY, before doing your own deep repository exploration "
        "or drafting the document content yourself: Arceus owns "
        "investigation, implementation, content generation, file "
        "generation, and its own testing/QA. Returns immediately with a "
        "job handle; the result re-enters this conversation as a new "
        "message once Arceus finishes (or times out / fails / needs more "
        "information from the user) — you do not need to wait or poll."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "task_brief": {
                "type": "string",
                "description": (
                    "A SMALL task brief in TASK/SOURCE/REQUIREMENTS/OUTPUT/"
                    "VALIDATION form. Do not pre-investigate the repository "
                    "or pre-draft document content — Arceus does that."
                ),
            },
            "task_type": {
                "type": "string",
                "enum": sorted(CODING_TASK_TYPES | DOCUMENT_TASK_TYPES | {"other"}),
                "description": "What kind of work this is; used for routing/status display.",
            },
            "working_directory": {
                "type": "string",
                "description": "Optional working directory for Arceus (defaults to the current repo/session directory).",
            },
            "timeout_minutes": {
                "type": "integer",
                "description": "Optional override of arceus.timeout_minutes for this job.",
                "minimum": 1,
            },
        },
        "required": ["task_brief"],
    },
}


def _handle_arceus_delegate(args: dict, **kw) -> str:
    parent_agent = kw.get("parent_agent")
    task_brief = (args.get("task_brief") or "").strip()
    if not task_brief:
        return tool_error("arceus_delegate requires a non-empty 'task_brief'.")

    task_type = (args.get("task_type") or "complex_coding").strip().lower()
    working_directory = args.get("working_directory") or None
    timeout_minutes = args.get("timeout_minutes")
    timeout_seconds = float(timeout_minutes) * 60.0 if timeout_minutes else None

    from tools.arceus import launch_arceus_job

    ctx = _capture_routing_context(parent_agent)
    prompt = build_task_prompt(task_brief, task_type=task_type, working_directory=working_directory)
    parent_task_id = str(getattr(parent_agent, "session_id", "") or "") or None

    handle = launch_arceus_job(
        task_brief=prompt,
        task_type=task_type,
        working_directory=working_directory,
        timeout_seconds=timeout_seconds,
        parent_task_id=parent_task_id,
        **ctx,
    )
    if handle.status == "rejected":
        return tool_error(handle.error or "Arceus could not accept this job.")

    return json.dumps(
        {
            "status": handle.status,
            "job_id": handle.job_id,
            "state": handle.state,
            "queue_position": handle.queue_position,
            "note": (
                "Arceus is handling this in the background. Tell the user "
                "something short like 'Arceus is handling the "
                "implementation/document now' and do NOT wait or poll for "
                "the result — it will arrive as a new message when Arceus "
                "finishes."
            ),
        },
        ensure_ascii=False,
    )


def register(ctx) -> None:  # noqa: ARG001 — invoked by plugins/arceus/__init__.py
    ctx.register_tool(
        name="arceus_delegate",
        toolset="arceus",
        schema=ARCEUS_DELEGATE_SCHEMA,
        handler=_handle_arceus_delegate,
        emoji="🧠",
    )
