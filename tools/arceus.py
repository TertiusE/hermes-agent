#!/usr/bin/env python3
"""
Arceus — Hermes' local Claude Code CLI token-offload worker.

Arceus lets Hermes hand heavy engineering / document-authoring work to the
official, subscription-authenticated ``claude`` CLI instead of burning
primary-model (GPT-5.6 "Luna") tokens on deep repository exploration or long
implementation loops. It deliberately reuses existing Hermes machinery
instead of building a parallel task framework:

  - Delivery of the final result back into the originating chat reuses the
    SAME rail as ``delegate_task(background=true)``
    (``tools.process_registry.process_registry.completion_queue`` +
    the gateway's ``_async_delegation_watcher`` / CLI's idle drain), by
    emitting a ``type="async_delegation"``-shaped completion event. No
    gateway/CLI changes are required for delivery to reach Discord,
    Telegram, the TUI, or the API server.
  - Persistence lives in the same ``state.db`` SQLite file Hermes already
    uses (``hermes_constants.get_hermes_home() / "state.db"``), following
    the exact WAL/connection idiom used by ``tools/async_delegation.py``.

What Arceus owns that ``async_delegation`` does NOT provide, because the
"child" here is an independent OS subprocess (the ``claude`` CLI) rather
than an in-process Python thread that dies with the parent:

  - An explicit lifecycle: QUEUED -> STARTING -> RUNNING -> one of
    SUCCEEDED / FAILED / TIMED_OUT / CANCELLED / NEEDS_USER_INPUT.
  - A durable ``arceus_jobs`` registry keyed by job id, tracking the
    *child's own* PID (+ start time, to guard against PID reuse) so a
    freshly restarted Hermes process can tell whether the subprocess is
    still alive, already finished, or vanished with no result.
  - A dedicated concurrency=1 admission queue (configurable), independent
    of the shared ``delegation.max_concurrent_children`` pool used by
    ordinary ``delegate_task`` subagents.
  - Two independent completion paths that both funnel through ONE atomic,
    idempotent finalize function: (1) the primary path — the owning
    thread's ``proc.wait()`` returning — and (2) a periodic watchdog sweep
    that reconciles jobs whose primary callback never fired (crash, missed
    event, or a Hermes restart mid-job).

Zero recurring model calls happen while Arceus works: everything above is
plain Python/thread/subprocess/SQLite bookkeeping. The primary model is
only invoked once to dispatch the job (a tool call) and once more, cheaply,
when the completion event re-enters the conversation as a new turn.
"""

from __future__ import annotations

import contextlib
import enum
import json
import logging
import os
import sqlite3
import subprocess
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List, Optional, Set, Tuple

from hermes_constants import get_hermes_home

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------
class ArceusState(str, enum.Enum):
    QUEUED = "QUEUED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    CANCELLED = "CANCELLED"
    NEEDS_USER_INPUT = "NEEDS_USER_INPUT"


TERMINAL_STATES: Set[str] = {
    ArceusState.SUCCEEDED.value,
    ArceusState.FAILED.value,
    ArceusState.TIMED_OUT.value,
    ArceusState.CANCELLED.value,
    ArceusState.NEEDS_USER_INPUT.value,
}

NON_TERMINAL_STATES: Set[str] = {
    ArceusState.QUEUED.value,
    ArceusState.STARTING.value,
    ArceusState.RUNNING.value,
}

_DEFAULT_TIMEOUT_MINUTES = 30
_DEFAULT_HEARTBEAT_SECONDS = 20
_DEFAULT_WATCHDOG_SECONDS = 30
_DEFAULT_MAX_CONCURRENT = 1
_TERMINATE_GRACE_SECONDS = 8
_OUTPUT_TAIL_CHARS = 4000
_NEEDS_INPUT_MARKER = "ARCEUS_NEEDS_INPUT:"

# Any RUNNING/STARTING job's claude-side PID whose owning Hermes process has
# not touched it in this long is presumed abandoned even if the PID happens
# to still resolve to a live process (defensive backstop for PID reuse /
# unreadable /proc). Mirrors kanban_db's backstop reclaim window.
_HEARTBEAT_ABANDON_SECONDS = 3600


def _default_config() -> Dict[str, Any]:
    """Best-effort read of the ``arceus`` config block; safe fallback."""
    try:
        from hermes_cli.config import load_config

        cfg = load_config() or {}
    except Exception:
        cfg = {}
    arceus_cfg = cfg.get("arceus") if isinstance(cfg, dict) else None
    return arceus_cfg if isinstance(arceus_cfg, dict) else {}


def _pid_helpers() -> Tuple[Callable[[int], bool], Callable[[int], Optional[int]]]:
    try:
        from gateway.status import _pid_exists, get_process_start_time

        return _pid_exists, get_process_start_time
    except Exception:  # pragma: no cover — gateway.status always importable in practice
        def _fallback_exists(pid: int) -> bool:
            try:
                os.kill(pid, 0)
                return True
            except Exception:
                return False

        return _fallback_exists, (lambda pid: None)


@dataclass
class ArceusJobHandle:
    job_id: str
    status: str  # "queued" | "started" | "rejected"
    state: str
    queue_position: int = 0
    error: Optional[str] = None


class ArceusEngine:
    """Owns Arceus's registry, admission queue, subprocess execution and
    watchdog. A module-level singleton (see bottom of file) backs normal
    operation; tests construct their own instance with fakes injected."""

    def __init__(
        self,
        *,
        db_path: Optional[Path] = None,
        popen_factory: Callable[..., "subprocess.Popen"] = subprocess.Popen,
        pid_exists_fn: Optional[Callable[[int], bool]] = None,
        get_process_start_time_fn: Optional[Callable[[int], Optional[int]]] = None,
        push_event_fn: Optional[Callable[[Dict[str, Any]], None]] = None,
        now_fn: Callable[[], float] = time.time,
        sleep_fn: Callable[[float], None] = time.sleep,
        config_fn: Callable[[], Dict[str, Any]] = _default_config,
        log_dir: Optional[Path] = None,
    ) -> None:
        self._db_path_override = db_path
        self._popen_factory = popen_factory
        self._now = now_fn
        self._sleep = sleep_fn
        self._config_fn = config_fn
        self._log_dir_override = log_dir

        if pid_exists_fn is None or get_process_start_time_fn is None:
            default_pid_exists, default_start_time = _pid_helpers()
            pid_exists_fn = pid_exists_fn or default_pid_exists
            get_process_start_time_fn = get_process_start_time_fn or default_start_time
        self._pid_exists = pid_exists_fn
        self._get_process_start_time = get_process_start_time_fn
        self._push_event = push_event_fn or self._default_push_event

        self._admission_lock = threading.Lock()
        self._running_job_id: Optional[str] = None
        self._queue: Deque[str] = deque()

        self._threads_lock = threading.Lock()
        self._threads: Dict[str, threading.Thread] = {}
        self._cancel_flags_lock = threading.Lock()
        self._cancel_flags: Set[str] = set()
        self._processes_lock = threading.Lock()
        self._processes: Dict[str, "subprocess.Popen"] = {}

        # Jobs recovered at startup whose claude-side PID may still be alive
        # (owning in-process thread is gone — a fresh Hermes process cannot
        # rejoin an old ThreadPoolExecutor worker). The watchdog is the ONLY
        # mechanism that finalizes these.
        self._orphans_lock = threading.Lock()
        self._orphans: Set[str] = set()

        self._watchdog_thread: Optional[threading.Thread] = None
        self._watchdog_stop = threading.Event()

    # ------------------------------------------------------------------
    # Config helpers
    # ------------------------------------------------------------------
    def _cfg(self) -> Dict[str, Any]:
        try:
            return self._config_fn() or {}
        except Exception:
            return {}

    def _enabled(self) -> bool:
        return bool(self._cfg().get("enabled", True))

    def _executable(self) -> str:
        return str(self._cfg().get("executable") or "claude")

    def _max_concurrent(self) -> int:
        try:
            return max(1, int(self._cfg().get("max_concurrent_jobs", _DEFAULT_MAX_CONCURRENT)))
        except Exception:
            return _DEFAULT_MAX_CONCURRENT

    def _timeout_seconds(self, override: Optional[float] = None) -> float:
        if override is not None:
            return float(override)
        try:
            minutes = float(self._cfg().get("timeout_minutes", _DEFAULT_TIMEOUT_MINUTES))
        except Exception:
            minutes = _DEFAULT_TIMEOUT_MINUTES
        return max(30.0, minutes * 60.0)

    def _heartbeat_seconds(self) -> float:
        watch_cfg = self._cfg().get("completion_watch") or {}
        try:
            return max(1.0, float(watch_cfg.get("heartbeat_seconds", _DEFAULT_HEARTBEAT_SECONDS)))
        except Exception:
            return _DEFAULT_HEARTBEAT_SECONDS

    def _watchdog_seconds(self) -> float:
        watch_cfg = self._cfg().get("completion_watch") or {}
        try:
            return max(1.0, float(watch_cfg.get("watchdog_seconds", _DEFAULT_WATCHDOG_SECONDS)))
        except Exception:
            return _DEFAULT_WATCHDOG_SECONDS

    def _forward_anthropic_api_key(self) -> bool:
        return bool(self._cfg().get("forward_anthropic_api_key", False))

    def _extra_cli_args(self) -> List[str]:
        raw = self._cfg().get("extra_cli_args") or []
        return [str(a) for a in raw] if isinstance(raw, list) else []

    def _effort(self) -> Optional[str]:
        """Return a bounded Claude effort level when configured.

        Artifact jobs should not silently inherit a high/max effort setting
        from a user's interactive Claude configuration.  ``None`` preserves
        the CLI default for callers that explicitly opt out.
        """
        value = self._cfg().get("effort", "medium")
        value = str(value).strip().lower() if value is not None else ""
        return value if value in {"low", "medium", "high", "xhigh", "max"} else None

    def _no_session_persistence(self) -> bool:
        return bool(self._cfg().get("no_session_persistence", True))

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def _db_path(self) -> Path:
        if self._db_path_override is not None:
            return self._db_path_override
        return get_hermes_home() / "state.db"

    def _log_dir(self) -> Path:
        base = self._log_dir_override or (get_hermes_home() / "cache" / "arceus")
        base.mkdir(parents=True, exist_ok=True)
        return base

    def _connect(self) -> sqlite3.Connection:
        path = self._db_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(path, timeout=10)
        try:
            self._init_schema(conn)
        except Exception:
            conn.close()
            raise
        return conn

    @staticmethod
    def _init_schema(conn: sqlite3.Connection) -> None:
        try:
            from hermes_state import apply_wal_with_fallback

            apply_wal_with_fallback(conn, db_label="state.db (arceus)")
        except Exception:
            # WAL is a durability/perf nicety; schema creation must still
            # proceed on backends where the pragma is unavailable.
            pass
        conn.execute(
            """CREATE TABLE IF NOT EXISTS arceus_jobs (
                job_id TEXT PRIMARY KEY,
                parent_task_id TEXT,
                session_key TEXT NOT NULL DEFAULT '',
                origin_ui_session_id TEXT NOT NULL DEFAULT '',
                origin_session_id TEXT NOT NULL DEFAULT '',
                parent_session_id TEXT,
                task_type TEXT NOT NULL DEFAULT 'complex_coding',
                task_brief TEXT,
                working_directory TEXT,
                state TEXT NOT NULL,
                owner_pid INTEGER,
                claude_pid INTEGER,
                claude_pid_started_at INTEGER,
                created_at REAL NOT NULL,
                started_at REAL,
                last_heartbeat_at REAL,
                completed_at REAL,
                timeout_seconds REAL,
                summary TEXT,
                error TEXT,
                delivered INTEGER NOT NULL DEFAULT 0
            )"""
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_arceus_jobs_state ON arceus_jobs(state)"
        )

    @contextlib.contextmanager
    def _transaction(self):
        conn = self._connect()
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Public: launch
    # ------------------------------------------------------------------
    def launch(
        self,
        *,
        task_brief: str,
        task_type: str = "complex_coding",
        working_directory: Optional[str] = None,
        session_key: str = "",
        origin_ui_session_id: str = "",
        origin_session_id: str = "",
        parent_session_id: Optional[str] = None,
        parent_task_id: Optional[str] = None,
        timeout_seconds: Optional[float] = None,
    ) -> ArceusJobHandle:
        if not self._enabled():
            return ArceusJobHandle(job_id="", status="rejected", state="", error="Arceus is disabled (arceus.enabled=false).")

        job_id = f"arceus_{uuid.uuid4().hex[:10]}"
        now = self._now()
        with self._transaction() as conn:
            conn.execute(
                """INSERT INTO arceus_jobs
                   (job_id, parent_task_id, session_key, origin_ui_session_id,
                    origin_session_id, parent_session_id, task_type, task_brief,
                    working_directory, state, owner_pid, created_at,
                    timeout_seconds, delivered)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)""",
                (
                    job_id, parent_task_id, session_key, origin_ui_session_id,
                    origin_session_id, str(parent_session_id) if parent_session_id else None,
                    task_type, task_brief, working_directory,
                    ArceusState.QUEUED.value, os.getpid(), now,
                    self._timeout_seconds(timeout_seconds),
                ),
            )

        with self._admission_lock:
            queue_position = len(self._queue) + (1 if self._running_job_id else 0)
            self._queue.append(job_id)
        self._pump_queue()
        status = "queued" if queue_position > 0 else "started"
        return ArceusJobHandle(job_id=job_id, status=status, state=ArceusState.QUEUED.value, queue_position=queue_position)

    # ------------------------------------------------------------------
    # Admission / concurrency=1 (configurable) queue
    # ------------------------------------------------------------------
    def _pump_queue(self) -> None:
        # NOTE: only a single concurrent job is actually executed regardless
        # of ``max_concurrent_jobs`` — the spec's default and required
        # behavior is strict concurrency=1 with a FIFO queue for the rest.
        # ``max_concurrent_jobs`` is read (and surfaced in /arceus-status)
        # for forward-compatibility but raising it above 1 has no effect yet;
        # this is a documented limitation, not silent under-delivery.
        job_id: Optional[str] = None
        with self._admission_lock:
            if self._running_job_id is not None:
                return
            if not self._queue:
                return
            job_id = self._queue.popleft()
            self._running_job_id = job_id

        thread = threading.Thread(
            target=self._run_job, args=(job_id,), name=f"arceus-worker-{job_id}", daemon=True,
        )
        with self._threads_lock:
            self._threads[job_id] = thread
        thread.start()

    def _release_slot_and_pump(self) -> None:
        with self._admission_lock:
            self._running_job_id = None
        self._pump_queue()

    # ------------------------------------------------------------------
    # Job execution (primary completion path lives here: proc.wait())
    # ------------------------------------------------------------------
    def _row(self, conn: sqlite3.Connection, job_id: str) -> Optional[sqlite3.Row]:
        conn.row_factory = sqlite3.Row
        return conn.execute("SELECT * FROM arceus_jobs WHERE job_id=?", (job_id,)).fetchone()

    def _sanitized_env(self) -> Dict[str, str]:
        env = dict(os.environ)
        if not self._forward_anthropic_api_key():
            # Arceus is subscription-CLI-backed; never let a locally exported
            # ANTHROPIC_API_KEY leak into the child and silently switch it to
            # metered API billing. The user's shell environment is untouched.
            env.pop("ANTHROPIC_API_KEY", None)
        return env

    def _run_job(self, job_id: str) -> None:
        try:
            self._run_job_inner(job_id)
        except Exception as exc:  # noqa: BLE001 — must never crash the worker thread
            logger.exception("Arceus job %s crashed in the worker thread", job_id)
            self._finalize(
                job_id,
                state=ArceusState.FAILED.value,
                error=f"Arceus worker crashed: {type(exc).__name__}: {exc}",
                source="primary",
            )
        finally:
            self._release_slot_and_pump()

    def _run_job_inner(self, job_id: str) -> None:
        with self._transaction() as conn:
            row = self._row(conn, job_id)
            if row is None or row["state"] != ArceusState.QUEUED.value:
                return
            conn.execute(
                "UPDATE arceus_jobs SET state=? WHERE job_id=?",
                (ArceusState.STARTING.value, job_id),
            )
            task_brief = row["task_brief"] or ""
            working_directory = row["working_directory"]
            timeout_seconds = row["timeout_seconds"] or self._timeout_seconds()

        # stream-json is intentional: single-result JSON leaves stdout empty
        # for the entire Claude run, making a healthy long job look stalled.
        # The final ``type=result`` event is still parsed for completion.
        cmd = [self._executable(), "-p", "--verbose", "--output-format", "stream-json"]
        effort = self._effort()
        if effort:
            cmd.extend(["--effort", effort])
        if self._no_session_persistence():
            cmd.append("--no-session-persistence")
        cmd.extend(self._extra_cli_args())
        stdout_path = self._log_dir() / f"{job_id}.stdout.log"
        stderr_path = self._log_dir() / f"{job_id}.stderr.log"

        try:
            with open(stdout_path, "wb") as out_f, open(stderr_path, "wb") as err_f:
                proc = self._popen_factory(
                    cmd,
                    cwd=working_directory or None,
                    env=self._sanitized_env(),
                    stdin=subprocess.PIPE,
                    stdout=out_f,
                    stderr=err_f,
                    start_new_session=True,
                )
                try:
                    if proc.stdin is not None:
                        proc.stdin.write(task_brief.encode("utf-8", errors="replace"))
                        proc.stdin.close()
                except Exception:
                    logger.debug("Arceus job %s: could not write prompt to stdin", job_id, exc_info=True)

                claude_pid = getattr(proc, "pid", None)
                started_at = self._now()
                claude_started_at = self._get_process_start_time(claude_pid) if claude_pid else None
                with self._processes_lock:
                    self._processes[job_id] = proc
                with self._transaction() as conn:
                    conn.execute(
                        """UPDATE arceus_jobs SET state=?, claude_pid=?, claude_pid_started_at=?,
                           started_at=?, last_heartbeat_at=? WHERE job_id=?""",
                        (ArceusState.RUNNING.value, claude_pid, claude_started_at, started_at, started_at, job_id),
                    )

                exit_code, timed_out = self._wait_with_heartbeat(job_id, proc, started_at, timeout_seconds)
        except FileNotFoundError as exc:
            self._finalize(
                job_id,
                state=ArceusState.FAILED.value,
                error=f"Could not launch the local claude CLI ({self._executable()!r}): {exc}",
                source="primary",
            )
            return
        except Exception as exc:  # noqa: BLE001
            self._finalize(
                job_id,
                state=ArceusState.FAILED.value,
                error=f"Failed to start Arceus subprocess: {type(exc).__name__}: {exc}",
                source="primary",
            )
            return
        finally:
            with self._processes_lock:
                self._processes.pop(job_id, None)

        cancelled = self._consume_cancel_flag(job_id)
        self._finalize_from_disk(
            job_id,
            exit_code=exit_code,
            timed_out=timed_out,
            cancelled=cancelled,
            source="primary",
        )

    def _wait_with_heartbeat(
        self, job_id: str, proc: "subprocess.Popen", started_at: float, timeout_seconds: float,
    ) -> Tuple[Optional[int], bool]:
        """Poll the child (never blocking wait()) so heartbeat + timeout + a
        cooperative cancel request are all serviced while we wait — this IS
        the liveness/heartbeat mechanism the spec calls for: a periodic,
        purely programmatic check, never a model call."""
        heartbeat_interval = self._heartbeat_seconds()
        poll_interval = min(1.0, heartbeat_interval)
        next_heartbeat = self._now()
        while True:
            exit_code = proc.poll()
            now = self._now()
            if exit_code is not None:
                return exit_code, False
            if now - started_at >= timeout_seconds:
                self._terminate(proc)
                return proc.poll(), True
            if self._consume_cancel_flag(job_id, peek=True):
                self._terminate(proc)
                return proc.poll(), False
            if now >= next_heartbeat:
                self._touch_heartbeat(job_id, now)
                next_heartbeat = now + heartbeat_interval
            self._sleep(poll_interval)

    def _touch_heartbeat(self, job_id: str, ts: float) -> None:
        try:
            with self._transaction() as conn:
                conn.execute(
                    "UPDATE arceus_jobs SET last_heartbeat_at=? WHERE job_id=? AND state=?",
                    (ts, job_id, ArceusState.RUNNING.value),
                )
        except Exception:
            logger.debug("Arceus job %s: heartbeat update failed", job_id, exc_info=True)

    @staticmethod
    def _terminate(proc: "subprocess.Popen") -> None:
        import signal as _signal

        try:
            pgid = os.getpgid(proc.pid)
            os.killpg(pgid, _signal.SIGTERM)
        except Exception:
            with contextlib.suppress(Exception):
                proc.terminate()
        try:
            proc.wait(timeout=_TERMINATE_GRACE_SECONDS)
            return
        except Exception:
            pass
        try:
            pgid = os.getpgid(proc.pid)
            os.killpg(pgid, _signal.SIGKILL)
        except Exception:
            with contextlib.suppress(Exception):
                proc.kill()
        with contextlib.suppress(Exception):
            proc.wait(timeout=_TERMINATE_GRACE_SECONDS)

    # ------------------------------------------------------------------
    # Cancellation
    # ------------------------------------------------------------------
    def cancel(self, job_id: str) -> bool:
        with self._cancel_flags_lock:
            self._cancel_flags.add(job_id)
        with self._processes_lock:
            proc = self._processes.get(job_id)
        if proc is not None:
            self._terminate(proc)
            return True
        # Not currently running in this process (queued, or an orphan from a
        # prior restart) — mark it cancelled directly.
        return self._finalize(job_id, state=ArceusState.CANCELLED.value, error="Cancelled by user request.", source="cancel")

    def _consume_cancel_flag(self, job_id: str, *, peek: bool = False) -> bool:
        with self._cancel_flags_lock:
            present = job_id in self._cancel_flags
            if present and not peek:
                self._cancel_flags.discard(job_id)
            return present

    # ------------------------------------------------------------------
    # Result parsing + atomic/idempotent finalize
    # ------------------------------------------------------------------
    def _read_tail(self, path: Path, limit: int = _OUTPUT_TAIL_CHARS) -> str:
        try:
            data = path.read_bytes()
        except Exception:
            return ""
        text = data.decode("utf-8", errors="replace")
        return text[-limit:] if len(text) > limit else text

    def _classify_output(self, job_id: str, stdout_text: str) -> Tuple[str, str, Optional[str]]:
        """Return (state, summary, error) from the CLI's captured stdout.

        Process exit is the completion SIGNAL; this only decides WHICH
        terminal state a completed process represents, per the compact
        result envelope convention Arceus's own task prompt asks for.
        """
        parsed: Optional[Dict[str, Any]] = None
        stripped = stdout_text.strip()
        if stripped:
            try:
                parsed = json.loads(stripped)
            except Exception:
                # stream-json emits one JSON object per line.  Read the last
                # result event (or the last parseable object) while retaining
                # the whole tail as the human-readable summary/progress log.
                for line in reversed(stripped.splitlines()):
                    try:
                        candidate = json.loads(line)
                    except Exception:
                        continue
                    if isinstance(candidate, dict):
                        if candidate.get("type") == "result" or "result" in candidate:
                            parsed = candidate
                            break
                        if parsed is None:
                            parsed = candidate

        result_text = ""
        is_error = False
        if isinstance(parsed, dict):
            result_text = str(parsed.get("result") or "")
            is_error = bool(parsed.get("is_error"))
            if not result_text and parsed.get("type") == "assistant":
                message = parsed.get("message") or {}
                content = message.get("content") if isinstance(message, dict) else None
                if isinstance(content, list):
                    result_text = "\n".join(
                        str(item.get("text") or "")
                        for item in content
                        if isinstance(item, dict) and item.get("type") == "text"
                    ).strip()
        else:
            result_text = stripped

        if _NEEDS_INPUT_MARKER in result_text:
            question = result_text.split(_NEEDS_INPUT_MARKER, 1)[1].strip()
            return ArceusState.NEEDS_USER_INPUT.value, result_text[:_OUTPUT_TAIL_CHARS], (question or "Arceus needs more information to continue.")

        if is_error:
            return ArceusState.FAILED.value, result_text[:_OUTPUT_TAIL_CHARS], (result_text[:800] or "Arceus reported an error.")

        return ArceusState.SUCCEEDED.value, result_text[:_OUTPUT_TAIL_CHARS], None

    def _finalize_from_disk(
        self,
        job_id: str,
        *,
        exit_code: Optional[int],
        timed_out: bool,
        cancelled: bool,
        source: str,
    ) -> bool:
        stdout_text = self._read_tail(self._log_dir() / f"{job_id}.stdout.log")
        stderr_text = self._read_tail(self._log_dir() / f"{job_id}.stderr.log")

        if cancelled:
            return self._finalize(job_id, state=ArceusState.CANCELLED.value, error="Cancelled while running.", source=source)
        if timed_out:
            return self._finalize(
                job_id,
                state=ArceusState.TIMED_OUT.value,
                error="Arceus exceeded its configured timeout and was terminated.",
                summary=stdout_text or None,
                source=source,
            )
        if exit_code is None:
            return self._finalize(
                job_id,
                state=ArceusState.FAILED.value,
                error="Arceus subprocess ended without a readable exit code.",
                summary=stderr_text[:800] or None,
                source=source,
            )
        if exit_code != 0:
            error = stderr_text[:800] or stdout_text[:800] or f"claude exited with status {exit_code}."
            return self._finalize(
                job_id, state=ArceusState.FAILED.value, error=error,
                summary=stdout_text or None, source=source,
            )

        state, summary, error = self._classify_output(job_id, stdout_text)
        return self._finalize(job_id, state=state, summary=summary, error=error, source=source)

    def _finalize(
        self,
        job_id: str,
        *,
        state: str,
        summary: Optional[str] = None,
        error: Optional[str] = None,
        source: str = "primary",
    ) -> bool:
        """Atomically transition to a terminal state and, if (and only if)
        this call wins the race, deliver the completion. Both the primary
        (process-exit) path and the watchdog path call this — whichever
        wins the ``UPDATE ... WHERE state NOT IN (terminal)`` race performs
        delivery; the loser is a silent, safe no-op. This is INVARIANT 7 /
        the duplicate-completion protection the spec requires."""
        completed_at = self._now()
        with self._transaction() as conn:
            cur = conn.execute(
                """UPDATE arceus_jobs SET state=?, summary=?, error=?, completed_at=?
                   WHERE job_id=? AND state NOT IN ({})""".format(
                    ",".join("?" for _ in TERMINAL_STATES)
                ),
                (state, summary, error, completed_at, job_id, *TERMINAL_STATES),
            )
            won = cur.rowcount == 1
            if won:
                conn.row_factory = sqlite3.Row
                row = conn.execute("SELECT * FROM arceus_jobs WHERE job_id=?", (job_id,)).fetchone()
            else:
                row = None

        if not won:
            logger.debug(
                "Arceus job %s: finalize(%s) from %s lost the race (already terminal) — no duplicate delivery.",
                job_id, state, source,
            )
            with self._orphans_lock:
                self._orphans.discard(job_id)
            return False

        logger.info("Arceus job %s finalized as %s (source=%s)", job_id, state, source)
        with self._orphans_lock:
            self._orphans.discard(job_id)
        self._deliver(dict(row))
        return True

    # ------------------------------------------------------------------
    # Delivery — reuses the SAME completion rail as delegate_task(background=true)
    # ------------------------------------------------------------------
    @staticmethod
    def _default_push_event(evt: Dict[str, Any]) -> None:
        from tools.process_registry import process_registry

        process_registry.completion_queue.put(evt)

    def _deliver(self, row: Dict[str, Any]) -> None:
        job_id = row["job_id"]
        state = row["state"]
        status = {
            ArceusState.SUCCEEDED.value: "completed",
            ArceusState.FAILED.value: "error",
            ArceusState.TIMED_OUT.value: "error",
            ArceusState.CANCELLED.value: "interrupted",
            ArceusState.NEEDS_USER_INPUT.value: "needs_user_input",
        }.get(state, "error")

        dispatched_at = row.get("created_at") or self._now()
        completed_at = row.get("completed_at") or self._now()
        brief = (row.get("task_brief") or "")[:160]
        evt = {
            "type": "async_delegation",
            "delegation_id": job_id,
            "session_key": row.get("session_key") or "",
            "origin_ui_session_id": row.get("origin_ui_session_id") or "",
            "origin_session_id": row.get("origin_session_id") or "",
            "parent_session_id": row.get("parent_session_id"),
            "goal": f"[ARCEUS {row.get('task_type', 'complex_coding')}] {brief}",
            "context": None,
            "toolsets": None,
            "role": "arceus",
            "model": "claude-code-cli",
            "status": status,
            "summary": row.get("summary"),
            "error": row.get("error"),
            "api_calls": 0,
            "duration_seconds": round(completed_at - dispatched_at, 2) if dispatched_at else None,
            "dispatched_at": dispatched_at,
            "completed_at": completed_at,
            "exit_reason": state,
            # Additive, Arceus-specific fields — ignored by the generic
            # formatter/gateway code, useful to anything Arceus-aware
            # (e.g. /arceus-status, or a future richer formatter).
            "arceus_job_id": job_id,
            "arceus_state": state,
            "arceus_parent_task_id": row.get("parent_task_id"),
        }
        try:
            self._push_event(evt)
        except Exception:
            logger.error("Arceus job %s: failed to enqueue completion event; marking undelivered", job_id, exc_info=True)
            return
        try:
            with self._transaction() as conn:
                conn.execute("UPDATE arceus_jobs SET delivered=1 WHERE job_id=?", (job_id,))
        except Exception:
            logger.debug("Arceus job %s: could not mark delivered", job_id, exc_info=True)

    # ------------------------------------------------------------------
    # Watchdog — secondary completion path
    # ------------------------------------------------------------------
    def watchdog_tick(self) -> int:
        """One sweep. Returns how many jobs were force-finalized.

        Covers: (a) an orphaned job re-adopted at startup whose claude PID
        has since exited (no in-process thread exists to notice), and
        (b) — as a pure backstop — any RUNNING/STARTING job in THIS process
        whose heartbeat has gone stale far past what the poll loop should
        ever allow, which would only happen if the owning thread itself
        wedged before reaching its own finally block.
        """
        finalized = 0
        with self._orphans_lock:
            orphan_ids = list(self._orphans)
        for job_id in orphan_ids:
            if self._reconcile_orphan(job_id):
                finalized += 1

        now = self._now()
        with self._transaction() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM arceus_jobs WHERE state IN ('RUNNING','STARTING')"
            ).fetchall()
        for row in rows:
            job_id = row["job_id"]
            if job_id in orphan_ids:
                continue
            with self._threads_lock:
                has_thread = job_id in self._threads and self._threads[job_id].is_alive()
            if has_thread:
                continue  # actively owned by a live worker thread — not our job
            last_heartbeat = row["last_heartbeat_at"] or row["started_at"] or row["created_at"]
            if last_heartbeat is not None and (now - last_heartbeat) < _HEARTBEAT_ABANDON_SECONDS:
                # No live thread AND no recent heartbeat update yet — could be
                # a brand-new job about to be picked up; leave it alone for
                # now rather than racing the real worker.
                continue
            if self._finalize(
                job_id,
                state=ArceusState.FAILED.value,
                error="Arceus job's worker thread disappeared without reaching a terminal state (missed completion callback); reconciled by the watchdog.",
                source="watchdog",
            ):
                finalized += 1
        return finalized

    def _reconcile_orphan(self, job_id: str) -> bool:
        with self._transaction() as conn:
            row = self._row(conn, job_id)
        if row is None or row["state"] not in NON_TERMINAL_STATES:
            with self._orphans_lock:
                self._orphans.discard(job_id)
            return False

        claude_pid = row["claude_pid"]
        claude_started_at = row["claude_pid_started_at"]
        if claude_pid:
            alive = self._pid_exists(int(claude_pid))
            if alive and claude_started_at is not None:
                current_start = self._get_process_start_time(int(claude_pid))
                alive = current_start is not None and int(current_start) == int(claude_started_at)
            if alive:
                return False  # still genuinely running; keep watching next tick

        # PID is gone (or was never recorded, e.g. it died before the STARTING
        # row got its pid write). No live thread will ever call the primary
        # path for this job again, so read whatever output made it to disk.
        return self._finalize_from_disk(job_id, exit_code=None, timed_out=False, cancelled=False, source="watchdog")

    def start_watchdog(self) -> None:
        if self._watchdog_thread is not None and self._watchdog_thread.is_alive():
            return
        self._watchdog_stop.clear()
        interval = self._watchdog_seconds()

        def _loop() -> None:
            while not self._watchdog_stop.wait(interval):
                try:
                    self.watchdog_tick()
                except Exception:
                    logger.debug("Arceus watchdog tick failed", exc_info=True)

        self._watchdog_thread = threading.Thread(target=_loop, name="arceus-watchdog", daemon=True)
        self._watchdog_thread.start()

    def stop_watchdog(self, *, join_timeout: float = 2.0) -> None:
        self._watchdog_stop.set()
        thread = self._watchdog_thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=join_timeout)
        self._watchdog_thread = None

    # ------------------------------------------------------------------
    # Restart reconciliation (one-shot, called at process startup)
    # ------------------------------------------------------------------
    def reconcile_on_startup(self) -> Dict[str, int]:
        """Resolve every non-terminal job left over from a prior process.

        A job still QUEUED/STARTING/RUNNING when the OWNING Hermes process
        exited has no live worker thread in THIS process (a fresh process
        cannot rejoin an old ThreadPoolExecutor). For each such job:

          - if its claude subprocess is still alive (by pid + start time,
            guarding against pid reuse): re-adopt it into the in-memory
            orphan set so the watchdog keeps watching and finalizes it the
            moment it exits — "strongest available recovery" per spec,
            rather than killing potentially-valuable in-flight work.
          - otherwise: finalize immediately from whatever was captured to
            the on-disk stdout/stderr log before the old process died
            (FAILED if nothing usable was written).

        Also delivers any job that reached a terminal state but was never
        marked ``delivered`` (e.g. the process died between finalize()'s
        DB commit and the completion_queue.put() call).
        """
        stats = {"reconciled_running": 0, "redelivered": 0}
        my_pid = os.getpid()
        with self._transaction() as conn:
            conn.row_factory = sqlite3.Row
            stale = conn.execute(
                "SELECT job_id, owner_pid, claude_pid FROM arceus_jobs WHERE state IN ('QUEUED','STARTING','RUNNING')"
            ).fetchall()
            undelivered = conn.execute(
                "SELECT job_id FROM arceus_jobs WHERE state IN ({}) AND delivered=0".format(
                    ",".join("?" for _ in TERMINAL_STATES)
                ),
                tuple(TERMINAL_STATES),
            ).fetchall()

        for r in stale:
            if int(r["owner_pid"] or -1) == my_pid:
                continue  # belongs to a live worker thread in THIS process
            job_id = r["job_id"]
            claude_pid = r["claude_pid"]
            if claude_pid and self._pid_exists(int(claude_pid)):
                with self._orphans_lock:
                    self._orphans.add(job_id)
                logger.info("Arceus: re-adopted still-running job %s (pid=%s) after restart", job_id, claude_pid)
            else:
                self._finalize_from_disk(job_id, exit_code=None, timed_out=False, cancelled=False, source="restart_reconciliation")
            stats["reconciled_running"] += 1

        for r in undelivered:
            with self._transaction() as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute("SELECT * FROM arceus_jobs WHERE job_id=?", (r["job_id"],)).fetchone()
            if row is not None:
                self._deliver(dict(row))
                stats["redelivered"] += 1

        if stale or undelivered:
            logger.info("Arceus restart reconciliation: %s", stats)
        return stats

    # ------------------------------------------------------------------
    # Status introspection (/arceus-status)
    # ------------------------------------------------------------------
    def status_snapshot(self) -> Dict[str, Any]:
        with self._admission_lock:
            running_job_id = self._running_job_id
            queued = len(self._queue)
        max_concurrent = self._max_concurrent()

        current: Optional[Dict[str, Any]] = None
        with self._transaction() as conn:
            conn.row_factory = sqlite3.Row
            if running_job_id:
                row = conn.execute("SELECT * FROM arceus_jobs WHERE job_id=?", (running_job_id,)).fetchone()
                if row:
                    current = dict(row)
            last_row = conn.execute(
                "SELECT job_id, state, completed_at FROM arceus_jobs WHERE state IN ({}) ORDER BY completed_at DESC LIMIT 1".format(
                    ",".join("?" for _ in TERMINAL_STATES)
                ),
                tuple(TERMINAL_STATES),
            ).fetchone()

        executable = self._executable()
        cli_installed = None
        cli_version = None
        try:
            import shutil as _shutil

            cli_path = _shutil.which(executable)
            cli_installed = cli_path is not None
            if cli_installed:
                probe = subprocess.run(
                    [executable, "--version"], capture_output=True, text=True, timeout=5,
                )
                cli_version = (probe.stdout or probe.stderr or "").strip() or None
        except Exception:
            cli_installed = cli_installed if cli_installed is not None else False

        now = self._now()
        return {
            "enabled": self._enabled(),
            "executable": executable,
            "cli_installed": cli_installed,
            "cli_version": cli_version,
            "auth_mode": "subscription (local claude CLI)" if not self._forward_anthropic_api_key() else "ANTHROPIC_API_KEY forwarded (non-default)",
            "max_concurrent_jobs": max_concurrent,
            "running": 1 if running_job_id else 0,
            "queued": queued,
            "current_job": (
                {
                    "job_id": current["job_id"],
                    "task_type": current["task_type"],
                    "state": current["state"],
                    "runtime_seconds": round(now - current["started_at"], 1) if current.get("started_at") else None,
                    "last_heartbeat_seconds_ago": (
                        round(now - current["last_heartbeat_at"], 1) if current.get("last_heartbeat_at") else None
                    ),
                    "parent_task_id": current.get("parent_task_id"),
                }
                if current
                else None
            ),
            "completion_watcher": "ACTIVE",
            "watchdog": "ACTIVE" if (self._watchdog_thread is not None and self._watchdog_thread.is_alive()) else "INACTIVE",
            "last_job": ({"job_id": last_row["job_id"], "state": last_row["state"]} if last_row else None),
        }


def format_status_report(snapshot: Dict[str, Any]) -> str:
    """Render ``status_snapshot()`` as the plain-text block from the spec."""
    lines = ["ARCEUS STATUS", ""]
    lines.append(f"Enabled: {'Yes' if snapshot['enabled'] else 'No'}")
    lines.append(f"Claude CLI: {'Installed' if snapshot['cli_installed'] else 'Not found'}")
    if snapshot.get("cli_version"):
        lines.append(f"Claude Version: {snapshot['cli_version']}")
    lines.append(f"Authentication: {snapshot['auth_mode']}")
    lines.append("")
    lines.append(f"Running: {snapshot['running']} / {snapshot['max_concurrent_jobs']}")
    lines.append(f"Queued: {snapshot['queued']}")
    job = snapshot.get("current_job")
    if job:
        lines.append("")
        lines.append("Current Job:")
        lines.append(f"- ID: {job['job_id']}")
        lines.append(f"- Type: {job['task_type']}")
        lines.append(f"- State: {job['state']}")
        if job.get("runtime_seconds") is not None:
            lines.append(f"- Runtime: {int(job['runtime_seconds'])}s")
        if job.get("last_heartbeat_seconds_ago") is not None:
            lines.append(f"- Last heartbeat: {int(job['last_heartbeat_seconds_ago'])}s ago")
        lines.append(f"- Parent Hermes task: {job.get('parent_task_id') or 'waiting'}")
    lines.append("")
    lines.append(f"Completion watcher: {snapshot['completion_watcher']}")
    lines.append(f"Watchdog: {snapshot['watchdog']}")
    last = snapshot.get("last_job")
    lines.append(f"Last job: {last['state'] if last else 'none'}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Module-level singleton + thin functional API
# ---------------------------------------------------------------------------
_engine: Optional[ArceusEngine] = None
_engine_lock = threading.Lock()


def get_engine() -> ArceusEngine:
    global _engine
    with _engine_lock:
        if _engine is None:
            _engine = ArceusEngine()
        return _engine


def launch_arceus_job(**kwargs: Any) -> ArceusJobHandle:
    return get_engine().launch(**kwargs)


def cancel_arceus_job(job_id: str) -> bool:
    return get_engine().cancel(job_id)


def reconcile_on_startup() -> Dict[str, int]:
    return get_engine().reconcile_on_startup()


def start_watchdog() -> None:
    get_engine().start_watchdog()


def stop_watchdog() -> None:
    get_engine().stop_watchdog()


def status_snapshot() -> Dict[str, Any]:
    return get_engine().status_snapshot()


def _reset_for_tests() -> None:
    """Test-only: drop the module singleton so a fresh engine (with fresh
    DI'd fakes / a fresh temp DB) can be installed between tests."""
    global _engine
    with _engine_lock:
        if _engine is not None:
            _engine.stop_watchdog()
        _engine = None
