"""Focused, dependency-injected tests for tools/arceus.py (the Arceus
child-job lifecycle worker: QUEUED -> STARTING -> RUNNING -> terminal).

Every test builds its OWN ``ArceusEngine`` with a temp SQLite file, a temp
log dir, a fake ``popen_factory`` (no real ``claude`` process is ever
spawned), fake pid/liveness probes, and a fake completion sink — never the
module-level singleton. The background watchdog THREAD is never started;
``watchdog_tick()`` is called directly so watchdog behavior is deterministic
and does not depend on wall-clock scheduling.
"""

from __future__ import annotations

import io
import json
import os
import subprocess
import threading
import time

from tools.arceus import ArceusEngine, ArceusState, TERMINAL_STATES

# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeProcess:
    """Stand-in for subprocess.Popen. ``pid`` defaults to a value far above
    any real PID range so ``os.getpgid()``/signals inside the engine's real
    ``_terminate()`` path always miss cleanly (ProcessLookupError) instead of
    risking hitting an unrelated real process during a timeout test."""

    def __init__(self, pid=900_000_001, exit_code=0, hang_event=None):
        self.pid = pid
        self.stdin = io.BytesIO()
        self._exit_code = exit_code
        self._hang_event = hang_event  # if given, poll() blocks until .set()
        self._terminated = False

    def poll(self):
        if self._hang_event is not None and not self._hang_event.is_set() and not self._terminated:
            return None
        return self._exit_code

    def terminate(self):
        self._terminated = True
        self._exit_code = -15

    def kill(self):
        self._terminated = True
        self._exit_code = -9

    def wait(self, timeout=None):
        if self.poll() is None:
            raise subprocess.TimeoutExpired(cmd="fake-claude", timeout=timeout)
        return self._exit_code


def make_popen_factory(*, stdout=b"", stderr=b"", pid=900_000_001, exit_code=0, hang_event=None, calls=None):
    """Build a fake ``popen_factory``. Instead of spawning a real process it
    writes the given fake stdout/stderr straight into the file handles the
    engine already opened (mirroring what a real
    ``claude -p --output-format json`` run would have produced on disk) and
    returns a FakeProcess in its place."""

    def factory(cmd, **kwargs):
        if calls is not None:
            calls.append({"cmd": cmd, "env": kwargs.get("env"), "cwd": kwargs.get("cwd")})
        out_f = kwargs.get("stdout")
        err_f = kwargs.get("stderr")
        if out_f is not None:
            out_f.write(stdout)
            out_f.flush()
        if err_f is not None:
            err_f.write(stderr)
            err_f.flush()
        return FakeProcess(pid=pid, exit_code=exit_code, hang_event=hang_event)

    return factory


def success_stdout(text="Implemented X.\nSTATUS: PASS"):
    return json.dumps({"result": text, "is_error": False}).encode("utf-8")


# ---------------------------------------------------------------------------
# Engine builder + small helpers
# ---------------------------------------------------------------------------


def make_engine(tmp_path, *, popen_factory, alive_pids=None, config=None, push_events=None):
    """``alive_pids`` maps claude_pid -> process-start-time, used to fake
    both ``pid_exists_fn`` and ``get_process_start_time_fn`` (the pid-reuse
    guard the engine uses for orphan/restart reconciliation)."""
    alive_pids = alive_pids if alive_pids is not None else {}
    push_events = push_events if push_events is not None else []
    cfg = {
        "enabled": True,
        "max_concurrent_jobs": 1,
        # heartbeat_seconds is floored to 1.0s inside the engine regardless
        # of this value; watchdog_seconds is unused here since tests call
        # watchdog_tick() directly instead of starting the background loop.
        "completion_watch": {"heartbeat_seconds": 1, "watchdog_seconds": 1},
    }
    if config:
        cfg.update(config)

    engine = ArceusEngine(
        db_path=tmp_path / "state.db",
        popen_factory=popen_factory,
        pid_exists_fn=lambda pid: pid in alive_pids,
        get_process_start_time_fn=lambda pid: alive_pids.get(pid),
        push_event_fn=lambda evt: push_events.append(evt),
        config_fn=lambda: cfg,
        log_dir=tmp_path / "logs",
    )
    return engine, push_events


def get_job(engine, job_id):
    with engine._transaction() as conn:
        row = engine._row(conn, job_id)
        return dict(row) if row else None


def insert_job(engine, job_id, *, state, owner_pid=0, claude_pid=None,
                claude_pid_started_at=None, parent_task_id=None, delivered=0, ts=None):
    """Directly seed a job row, bypassing launch()/the worker thread —
    used to simulate jobs that belong to a *different* (crashed/restarted)
    Hermes process, which is exactly the scenario the watchdog and restart
    reconciliation paths exist to recover from."""
    ts = ts if ts is not None else time.time()
    with engine._transaction() as conn:
        conn.execute(
            """INSERT INTO arceus_jobs (job_id, parent_task_id, state, owner_pid, claude_pid,
               claude_pid_started_at, created_at, started_at, last_heartbeat_at, delivered)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (job_id, parent_task_id, state, owner_pid, claude_pid, claude_pid_started_at, ts, ts, ts, delivered),
        )


def wait_until(predicate, timeout=5.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def wait_for_state(engine, job_id, states, timeout=5.0):
    states = set(states)
    ok = wait_until(lambda: (get_job(engine, job_id) or {}).get("state") in states, timeout=timeout)
    row = get_job(engine, job_id)
    assert ok, f"job {job_id} never reached {states}; last row={row}"
    return row


# ---------------------------------------------------------------------------
# 1. State transitions (QUEUED -> RUNNING -> SUCCEEDED) + heartbeat
# ---------------------------------------------------------------------------


def test_state_transitions_and_heartbeat_on_success(tmp_path):
    hang = threading.Event()
    factory = make_popen_factory(stdout=success_stdout(), hang_event=hang, pid=900_000_011)
    engine, events = make_engine(tmp_path, popen_factory=factory)
    try:
        handle = engine.launch(task_brief="do work", parent_task_id="hermes-parent-1")
        assert handle.status == "started"
        assert handle.state == ArceusState.QUEUED.value

        row = wait_for_state(engine, handle.job_id, {ArceusState.RUNNING.value})
        assert row["claude_pid"] == 900_000_011
        assert row["parent_task_id"] == "hermes-parent-1"
        assert row["last_heartbeat_at"] is not None
        heartbeat_before = row["last_heartbeat_at"]

        # A liveness heartbeat must land purely from programmatic polling
        # (no model call) while the job is still genuinely RUNNING.
        assert wait_until(
            lambda: (get_job(engine, handle.job_id) or {}).get("last_heartbeat_at", 0) > heartbeat_before,
            timeout=4.0,
        ), "no heartbeat update observed while RUNNING"

        hang.set()  # let the fake claude process "exit"
        row = wait_for_state(engine, handle.job_id, TERMINAL_STATES)
        assert row["state"] == ArceusState.SUCCEEDED.value
        assert row["completed_at"] is not None
        assert len(events) == 1
        assert events[0]["arceus_state"] == ArceusState.SUCCEEDED.value
        assert events[0]["arceus_parent_task_id"] == "hermes-parent-1"
    finally:
        engine.stop_watchdog()


# ---------------------------------------------------------------------------
# 2. Process exit -> primary completion handler
# ---------------------------------------------------------------------------


def test_process_exit_triggers_completion_handler(tmp_path):
    factory = make_popen_factory(stdout=success_stdout("Implemented X.\nSTATUS: PASS"))
    engine, events = make_engine(tmp_path, popen_factory=factory)
    handle = engine.launch(task_brief="do work", parent_task_id="parent-2")

    row = wait_for_state(engine, handle.job_id, TERMINAL_STATES)
    assert row["state"] == ArceusState.SUCCEEDED.value
    assert row["delivered"] == 1
    assert len(events) == 1
    evt = events[0]
    assert evt["type"] == "async_delegation"  # reuses the existing completion rail
    assert evt["arceus_job_id"] == handle.job_id
    assert evt["status"] == "completed"


def test_process_crash_nonzero_exit_marks_failed(tmp_path):
    factory = make_popen_factory(stdout=b"", stderr=b"traceback: boom", exit_code=1)
    engine, events = make_engine(tmp_path, popen_factory=factory)
    handle = engine.launch(task_brief="do work")

    row = wait_for_state(engine, handle.job_id, TERMINAL_STATES)
    assert row["state"] == ArceusState.FAILED.value
    assert "boom" in (row["error"] or "")
    assert len(events) == 1
    assert events[0]["status"] == "error"


# ---------------------------------------------------------------------------
# 3. Missed callback recovery (watchdog fallback) — CRITICAL per spec
# ---------------------------------------------------------------------------


def test_missed_callback_recovery_via_watchdog(tmp_path):
    """Simulate the primary process-exit callback never firing (e.g. the
    owning worker thread crashed or this job was re-adopted from a prior
    process): the job sits RUNNING with no live worker thread, the claude
    subprocess has already exited, and useful output made it to disk. The
    watchdog — NOT the primary path — must still reconcile it to a terminal
    state and deliver exactly one completion event."""
    engine, events = make_engine(tmp_path, popen_factory=make_popen_factory(), alive_pids={})
    job_id = "arceus_missed_callback"
    insert_job(engine, job_id, state=ArceusState.RUNNING.value, owner_pid=424242,
               claude_pid=555555, parent_task_id="parent-missed")
    # The real claude process already exited by the time the watchdog looks;
    # its output made it to disk but the exit code itself is unrecoverable
    # without a live process handle.
    (engine._log_dir() / f"{job_id}.stdout.log").write_bytes(success_stdout())

    # This is what reconcile_on_startup() would have done at adoption time;
    # done directly here to isolate the watchdog reconciliation path itself.
    with engine._orphans_lock:
        engine._orphans.add(job_id)

    finalized = engine.watchdog_tick()

    assert finalized == 1
    row = get_job(engine, job_id)
    assert row["state"] in TERMINAL_STATES, "watchdog must resolve to a terminal state, never leave it RUNNING"
    # Known, intentional limitation: because the exit code cannot be
    # recovered once the owning process/thread is gone, watchdog-recovered
    # jobs are conservatively marked FAILED rather than guessed SUCCEEDED
    # from log content alone. The critical invariant this test protects is
    # that reconciliation happens and the parent is woken exactly once —
    # not silence.
    assert row["state"] == ArceusState.FAILED.value
    assert row["delivered"] == 1
    assert len(events) == 1
    assert events[0]["arceus_job_id"] == job_id
    assert events[0]["status"] == "error"

    # A second sweep must be a no-op (nothing left to reconcile / redeliver).
    assert engine.watchdog_tick() == 0
    assert len(events) == 1


# ---------------------------------------------------------------------------
# 4. Duplicate completion / idempotent finalization — CRITICAL per spec
# ---------------------------------------------------------------------------


def test_duplicate_completion_is_idempotent(tmp_path):
    factory = make_popen_factory(stdout=success_stdout())
    engine, events = make_engine(tmp_path, popen_factory=factory)
    handle = engine.launch(task_brief="do work")

    row = wait_for_state(engine, handle.job_id, TERMINAL_STATES)
    assert row["state"] == ArceusState.SUCCEEDED.value
    assert len(events) == 1

    # Simulate the exact race the spec calls out: a watchdog sweep discovers
    # the "same" completion just after the primary process-exit path already
    # finalized it.
    won = engine._finalize(handle.job_id, state=ArceusState.FAILED.value, error="dup", source="watchdog")

    assert won is False, "the second finalize() must lose the atomic race"
    assert len(events) == 1, "no duplicate user-facing completion may be delivered"
    row_after = get_job(engine, handle.job_id)
    assert row_after["state"] == ArceusState.SUCCEEDED.value, "the winning terminal state must not be overwritten"


def test_concurrent_finalize_race_delivers_exactly_once(tmp_path):
    """Same invariant as above, but exercised with genuine thread
    concurrency instead of sequential calls, since the real race is between
    the worker thread's proc.wait()-path and the watchdog thread."""
    factory = make_popen_factory(stdout=success_stdout())
    engine, events = make_engine(tmp_path, popen_factory=factory)
    job_id = "arceus_race"
    insert_job(engine, job_id, state=ArceusState.RUNNING.value, owner_pid=os.getpid())

    barrier = threading.Barrier(2)

    def finalize_from(source):
        barrier.wait(timeout=5)
        engine._finalize(job_id, state=ArceusState.SUCCEEDED.value, summary="ok", source=source)

    t1 = threading.Thread(target=finalize_from, args=("primary",))
    t2 = threading.Thread(target=finalize_from, args=("watchdog",))
    t1.start()
    t2.start()
    t1.join(timeout=5)
    t2.join(timeout=5)

    assert len(events) == 1, f"exactly one delivery expected under a real race, got {len(events)}"


# ---------------------------------------------------------------------------
# 5. Queue: max concurrency 1, FIFO, independent per-job finalization
# ---------------------------------------------------------------------------


def test_queue_runs_one_at_a_time_fifo(tmp_path):
    hang = threading.Event()
    factory = make_popen_factory(stdout=success_stdout(), hang_event=hang, pid=900_000_021)
    engine, events = make_engine(tmp_path, popen_factory=factory)
    try:
        h1 = engine.launch(task_brief="job1", parent_task_id="parentA")
        assert h1.status == "started"
        wait_for_state(engine, h1.job_id, {ArceusState.RUNNING.value})

        h2 = engine.launch(task_brief="job2", parent_task_id="parentB")
        assert h2.status == "queued"
        assert h2.queue_position == 1
        time.sleep(0.2)
        assert get_job(engine, h2.job_id)["state"] == ArceusState.QUEUED.value, "job2 must not start before job1 finishes"

        hang.set()  # release job1; by the time job2 starts, hang is already set -> it completes immediately too
        wait_for_state(engine, h1.job_id, TERMINAL_STATES)
        wait_for_state(engine, h2.job_id, TERMINAL_STATES)

        assert get_job(engine, h1.job_id)["state"] == ArceusState.SUCCEEDED.value
        assert get_job(engine, h2.job_id)["state"] == ArceusState.SUCCEEDED.value
        assert get_job(engine, h1.job_id)["parent_task_id"] == "parentA"
        assert get_job(engine, h2.job_id)["parent_task_id"] == "parentB"
        # Each parent must be woken independently — no request disappears.
        assert len(events) == 2
        assert {e["arceus_job_id"] for e in events} == {h1.job_id, h2.job_id}
        assert {e["arceus_parent_task_id"] for e in events} == {"parentA", "parentB"}
    finally:
        engine.stop_watchdog()


# ---------------------------------------------------------------------------
# 6. Timeout: process is terminated and job is marked TIMED_OUT
# ---------------------------------------------------------------------------


def test_timeout_terminates_process_and_marks_timed_out(tmp_path):
    hang = threading.Event()  # never set -> the fake process "hangs forever" until terminate()
    factory = make_popen_factory(hang_event=hang, pid=900_000_031)
    engine, events = make_engine(tmp_path, popen_factory=factory)
    try:
        handle = engine.launch(task_brief="job that hangs forever", timeout_seconds=0.3)
        row = wait_for_state(engine, handle.job_id, TERMINAL_STATES, timeout=8.0)

        assert row["state"] == ArceusState.TIMED_OUT.value
        assert "timeout" in (row["error"] or "").lower()
        assert len(events) == 1
        assert events[0]["arceus_state"] == ArceusState.TIMED_OUT.value
        assert events[0]["status"] == "error"
    finally:
        engine.stop_watchdog()


# ---------------------------------------------------------------------------
# 7. No model polling while waiting — INVARIANT 5 / 8
# ---------------------------------------------------------------------------


def test_no_completion_event_fires_while_still_running(tmp_path):
    """The completion event is what would trigger a new (cheap) model
    invocation to finalize with the user. It must fire exactly once, and
    only once the job actually reaches a terminal state — never on a timer
    while the job is genuinely still RUNNING. Liveness is tracked purely via
    process polling/heartbeat, never a model call."""
    hang = threading.Event()
    factory = make_popen_factory(stdout=success_stdout(), hang_event=hang, pid=900_000_041)
    engine, events = make_engine(tmp_path, popen_factory=factory)
    try:
        handle = engine.launch(task_brief="slow job")
        wait_for_state(engine, handle.job_id, {ArceusState.RUNNING.value})

        # Multiple heartbeat/poll cycles elapse while genuinely running.
        time.sleep(1.5)
        assert events == [], "no completion/model-wake event may fire while RUNNING"
        assert get_job(engine, handle.job_id)["state"] == ArceusState.RUNNING.value

        hang.set()
        wait_for_state(engine, handle.job_id, TERMINAL_STATES)
        assert len(events) == 1, "exactly one wake, only after the job actually completed"
    finally:
        engine.stop_watchdog()


# ---------------------------------------------------------------------------
# 8. Restart reconciliation
# ---------------------------------------------------------------------------


def test_restart_reconciliation(tmp_path):
    alive_pids = {7_000_001: 12345}  # pid -> start_time: "still genuinely running" post-restart
    engine, events = make_engine(tmp_path, popen_factory=make_popen_factory(), alive_pids=alive_pids)
    other_owner_pid = os.getpid() + 999_999  # guaranteed to differ from our own pid

    # (a) claude subprocess still alive after "restart" -> re-adopt, do NOT finalize yet.
    insert_job(engine, "arceus_still_running", state=ArceusState.RUNNING.value,
               owner_pid=other_owner_pid, claude_pid=7_000_001, claude_pid_started_at=12345,
               parent_task_id="parentA")

    # (b) claude subprocess is gone, no live thread anywhere -> finalize immediately (best-effort).
    insert_job(engine, "arceus_gone", state=ArceusState.RUNNING.value,
               owner_pid=other_owner_pid, claude_pid=7_000_099, parent_task_id="parentB")
    (engine._log_dir() / "arceus_gone.stdout.log").write_bytes(success_stdout())

    # (c) already terminal but never delivered (process died between the DB commit
    #     and the completion_queue.put() call in a prior run) -> must be redelivered.
    insert_job(engine, "arceus_undelivered", state=ArceusState.FAILED.value,
               owner_pid=other_owner_pid, parent_task_id="parentC", delivered=0)

    stats = engine.reconcile_on_startup()

    assert stats["reconciled_running"] == 2
    assert stats["redelivered"] == 1

    still_running = get_job(engine, "arceus_still_running")
    assert still_running["state"] == ArceusState.RUNNING.value, "must not be killed/finalized while genuinely alive"
    with engine._orphans_lock:
        assert "arceus_still_running" in engine._orphans, "must be handed to the watchdog to finish later"

    gone = get_job(engine, "arceus_gone")
    assert gone["state"] in TERMINAL_STATES
    assert gone["delivered"] == 1

    undelivered = get_job(engine, "arceus_undelivered")
    assert undelivered["state"] == ArceusState.FAILED.value, "redelivery must not mutate an already-terminal state"
    assert undelivered["delivered"] == 1

    delivered_ids = {e["arceus_job_id"] for e in events}
    assert delivered_ids == {"arceus_gone", "arceus_undelivered"}
    assert len(events) == 2

    # Now finish reconciling (a): its claude process exits, watchdog sweep must pick it up.
    alive_pids.pop(7_000_001)
    finalized = engine.watchdog_tick()
    assert finalized == 1
    assert get_job(engine, "arceus_still_running")["state"] in TERMINAL_STATES
    assert len(events) == 3


# ---------------------------------------------------------------------------
# Bonus: subscription auth hygiene (explicit requirement, cheap to pin down)
# ---------------------------------------------------------------------------


def test_stream_json_result_is_parsed_and_launch_is_bounded(tmp_path):
    stream = b"\n".join([
        json.dumps({"type": "system", "subtype": "init"}).encode(),
        json.dumps({"type": "assistant", "message": {"content": [{"type": "text", "text": "working"}]}}).encode(),
        json.dumps({"type": "result", "is_error": False, "result": "STATUS: PASS\\nSUMMARY: streamed"}).encode(),
    ])
    calls = []
    factory = make_popen_factory(stdout=stream, calls=calls)
    engine, _events = make_engine(
        tmp_path,
        popen_factory=factory,
        config={"effort": "medium", "no_session_persistence": True},
    )

    handle = engine.launch(task_brief="do work")
    row = wait_for_state(engine, handle.job_id, TERMINAL_STATES)
    assert row is not None

    assert row["state"] == ArceusState.SUCCEEDED.value
    assert "streamed" in (row["summary"] or "")
    cmd = calls[0]["cmd"]
    assert "--output-format" in cmd and "stream-json" in cmd
    assert "--verbose" in cmd
    assert "--effort" in cmd and "medium" in cmd
    assert "--no-session-persistence" in cmd


def test_child_env_strips_anthropic_api_key_without_touching_parent_env(tmp_path, monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-super-secret-should-never-reach-child")
    calls = []
    factory = make_popen_factory(stdout=success_stdout(), calls=calls)
    engine, _events = make_engine(tmp_path, popen_factory=factory)

    handle = engine.launch(task_brief="do work")
    wait_for_state(engine, handle.job_id, TERMINAL_STATES)

    assert len(calls) == 1
    child_env = calls[0]["env"]
    assert "ANTHROPIC_API_KEY" not in child_env, "subscription-mode child must not see the API key"
    assert child_env.get("PATH") == os.environ.get("PATH"), "sanitizer must not wipe the rest of the environment"
    # The parent/user's own shell environment must be untouched.
    assert os.environ.get("ANTHROPIC_API_KEY") == "sk-super-secret-should-never-reach-child"
