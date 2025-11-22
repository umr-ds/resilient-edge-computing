from __future__ import annotations

import queue
import subprocess as sp
import threading
import time
from dataclasses import dataclass
from typing import IO, Iterable, Mapping


@dataclass
class ProcessOutput:
    """
    Result of a process run.

    Attributes:
        node_id: Name of the process.
        returncode: Process exit code.
        stdout: Captured stdout.
        stderr: Captured stderr.
        duration: Time between start of waiting and completion.
        timed_out: True if the process exceeded the timeout and was killed.
    """

    node_id: str
    returncode: int
    stdout: str
    stderr: str
    duration: float
    timed_out: bool


@dataclass
class _ProcessState:
    """
    Internal state tracking for a single monitored process.

    Attributes:
        proc: The subprocess.Popen instance.
        reqs: Mapping of required message -> expected count.
        stdout: Captured stdout.
        stderr: Captured stderr.
        satisfied: True if all requirements have been met.
        returncode: The process's return code once it has exited, or None if still running.
    """

    proc: sp.Popen[str]
    reqs: Mapping[str, int]
    stdout: str = ""
    stderr: str = ""
    satisfied: bool = False
    returncode: int | None = None


def _normalize_requirements(
    required_messages: Iterable[str] | Mapping[str, int],
) -> dict[str, int]:
    """
    Normalize requirements into a mapping of message -> count.
    Removes any zero-count or negative-count entries.

    Args:
        required_messages: Either an iterable of messages (each must appear once), or a mapping of message -> count.

    Returns:
        dict[str, int]: Mapping of message -> expected count.
    """
    if isinstance(required_messages, Mapping):
        return {msg: count for msg, count in required_messages.items() if count > 0}
    return {msg: 1 for msg in required_messages}


def _check_requirements(
    combined_output: str, required_messages: Mapping[str, int]
) -> list[str]:
    """
    Checks if the combined output meets the message count requirements.

    Args:
        combined_output: The combined stdout and stderr of a process.
        required_messages: Mapping of message -> expected count.

    Returns:
        list[str]: List of requirement messages that were not met.
    """
    failures = []
    for msg, expected_count in required_messages.items():
        actual_count = combined_output.count(msg)
        if actual_count != expected_count:
            failures.append(f"{msg} (expected {expected_count}, found {actual_count})")
    return failures


def _stream_reader(
    node_id: str,
    stream_name: str,
    pipe: IO[str],
    q: queue.Queue,
) -> None:
    """
    Reads lines from a process stream and puts them into a queue.

    Args:
        node_id: Logical name of the process.
        stream_name: "stdout" or "stderr".
        pipe: The stream to read from.
        q: Queue to put the read lines into.
    """
    # Read until EOF
    for line in iter(pipe.readline, ""):
        q.put((node_id, stream_name, line))


def _monitor_processes(
    processes: Mapping[str, sp.Popen[str]],
    timeout: float,
    required_messages: Mapping[str, Mapping[str, int]],
    terminate_on_success: bool,
) -> dict[str, ProcessOutput]:
    """
    Monitors multiple processes, capturing their output and checking for required messages.
    Can terminate processes early if requirements are met.

    Args:
        processes: Mapping node_id -> Popen[str].
        timeout: Per-process timeout in seconds.
        required_messages: Mapping node_id -> Mapping of required message -> count.
        terminate_on_success: If True, kills ALL processes immediately once ALL processes have found their required messages.

    Returns:
        Mapping node_id -> ProcessOutput.
    """
    start = time.monotonic()

    # Per process state
    proc_states: dict[str, _ProcessState] = {}
    for node_id, proc in processes.items():
        proc_states[node_id] = _ProcessState(
            proc=proc,
            reqs=required_messages.get(node_id, {}),
            satisfied=not required_messages.get(node_id, {}),  # True if no requirements
        )

    # Queue of (node_id, stream_name, line)
    event_queue: queue.Queue[tuple[str, str, str]] = queue.Queue()
    threads: list[threading.Thread] = []

    # Start reader threads
    for node_id, state in proc_states.items():
        if state.proc.stdout:
            t = threading.Thread(
                target=_stream_reader,
                args=(node_id, "stdout", state.proc.stdout, event_queue),
                daemon=True,
            )
            t.start()
            threads.append(t)

        if state.proc.stderr:
            t = threading.Thread(
                target=_stream_reader,
                args=(node_id, "stderr", state.proc.stderr, event_queue),
                daemon=True,
            )
            t.start()
            threads.append(t)

    timed_out = False

    try:
        while True:
            # Success Check
            if terminate_on_success:
                if all(state.satisfied for state in proc_states.values()):
                    break

            # Timeout Check
            if time.monotonic() - start > timeout:
                timed_out = True
                break

            # Exit Check
            if not terminate_on_success:
                if all(state.proc.poll() is not None for state in proc_states.values()):
                    # Processes have exited, but we may still have output to read
                    if event_queue.empty():
                        break

            # Fetch Data
            try:
                # Briefly wait for new data
                node_id, stream, line = event_queue.get(timeout=0.1)

                # Store Data
                state = proc_states[node_id]
                if stream == "stdout":
                    state.stdout += line
                else:
                    state.stderr += line

                # Check requirements if not yet satisfied
                if not state.satisfied:
                    missing = _check_requirements(
                        combined_output=state.stdout + "\n" + state.stderr,
                        required_messages=state.reqs,
                    )

                    if not missing:
                        state.satisfied = True

            except queue.Empty:
                # No new data in 0.1s
                # Try again
                continue

    finally:
        # Cleanup
        for state in proc_states.values():
            proc = state.proc
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=0.5)
                except sp.TimeoutExpired:
                    proc.kill()
            state.returncode = proc.returncode

    # Collect results
    results: dict[str, ProcessOutput] = {}
    for node_id, state in proc_states.items():
        results[node_id] = ProcessOutput(
            node_id=node_id,
            returncode=state.returncode or -1,
            stdout=state.stdout,
            stderr=state.stderr,
            duration=time.monotonic() - start,
            timed_out=timed_out,
        )

    return results


def run_and_expect_single(
    node_id: str,
    proc: sp.Popen[str],
    timeout: float,
    required_messages: Iterable[str] | Mapping[str, int],
    terminate_on_success: bool = False,
) -> ProcessOutput:
    """
    Wait for a single process with a timeout.
    Asserts that its output contains required messages.

    Args:
        node_id: Logical name of the process.
        proc: The process to run.
        timeout: Timeout in seconds.
        required_messages: Either an iterable of messages (each must appear once), or a mapping of message -> count.
        terminate_on_success: If True, kills the process immediately once required messages are found, without waiting for timeout.

    Raises:
        AssertionError with details if any message requirement is not met.

    Returns:
        ProcessOutput: The result of the process run.
    """
    normalized_reqs = _normalize_requirements(required_messages)

    results = _monitor_processes(
        processes={node_id: proc},
        timeout=timeout,
        required_messages={node_id: normalized_reqs},
        terminate_on_success=terminate_on_success,
    )

    output = results[node_id]

    # Validation
    failures = _check_requirements(
        combined_output=output.stdout + "\n" + output.stderr,
        required_messages=normalized_reqs,
    )

    if failures:
        raise AssertionError(
            f"{node_id} missing messages: {', '.join(failures)}. "
            f"stdout: {output.stdout[:500]}, stderr: {output.stderr[:500]}"
        )

    return output


def run_and_expect_multiple(
    processes: Mapping[str, sp.Popen[str]],
    timeout: float,
    required_messages: Mapping[str, Iterable[str] | Mapping[str, int]],
    terminate_on_success: bool = False,
) -> dict[str, ProcessOutput]:
    """
    Wait for multiple processes in parallel, each with the same per-process timeout.
    Asserts that each process's output contains required messages.

    Args:
        processes: Mapping node_id -> Popen[str].
        timeout: Per-process timeout in seconds.
        required_messages: Mapping node_id -> (Iterable or Mapping) of required messages.
        terminate_on_success: If True, kills ALL processes immediately once ALL processes have found their required messages.

    Raises:
        AssertionError with details if any message requirement is not met.

    Returns:
        Mapping node_id -> ProcessOutput.
    """
    if not processes:
        return {}

    normalized_reqs = {
        node_id: _normalize_requirements(msgs)
        for node_id, msgs in required_messages.items()
    }

    outputs = _monitor_processes(
        processes=processes,
        timeout=timeout,
        required_messages=normalized_reqs,
        terminate_on_success=terminate_on_success,
    )

    # Validation
    for node_id, output in outputs.items():
        failures = _check_requirements(
            combined_output=output.stdout + "\n" + output.stderr,
            required_messages=normalized_reqs.get(node_id, {}),
        )
        if failures:
            raise AssertionError(
                f"{node_id} missing messages: {', '.join(failures)}. "
                f"stdout: {output.stdout[:500]}, stderr: {output.stderr[:500]}"
            )

    return outputs
