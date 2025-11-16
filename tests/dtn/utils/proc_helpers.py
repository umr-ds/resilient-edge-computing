from __future__ import annotations

import subprocess as sp
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Iterable, Mapping


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


def _communicate_with_timeout(
    node_id: str,
    proc: sp.Popen[str],
    timeout: float,
) -> ProcessOutput:
    """
    Wait for a single process with a timeout, kill on timeout, and capture output.

    Args:
        node_id: Name of the process.
        proc: The process to run.
        timeout: Timeout in seconds.

    Returns:
        ProcessOutput: The result of the process run.
    """
    start = time.monotonic()
    timed_out = False

    try:
        stdout, stderr = proc.communicate(timeout=timeout)
    except sp.TimeoutExpired:
        timed_out = True
        # Kill and drain remaining output
        proc.kill()
        stdout, stderr = proc.communicate()

    duration = time.monotonic() - start

    # communicate() may return None if no output
    stdout = stdout or ""
    stderr = stderr or ""

    return ProcessOutput(
        node_id=node_id,
        returncode=proc.returncode,
        stdout=stdout,
        stderr=stderr,
        duration=duration,
        timed_out=timed_out,
    )


def _expect_in_output(
    output: ProcessOutput,
    required_messages: Iterable[str] | Mapping[str, int],
) -> None:
    """
    Assert that the process output contains all required messages.

    Args:
        output: The process output to check.
        required_messages: Either an iterable of messages (each must appear once), or a mapping of message -> count.

    Raises:
        AssertionError with details if any message requirement is not met.
    """
    combined = output.stdout + "\n" + output.stderr

    # Normalize to dict format: message -> count
    if isinstance(required_messages, Mapping):
        requirements = required_messages
    else:
        requirements = {msg: 1 for msg in required_messages}

    failures = []
    for msg, expected_count in requirements.items():
        actual_count = combined.count(msg)
        if actual_count != expected_count:
            failures.append(f"{msg} (expected {expected_count}, found {actual_count})")

    if failures:
        raise AssertionError(
            f"{output.node_id} missing messages: {', '.join(failures)}. "
            f"stdout: {output.stdout[:500]}, stderr: {output.stderr[:500]}"
        )


def run_and_expect_single(
    node_id: str,
    proc: sp.Popen[str],
    timeout: float,
    required_messages: Iterable[str] | Mapping[str, int],
) -> ProcessOutput:
    """
    Wait for a single process with a timeout.
    Asserts that its output contains required messages.

    Args:
        node_id: Logical name of the process.
        proc: The process to run.
        timeout: Timeout in seconds.
        required_messages: Either an iterable of messages (each must appear once), or a mapping of message -> count.

    Raises:
        AssertionError with details if any message requirement is not met.

    Returns:
        ProcessOutput: The result of the process run.
    """
    result = _communicate_with_timeout(node_id, proc, timeout)
    _expect_in_output(result, required_messages)

    return result


def run_and_expect_multiple(
    processes: Mapping[str, sp.Popen[str]],
    timeout: float,
    required_messages: Mapping[str, Iterable[str] | Mapping[str, int]],
) -> dict[str, ProcessOutput]:
    """
    Wait for multiple processes in parallel, each with the same per-process timeout.
    Asserts that each process's output contains required messages.

    Args:
        processes: Mapping node_id -> Popen[str].
        timeout: Per-process timeout in seconds.
        required_messages: Mapping node_id -> (Iterable or Mapping) of required messages.

    Raises:
        AssertionError with details if any message requirement is not met.

    Returns:
        Mapping node_id -> ProcessOutput.
    """
    results: dict[str, ProcessOutput] = {}

    if not processes:
        return results

    with ThreadPoolExecutor(max_workers=len(processes)) as executor:
        future_to_node: dict = {
            executor.submit(_communicate_with_timeout, node_id, proc, timeout): node_id
            for node_id, proc in processes.items()
        }

        for future in as_completed(future_to_node):
            node_id = future_to_node[future]
            results[node_id] = future.result()

    for node_id, output in results.items():
        _expect_in_output(output, required_messages.get(node_id, []))

    return results
