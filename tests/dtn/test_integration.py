from __future__ import annotations

import subprocess as sp
import time
from pathlib import Path

from tests.dtn.utils.integration_helpers import DtnTestEnvironment, requires_docker


@requires_docker
def test_connection_to_daemons_go(
    broker_datastore_executor_client_go_env: DtnTestEnvironment,
) -> None:
    env = broker_datastore_executor_client_go_env

    # Verify all daemons are still running
    for node_id, proc in env.daemon_processes.items():
        assert proc.poll() is None, f"Daemon '{node_id}' is not running"

    # Start long-running Python server nodes in their respective containers
    test_processes: dict[str, sp.Popen[str]] = {}

    # Container paths for data storage
    datastore_root = Path("/tmp/test_data/datastore_root")
    executor_root = Path("/tmp/test_data/executor_root")

    # Create data directories in containers
    env.compose_env.exec("datastore-ns", f"mkdir -p {datastore_root}")
    env.compose_env.exec("executor-ns", f"mkdir -p {executor_root}")

    # Start broker
    broker_cmd = (
        f"cd /app && uv run rec_dtn "
        f"--id dtn://broker/ "
        f"--socket {env.socket_paths['broker']} "
        f"-v "
        f"broker"
    )
    test_processes["broker"] = env.compose_env.popen("broker-ns", broker_cmd)

    # Start datastore
    datastore_cmd = (
        f"cd /app && uv run rec_dtn "
        f"--id dtn://datastore/ "
        f"--socket {env.socket_paths['datastore']} "
        f"-v "
        f"datastore {datastore_root}"
    )
    test_processes["datastore"] = env.compose_env.popen("datastore-ns", datastore_cmd)

    # Start executor
    executor_cmd = (
        f"cd /app && uv run rec_dtn "
        f"--id dtn://executor/ "
        f"--socket {env.socket_paths['executor']} "
        f"-v "
        f"executor {executor_root}"
    )
    test_processes["executor"] = env.compose_env.popen("executor-ns", executor_cmd)

    # Start client
    client_cmd = (
        f"cd /app && uv run rec_dtn "
        f"--id dtn://client/ "
        f"--socket {env.socket_paths['client']} "
        f"-v "
        f"client query dtn://test-submitter/"
    )
    test_processes["client"] = env.compose_env.popen("client-ns", client_cmd)

    start_time = time.time()
    connection_timeout = 10.0
    end_time = start_time + connection_timeout

    # Check each process for successful connection by looking for "Connected to dtnd" in stderr
    failed_nodes = []
    connected_nodes = set()

    while len(connected_nodes) < len(test_processes):
        if time.time() >= end_time:
            for node_id in test_processes.keys():
                if node_id not in connected_nodes:
                    failed_nodes.append(
                        f"{node_id} (timeout after {connection_timeout}s waiting for connection confirmation)"
                    )
            break

        for node_id, proc in test_processes.items():
            # Skip nodes that already connected
            if node_id in connected_nodes:
                continue

            # Check if process has crashed
            if proc.poll() is not None:
                _stdout, stderr = proc.communicate()
                failed_nodes.append(
                    f"{node_id} (exited with code {proc.returncode}): {stderr[:300]}"
                )
                # Mark as done (failed)
                connected_nodes.add(node_id)
                continue

            line = proc.stderr.readline()
            if line:
                if "Connected to dtnd" in line:
                    connected_nodes.add(node_id)

        # Small sleep to avoid busy-waiting
        time.sleep(0.05)

    # Note: ComposeEnvironment.__exit__ will clean up all containers

    assert (
        len(failed_nodes) == 0
    ), f"Some Python nodes failed to connect to their daemons: {', '.join(failed_nodes)}"


@requires_docker
def test_simple_job_execution_go(
    broker_datastore_executor_client_go_env: DtnTestEnvironment,
) -> None:
    env = broker_datastore_executor_client_go_env

    # Start long-running Python server nodes in their respective containers
    test_processes: dict[str, sp.Popen[str]] = {}

    # Container paths for data storage
    datastore_root = Path("/tmp/test_data/datastore_root")
    executor_root = Path("/tmp/test_data/executor_root")

    # Create data directories in containers
    env.compose_env.exec("datastore-ns", f"mkdir -p {datastore_root}")
    env.compose_env.exec("executor-ns", f"mkdir -p {executor_root}")

    # Start broker
    broker_cmd = (
        f"cd /app && uv run rec_dtn "
        f"--id dtn://broker/ "
        f"--socket {env.socket_paths['broker']} "
        f"-v "
        f"broker"
    )
    test_processes["broker"] = env.compose_env.popen("broker-ns", broker_cmd)

    # Start datastore
    datastore_cmd = (
        f"cd /app && uv run rec_dtn "
        f"--id dtn://datastore/ "
        f"--socket {env.socket_paths['datastore']} "
        f"-v "
        f"datastore {datastore_root}"
    )
    test_processes["datastore"] = env.compose_env.popen("datastore-ns", datastore_cmd)

    # Start executor
    executor_cmd = (
        f"cd /app && uv run rec_dtn "
        f"--id dtn://executor/ "
        f"--socket {env.socket_paths['executor']} "
        f"-v "
        f"executor {executor_root}"
    )
    test_processes["executor"] = env.compose_env.popen("executor-ns", executor_cmd)

    # Start client
    client_cmd = (
        f"cd /app && uv run rec_dtn "
        f"--id dtn://client/ "
        f"--socket {env.socket_paths['client']} "
        f"-v "
        f"client exec tests/dtn/artifacts/execution_plans/execution_plan_simple.toml dtn://datastore/"
    )
    test_processes["client"] = env.compose_env.popen("client-ns", client_cmd)

    start_time = time.time()
    execution_timeout = 120.0
    end_time = start_time + execution_timeout

    # Wait for execution plan submission to complete
    required_messages = {
        "Job submitted successfully",
        "Execution plan submitted",
    }
    found_messages = set()
    client_proc = test_processes["client"]

    while len(found_messages) < len(required_messages):
        if time.time() >= end_time:
            missing_messages = required_messages - found_messages
            assert False, (
                f"Execution plan did not complete within {execution_timeout}s. "
                f"Missing messages: {', '.join(missing_messages)}"
            )

        # Check if client process has exited
        if client_proc.poll() is not None:
            # Read any remaining output
            _stdout, stderr = client_proc.communicate()
            for line in stderr.splitlines():
                for message in required_messages:
                    if message in line and message not in found_messages:
                        found_messages.add(message)

            # All messages found
            if len(found_messages) == len(required_messages):
                break

            # Otherwise, the client exited prematurely
            missing_messages = required_messages - found_messages
            assert False, (
                f"Client exited with code {client_proc.returncode} before all messages were found. "
                f"Missing messages: {', '.join(missing_messages)}. "
                f"stderr: {stderr[:500]}"
            )

        line = client_proc.stderr.readline()
        if line:
            for message in required_messages:
                if message in line and message not in found_messages:
                    found_messages.add(message)

        # Small sleep to avoid busy-waiting
        time.sleep(0.05)

    assert len(found_messages) == len(
        required_messages
    ), f"Execution plan completed successfully with all required messages: {', '.join(found_messages)}"

    # Wait 30s for the job execution to complete
    time.sleep(30)

    # Start client
    client_cmd = (
        f"cd /app && uv run rec_dtn "
        f"--id dtn://client/ "
        f"--socket {env.socket_paths['client']} "
        f"-v "
        f"client check"
    )
    test_processes["client"] = env.compose_env.popen("client-ns", client_cmd)

    start_time = time.time()
    execution_timeout = 120.0
    end_time = start_time + execution_timeout

    # Wait for results
    required_messages = {
        "Received job result",
    }
    found_messages = set()
    client_proc = test_processes["client"]

    while len(found_messages) < len(required_messages):
        if time.time() >= end_time:
            missing_messages = required_messages - found_messages
            assert False, (
                f"Execution plan did not complete within {execution_timeout}s. "
                f"Missing messages: {', '.join(missing_messages)}"
            )

        # Check if client process has exited
        if client_proc.poll() is not None:
            # Read any remaining output
            _stdout, stderr = client_proc.communicate()
            for line in stderr.splitlines():
                for message in required_messages:
                    if message in line and message not in found_messages:
                        found_messages.add(message)

            # All messages found
            if len(found_messages) == len(required_messages):
                break

            # Otherwise, the client exited prematurely
            missing_messages = required_messages - found_messages
            assert False, (
                f"Client exited with code {client_proc.returncode} before all messages were found. "
                f"Missing messages: {', '.join(missing_messages)}. "
                f"stderr: {stderr[:500]}"
            )

        line = client_proc.stderr.readline()
        if line:
            for message in required_messages:
                if message in line and message not in found_messages:
                    found_messages.add(message)

        # Small sleep to avoid busy-waiting
        time.sleep(0.05)

    assert len(found_messages) == len(
        required_messages
    ), f"Execution plan completed successfully with all required messages: {', '.join(found_messages)}"
