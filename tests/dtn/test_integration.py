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
