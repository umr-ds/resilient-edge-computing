from __future__ import annotations

import subprocess as sp
import time
from pathlib import Path

from tests.dtn.utils.integration_helpers import DtnTestEnvironment, requires_docker
from tests.dtn.utils.proc_helpers import run_and_expect_multiple, run_and_expect_single


@requires_docker
def test_connection_to_daemons_go(dtnd_go_env: DtnTestEnvironment) -> None:
    env = dtnd_go_env

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

    run_and_expect_multiple(
        processes=test_processes,
        timeout=10.0,
        required_messages={
            "broker": ["Connected to dtnd"],
            "datastore": ["Connected to dtnd"],
            "executor": ["Connected to dtnd"],
            "client": ["Connected to dtnd"],
        },
    )


@requires_docker
def test_execution_plan_once_go(dtnd_go_bde_env: DtnTestEnvironment) -> None:
    env = dtnd_go_bde_env

    # Start client
    client_cmd = (
        f"cd /app && uv run rec_dtn "
        f"--id dtn://client/ "
        f"--socket {env.socket_paths['client']} "
        f"-v "
        f"client exec tests/dtn/artifacts/execution_plans/execution_plan_once.toml dtn://datastore/"
    )
    client_proc = env.compose_env.popen("client-ns", client_cmd)

    run_and_expect_single(
        node_id="client",
        proc=client_proc,
        timeout=120.0,
        required_messages=[
            "Job submitted successfully",
            "Execution plan submitted",
        ],
    )

    # Check for the job result again and again until we find it or timeout
    start_time = time.time()
    execution_timeout = 120.0
    end_time = start_time + execution_timeout

    while True:
        if time.time() >= end_time:
            raise TimeoutError("Job result was not received within the expected time")

        # Start client
        client_cmd = (
            f"cd /app && uv run rec_dtn "
            f"--id dtn://client/ "
            f"--socket {env.socket_paths['client']} "
            f"-v "
            f"client check"
        )
        client_proc = env.compose_env.popen("client-ns", client_cmd)
        try:
            run_and_expect_single(
                node_id="client",
                proc=client_proc,
                timeout=30.0,
                required_messages=[
                    "Received job result",
                ],
            )
            return  # Test passed
        except AssertionError:
            pass  # Try again
