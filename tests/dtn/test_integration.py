from __future__ import annotations

import subprocess as sp
from pathlib import Path

from tests.dtn.utils.integration_helpers import DtnTestEnvironment, requires_docker
from tests.dtn.utils.proc_helpers import run_and_expect_multiple, run_and_expect_single


@requires_docker
def test_connection_to_daemons(dtnd_env: DtnTestEnvironment) -> None:
    env = dtnd_env

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
        f"client --results_dir /tmp/test_data/client_results "
        f"query dtn://test-submitter/"
    )
    test_processes["client"] = env.compose_env.popen("client-ns", client_cmd)

    run_and_expect_multiple(
        processes=test_processes,
        timeout=30.0,
        required_messages={
            "broker": ["Successfully registered with dtnd"],
            "datastore": ["Successfully registered with dtnd"],
            "executor": ["Successfully registered with dtnd"],
            "client": ["Successfully registered with dtnd"],
        },
        terminate_on_success=True,
    )


@requires_docker
def test_execution_plan_once(dtnd_bde_env: DtnTestEnvironment) -> None:
    env = dtnd_bde_env

    results_dir = Path("/tmp/test_data/client_results/")

    # Start client with exec command
    exec_cmd = (
        f"cd /app && uv run rec_dtn "
        f"--id dtn://client/ "
        f"--socket {env.socket_paths['client']} "
        f"-v "
        f"client --results_dir {results_dir} "
        f"exec tests/dtn/artifacts/execution_plans/execution_plan_once.toml"
    )
    exec_proc = env.compose_env.popen("client-ns", exec_cmd)

    run_and_expect_single(
        node_id="client",
        proc=exec_proc,
        timeout=120.0,
        required_messages=[
            "Job submitted successfully",
            "Execution plan submitted",
        ],
    )

    # Start client with listen command to retrieve any result bundles from daemon
    check_cmd = (
        f"cd /app && uv run rec_dtn "
        f"--id dtn://client/ "
        f"--socket {env.socket_paths['client']} "
        f"-v "
        f"client --results_dir {results_dir} listen"
    )
    check_proc = env.compose_env.popen("client-ns", check_cmd)
    run_and_expect_single(
        node_id="client",
        proc=check_proc,
        timeout=120.0,
        required_messages=[
            "Received job result",
        ],
        terminate_on_success=True,
    )

    # Check that the results file exists in the client results directory
    exec_result = env.compose_env.exec("client-ns", f"ls {results_dir}")
    result_files = [f for f in exec_result.stdout.split() if f.endswith("_result.zip")]
    assert (
        len(result_files) == 1
    ), f"Expected 1 job result file, found {len(result_files)} in client results: {exec_result.stdout}"


@requires_docker
def test_execution_plan_twice_named(dtnd_bde_env: DtnTestEnvironment) -> None:
    env = dtnd_bde_env

    results_dir = Path("/tmp/test_data/client_results/")

    # Start client with exec command
    exec_cmd = (
        f"cd /app && uv run rec_dtn "
        f"--id dtn://client/ "
        f"--socket {env.socket_paths['client']} "
        f"-v "
        f"client --results_dir {results_dir} "
        f"exec tests/dtn/artifacts/execution_plans/execution_plan_twice_named.toml"
    )
    exec_proc = env.compose_env.popen("client-ns", exec_cmd)

    run_and_expect_single(
        node_id="client",
        proc=exec_proc,
        timeout=120.0,
        required_messages={
            "Successfully published": 4,
            "Job submitted successfully": 2,
            "Execution plan submitted": 1,
        },
    )

    # Start client with listen command to retrieve any result bundles from daemon
    check_cmd = (
        f"cd /app && uv run rec_dtn "
        f"--id dtn://client/ "
        f"--socket {env.socket_paths['client']} "
        f"-v "
        f"client --results_dir {results_dir} listen"
    )
    check_proc = env.compose_env.popen("client-ns", check_cmd)
    run_and_expect_single(
        node_id="client",
        proc=check_proc,
        timeout=120.0,
        required_messages={
            "Received job result": 2,
        },
        terminate_on_success=True,
    )

    # Check that the results file exists in the client results directory
    exec_result = env.compose_env.exec("client-ns", f"ls {results_dir}")
    result_files = [f for f in exec_result.stdout.split() if f.endswith("_result.zip")]
    assert (
        len(result_files) == 2
    ), f"Expected 2 job result files, found {len(result_files)} in client results: {exec_result.stdout}"
