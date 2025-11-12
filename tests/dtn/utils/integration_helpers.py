import subprocess as sp
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Generator

import pytest

from tests.dtn.utils.compose_env import ComposeEnvironment


def _check_docker_support() -> tuple[bool, str]:
    """
    Check if Docker and Docker Compose are available.

    Returns:
        Tuple of (is_supported, reason_if_not_supported)
    """
    try:
        sp.run(
            ["docker", "version"],
            capture_output=True,
            check=True,
            timeout=5,
        )
        sp.run(
            ["docker", "compose", "version"],
            capture_output=True,
            check=True,
            timeout=5,
        )
        return True, ""
    except Exception as e:
        return False, f"Error checking Docker support: {e}"


_DOCKER_SUPPORTED, _DOCKER_SKIP_REASON = _check_docker_support()

# Pytest marker to skip tests requiring Docker support
requires_docker = pytest.mark.skipif(
    not _DOCKER_SUPPORTED,
    reason=f"Docker support required but not available: {_DOCKER_SKIP_REASON}",
)

HERE = Path(__file__).resolve().parent
ARTIFACTS_DIR = HERE.parent / "artifacts"
COMPOSE_FILE = ARTIFACTS_DIR / "compose.integration-test.yml"


@dataclass
class DtnTestEnvironment:
    """
    Test environment for integration tests.

    Attributes:
        compose_env: ComposeEnvironment managing the Docker containers.
        node_ids: List of node identifiers (e.g., ["broker", "datastore", "executor", "client"]).
        socket_paths: Mapping from node ID to its Unix socket path for communication.
        config_paths: Mapping from node ID to its TOML configuration file path.
        daemon_processes: Mapping from node ID to its running daemon Popen process.
    """

    compose_env: ComposeEnvironment
    node_ids: list[str]
    socket_paths: dict[str, Path]
    config_paths: dict[str, Path]
    daemon_processes: dict[str, sp.Popen[str]]


def create_dtnd_config_go(
    node_id: str,
    socket_dir: Path,
    store_path: Path,
) -> str:
    """
    Create a Go DTN daemon configuration content as a string.

    Args:
        node_id: The node identifier.
            Will be used to construct the full DTN URI as "dtn://{node_id}/".
        socket_dir: Directory path where the socket file will be created.
        store_path: Path to the directory where the daemon will store its data.

    Returns:
        Configuration file content as a TOML-formatted string.
    """
    socket_path = socket_dir / f"{node_id}.socket"

    config_content = f"""node_id = "dtn://{node_id}/"
log_level = "Debug"

[Store]
path = "{store_path}"

[Routing]
algorithm = "epidemic"

[Agents]
[Agents.REC]
socket = "{socket_path}"

[[Listener]]
type = "QUICL"
address = ":35037"

[Cron]
dispatch = "10s"
gc = "1h15s"
"""

    return config_content


@pytest.fixture
def broker_datastore_executor_client_go_env() -> (
    Generator[DtnTestEnvironment, None, None]
):
    """
    Provides a DTN test environment with four Docker containers.
    Each container runs its own Go DTN Daemon (service name, node id):
    - broker-ns, dtn://broker/
    - datastore-ns, dtn://datastore/
    - executor-ns, dtn://executor/
    - client-ns, dtn://client/

    The containers are managed via Docker Compose and automatically cleaned up when the test completes.

    Yields:
        DtnTestEnvironment containing:
            - compose_env: ComposeEnvironment for executing commands in containers
            - node_ids: List of node identifiers
            - socket_paths: Container paths to Unix domain sockets for each node
            - config_paths: Container paths to TOML config files for each daemon
            - daemon_processes: Running dtnd-go daemon processes for each node
    """
    # Create configuration files for each DTN daemon
    nodes = ["broker", "datastore", "executor", "client"]
    service_mapping = {
        "broker": "broker-ns",
        "datastore": "datastore-ns",
        "executor": "executor-ns",
        "client": "client-ns",
    }

    # Paths inside the container
    container_socket_dir = Path("/tmp/test_data")
    container_store_base = Path("/tmp/test_data/stores")
    container_config_dir = Path("/tmp/test_data/configs")

    socket_paths = {node: container_socket_dir / f"{node}.socket" for node in nodes}
    config_paths = {node: container_config_dir / f"{node}.toml" for node in nodes}
    store_paths = {node: container_store_base / node for node in nodes}

    with ComposeEnvironment(COMPOSE_FILE) as compose_env:
        # Create necessary directories in each container
        for service in service_mapping.values():
            compose_env.exec(service, f"mkdir -p {container_socket_dir}")
            compose_env.exec(service, f"mkdir -p {container_config_dir}")
            compose_env.exec(service, f"mkdir -p {container_store_base}")

        # Create config files and store directories for each node
        for node in nodes:
            service = service_mapping[node]
            config_content = create_dtnd_config_go(
                node, container_socket_dir, store_paths[node]
            )

            # Write config file to container
            compose_env.exec(
                service, f"cat > {config_paths[node]} << 'EOF'\n{config_content}\nEOF"
            )

            # Create store directory
            compose_env.exec(service, f"mkdir -p {store_paths[node]}")

        # Start DTN Daemon in each container
        daemon_processes = {}
        for node in nodes:
            service = service_mapping[node]
            config_path = config_paths[node]
            cmd = f"dtnd-go {config_path}"
            daemon_processes[node] = compose_env.popen(service, cmd)

        # Give daemons a moment to start up
        time.sleep(1)

        # Verify all daemons are still running
        for node, proc in daemon_processes.items():
            if proc.poll() is not None:
                stdout, stderr = proc.communicate()
                raise RuntimeError(
                    f"Daemon '{node}' failed to start. "
                    f"Exit code: {proc.returncode}, "
                    f"stdout: {stdout}, "
                    f"stderr: {stderr}"
                )

        yield DtnTestEnvironment(
            compose_env=compose_env,
            node_ids=nodes,
            socket_paths=socket_paths,
            config_paths=config_paths,
            daemon_processes=daemon_processes,
        )
