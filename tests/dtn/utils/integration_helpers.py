import subprocess as sp
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Iterator

import pytest

from tests.dtn.utils.compose_env import ComposeEnvironment


class DaemonType(Enum):
    """Enum for supported DTN daemon types."""

    GO = "go"
    RUST = "rust"


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
        daemon_type: The type of DTN daemon being used (Go or Rust).
        node_ids: List of node identifiers (e.g., ["broker", "datastore", "executor", "client"]).
        socket_paths: Mapping from node ID to its Unix socket path for communication.
        config_paths: Mapping from node ID to its TOML configuration file path.
        daemon_processes: Mapping from node ID to its running daemon Popen process.
    """

    compose_env: ComposeEnvironment
    daemon_type: DaemonType
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


def create_dtnd_config_rs(
    node_id: str,
    socket_dir: Path,
    store_path: Path,
) -> str:
    """
    Create a Rust DTN daemon configuration content as a string.

    Args:
        node_id: The node identifier.
        socket_dir: Directory path where the socket file will be created.
        store_path: Path to the directory where the daemon will store its data.

    Returns:
        Configuration file content as a TOML-formatted string.
    """
    socket_path = socket_dir / f"{node_id}.socket"

    config_content = f"""nodeid = "{node_id}"
debug = true
beacon-period = true

workdir = "{store_path}"
db = "mem"

recsocket = "{socket_path}"

[routing]
strategy = "epidemic"

[convergencylayers]
cla.0.id = "tcp"
cla.0.port = 16163

[core]
janitor = "10s"

[discovery]
interval = "2s"
peer-timeout = "20s"
port = 3003
"""

    return config_content


def _create_dtnd_env(
    daemon_type: DaemonType,
) -> Iterator[DtnTestEnvironment]:
    """
    Internal helper to create a DTN test environment with the specified daemon type.

    Args:
        daemon_type: The type of DTN daemon to use (Go or Rust).

    Yields:
        DtnTestEnvironment with the specified daemon running in each container.
    """
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

    # Select daemon binary based on type
    if daemon_type == DaemonType.GO:
        daemon_cmd_template = "dtnd-go {config_path}"
    else:
        daemon_cmd_template = "dtnd-rs -c {config_path}"

    with ComposeEnvironment(COMPOSE_FILE) as compose_env:
        # Create necessary directories in each container
        for service in service_mapping.values():
            compose_env.exec(service, f"mkdir -p {container_socket_dir}")
            compose_env.exec(service, f"mkdir -p {container_config_dir}")
            compose_env.exec(service, f"mkdir -p {container_store_base}")

        # Create config files and store directories for each node
        for node in nodes:
            service = service_mapping[node]

            if daemon_type == DaemonType.GO:
                config_content = create_dtnd_config_go(
                    node, container_socket_dir, store_paths[node]
                )
            else:
                config_content = create_dtnd_config_rs(
                    node,
                    container_socket_dir,
                    store_paths[node],
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
            cmd = daemon_cmd_template.format(config_path=config_path)
            daemon_processes[node] = compose_env.popen(service, cmd)

        # Give daemons a moment to start up
        time.sleep(1)

        # Verify all daemons are still running
        for node, proc in daemon_processes.items():
            if proc.poll() is not None:
                stdout, stderr = proc.communicate()
                raise RuntimeError(
                    f"Daemon '{node}' ({daemon_type.value}) failed to start. "
                    f"Exit code: {proc.returncode}, "
                    f"stdout: {stdout}, "
                    f"stderr: {stderr}"
                )

        yield DtnTestEnvironment(
            compose_env=compose_env,
            daemon_type=daemon_type,
            node_ids=nodes,
            socket_paths=socket_paths,
            config_paths=config_paths,
            daemon_processes=daemon_processes,
        )


@pytest.fixture(params=[DaemonType.GO, DaemonType.RUST], ids=["go", "rust"])
def dtnd_env(request: pytest.FixtureRequest) -> Iterator[DtnTestEnvironment]:
    """
    Parameterized fixture that provides a DTN test environment with both Go and Rust daemons.

    This fixture runs each test twice: once with the Go daemon and once with the Rust daemon.

    Yields:
        DtnTestEnvironment with either Go or Rust daemon running in each container.
    """
    yield from _create_dtnd_env(request.param)


@pytest.fixture
def dtnd_go_env() -> Iterator[DtnTestEnvironment]:
    """
    Provides a DTN test environment with four Docker containers using the Go daemon.

    Each container runs its own Go DTN Daemon (service name, node id):
    - broker-ns, dtn://broker/
    - datastore-ns, dtn://datastore/
    - executor-ns, dtn://executor/
    - client-ns, dtn://client/

    Yields:
        DtnTestEnvironment with Go daemon running in each container.
    """
    yield from _create_dtnd_env(DaemonType.GO)


@pytest.fixture
def dtnd_rs_env() -> Iterator[DtnTestEnvironment]:
    """
    Provides a DTN test environment with four Docker containers using the Rust daemon.

    Each container runs its own Rust DTN Daemon (service name, node id):
    - broker-ns, broker
    - datastore-ns, datastore
    - executor-ns, executor
    - client-ns, client

    Yields:
        DtnTestEnvironment with Rust daemon running in each container.
    """
    yield from _create_dtnd_env(DaemonType.RUST)


def _start_bde_services(env: DtnTestEnvironment) -> None:
    """
    Start broker, datastore, and executor services in the given environment.

    Args:
        env: The DTN test environment to start services in.
    """
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
    env.compose_env.popen("broker-ns", broker_cmd)

    # Start datastore
    datastore_cmd = (
        f"cd /app && uv run rec_dtn "
        f"--id dtn://datastore/ "
        f"--socket {env.socket_paths['datastore']} "
        f"-v "
        f"datastore {datastore_root}"
    )
    env.compose_env.popen("datastore-ns", datastore_cmd)

    # Start executor
    executor_cmd = (
        f"cd /app && uv run rec_dtn "
        f"--id dtn://executor/ "
        f"--socket {env.socket_paths['executor']} "
        f"-v "
        f"executor {executor_root}"
    )
    env.compose_env.popen("executor-ns", executor_cmd)


@pytest.fixture
def dtnd_bde_env(dtnd_env: DtnTestEnvironment) -> Iterator[DtnTestEnvironment]:
    """
    Parameterized fixture with broker, datastore, and executor already running.

    This fixture extends dtnd_env (parameterized for both Go and Rust) by starting
    the broker, datastore, and executor services, leaving the client available
    for test-specific use.

    Yields:
        DtnTestEnvironment with broker, datastore, and executor services running.
    """
    _start_bde_services(dtnd_env)
    yield dtnd_env


@pytest.fixture
def dtnd_go_bde_env(dtnd_go_env: DtnTestEnvironment) -> Iterator[DtnTestEnvironment]:
    """
    Provides a Go DTN test environment with broker, datastore, and executor already running.

    This fixture extends dtnd_go_env by starting the broker, datastore, and executor services,
    leaving the client available for test-specific use.

    Yields:
        DtnTestEnvironment with Go daemon and broker, datastore, and executor services running.
    """
    _start_bde_services(dtnd_go_env)
    yield dtnd_go_env


@pytest.fixture
def dtnd_rs_bde_env(dtnd_rs_env: DtnTestEnvironment) -> Iterator[DtnTestEnvironment]:
    """
    Provides a Rust DTN test environment with broker, datastore, and executor already running.

    This fixture extends dtnd_rs_env by starting the broker, datastore, and executor services,
    leaving the client available for test-specific use.

    Yields:
        DtnTestEnvironment with Rust daemon and broker, datastore, and executor services running.
    """
    _start_bde_services(dtnd_rs_env)
    yield dtnd_rs_env
