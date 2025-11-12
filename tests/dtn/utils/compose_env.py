from __future__ import annotations

import json
import subprocess as sp
import time
import uuid
from dataclasses import dataclass
from pathlib import Path


@dataclass
class ExecResult:
    """
    Result of executing a command in a container.

    Attributes:
        exit_code: Exit code of the command.
        stdout: Standard output captured from the command.
        stderr: Standard error output captured from the command.
    """

    exit_code: int
    stdout: str
    stderr: str


class ComposeEnvironment:
    """
    Manages isolated containers using Docker Compose.
    """

    _services: set[str]
    _project_name: str
    _compose_file: Path
    _container_names: dict[str, str]

    def __init__(
        self,
        compose_file: Path,
        services: set[str] | None = None,
    ) -> None:
        """
        Initialize the ComposeEnvironment with service configuration.

        Args:
            services: Set of service names to use, or None to use all services from the compose file.
            compose_file: Path to the docker-compose.yml file.

        Raises:
            ValueError: If services set is empty or contains services not defined in the compose file.
        """
        self._compose_file = compose_file
        self._project_name = f"compose_env_{uuid.uuid4()}"
        self._container_names = {}

        config = self._get_compose_config()
        available_services = set(config.get("services", {}).keys())

        if services is None:
            # Use all available services
            self._services = available_services
        else:
            self._services = services
            unknown_services = self._services - available_services

            if unknown_services:
                raise ValueError(
                    f"Unknown services: {unknown_services}. "
                    f"Available services in compose file: {available_services}"
                )

        if len(self._services) == 0:
            raise ValueError("Compose file must contain at least one service")

        for service in self._services:
            service_config = config["services"][service]
            if "container_name" in service_config:
                # Use explicit container_name if set
                self._container_names[service] = service_config["container_name"]
            else:
                # Docker Compose default: projectname-servicename-1
                self._container_names[service] = f"{self._project_name}-{service}-1"

    @property
    def services(self) -> set[str]:
        """
        Get the set of services managed by this environment.

        Returns:
            Set of service names.
        """
        return self._services

    def _get_compose_config(self) -> dict:
        """
        Get the parsed compose configuration.

        Returns:
            Parsed compose config as a dictionary.
        """
        result = sp.run(
            [
                "docker",
                "compose",
                "-f",
                str(self._compose_file),
                "-p",
                self._project_name,
                "config",
                "--format",
                "json",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        return json.loads(result.stdout)

    def __enter__(self) -> ComposeEnvironment:
        """
        Enter the context manager and set up the Docker Compose environment.

        Builds images and starts only the requested services.

        Returns:
            Self for use in with statement.

        Raises:
            RuntimeError: If Docker Compose setup fails.
        """
        # Clean up any existing containers/networks from previous runs
        self._cleanup()

        try:
            # Build images
            sp.run(
                [
                    "docker",
                    "compose",
                    "-f",
                    str(self._compose_file),
                    "-p",
                    self._project_name,
                    "build",
                    "--pull",
                ]
                + list(self._services),
                capture_output=True,
                check=True,
            )

            # Start containers
            sp.run(
                [
                    "docker",
                    "compose",
                    "-f",
                    str(self._compose_file),
                    "-p",
                    self._project_name,
                    "up",
                    "-d",
                ]
                + list(self._services),
                capture_output=True,
                check=True,
            )

            # Give containers a moment to fully initialize
            time.sleep(0.5)

        except sp.CalledProcessError as e:
            error_msg = e.stderr.decode() if e.stderr else str(e)
            raise RuntimeError(
                f"Failed to setup Docker Compose environment: {error_msg}"
            )

        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """
        Exit the context manager and clean up all resources.

        Stops and removes containers and networks.
        """
        self._cleanup()

    def _cleanup(self) -> None:
        """
        Clean up Docker Compose resources.
        """
        try:
            sp.run(
                [
                    "docker",
                    "compose",
                    "-f",
                    str(self._compose_file),
                    "-p",
                    self._project_name,
                    "down",
                    "--remove-orphans",
                    "--volumes",
                    "--timeout",
                    "5",
                ],
                capture_output=True,
                check=False,
                timeout=10,
            )
        except Exception:
            pass

    def exec(self, service: str, cmd: str) -> ExecResult:
        """
        Execute a command in a service container and wait for completion.

        Args:
            service: Name of the service to execute in.
            cmd: Shell command to execute.

        Returns:
            ExecResult containing exit code, stdout, and stderr.

        Raises:
            ValueError: If the service name is not recognized.
        """
        if service not in self._services:
            raise ValueError(f"Unknown service {service}")

        container_name = self._container_names[service]

        result = sp.run(
            ["docker", "exec", container_name, "bash", "-c", cmd],
            capture_output=True,
            text=True,
        )

        return ExecResult(
            exit_code=result.returncode,
            stdout=result.stdout,
            stderr=result.stderr,
        )

    def popen(self, service: str, cmd: str) -> sp.Popen[str]:
        """
        Start a long-running command in a service container.

        Args:
            service: Name of the service to execute in.
            cmd: Shell command to execute.

        Returns:
            Popen handle for the running process.

        Raises:
            ValueError: If the service name is not recognized.
        """
        if service not in self._services:
            raise ValueError(f"Unknown service {service}")

        container_name = self._container_names[service]

        return sp.Popen(
            ["docker", "exec", container_name, "bash", "-c", cmd],
            stdout=sp.PIPE,
            stderr=sp.PIPE,
            text=True,
        )
