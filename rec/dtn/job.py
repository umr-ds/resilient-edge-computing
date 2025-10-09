from __future__ import annotations

import shutil
from dataclasses import dataclass, field

import psutil

from rec.dtn.eid import EID


@dataclass
class Capabilities:
    """
    Represents required or available system resources for job scheduling.

    Attributes:
        cpu_cores (int): Number of CPU cores available on the system.
        free_cpu_capacity (int): Available CPU capacity from 0 to cpu_cores*100.
            Each core contributes 100 units, so a 4-core system has max capacity of 400.
        free_memory (int): Available memory in bytes.
        free_disk_space (int): Available disk space in bytes.
    """

    cpu_cores: int = 1
    free_cpu_capacity: int = 0
    free_memory: int = 0
    free_disk_space: int = 0

    @classmethod
    def from_system(cls) -> Capabilities:
        """
        Create a Capabilities instance from current system resource usage.

        Returns:
            Capabilities: Current system capabilities.
        """
        cpu_count = psutil.cpu_count()
        cpu_usage_percent = psutil.cpu_percent(interval=0.1)
        total_cpu_capacity = cpu_count * 100
        used_cpu_capacity = cpu_usage_percent * cpu_count
        free_cpu_capacity = min(
            max(0, total_cpu_capacity - used_cpu_capacity), total_cpu_capacity
        )

        free_memory = psutil.virtual_memory().available
        free_disk_space = shutil.disk_usage("/").free

        return cls(
            cpu_cores=cpu_count,
            free_cpu_capacity=int(free_cpu_capacity),
            free_memory=free_memory,
            free_disk_space=free_disk_space,
        )

    def is_capable_of(self, caps: Capabilities) -> bool:
        """
        Check if this system can satisfy the resource requirements of another capability set.

        Args:
            caps (Capabilities): Required resource capabilities to check against.

        Returns:
            bool: True if this system can satisfy all resource requirements, False otherwise.
        """
        return (
            self.cpu_cores >= caps.cpu_cores
            and self.free_cpu_capacity >= caps.free_cpu_capacity
            and self.free_memory >= caps.free_memory
            and self.free_disk_space >= caps.free_disk_space
        )


@dataclass
class JobInfo:
    """
    Complete specification for a WebAssembly job execution request.

    Attributes:
        wasm_module (str): Named reference to the WebAssembly module to execute.
        capabilities (Capabilities): Required system resource capabilities to run this job.
        argv (list[str]): Program arguments.
        env (dict[str, str]): Environment variables for the execution environment.
        stdin_file (str | None): Optional named reference to stdin data for the module.
        dirs (list[str]): Directory paths that will be created in the execution environment before the WASM module is executed.
        data (dict[str, str]): Mapping of execution environment paths to named data references that should be placed at those locations before execution.
        stdout_file (str | None): Path in the execution environment where stdout should be written.
        stderr_file (str | None): Path in the execution environment where stderr should be written.
        results (list[str]): Execution environment paths to collect and send directly to the `results_receiver`.
            Files and directories at these paths will be packaged into a single zip file.
        named_results (dict[str, str]): Maps paths to named data identifiers.
            Files at these paths will be collected after execution and stored in Datastores.
            Directories are automatically zipped.
            This is for persistent results that can be referenced by other jobs or retrieved later.
        results_receiver (EID | None): Optional endpoint to send the results zip to.
    """

    wasm_module: str
    capabilities: Capabilities
    argv: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    stdin_file: str | None = None
    dirs: list[str] = field(default_factory=list)
    data: dict[str, str] = field(default_factory=dict)
    stdout_file: str | None = None
    stderr_file: str | None = None
    results: list[str] = field(default_factory=list)
    named_results: dict[str, str] = field(default_factory=dict)
    results_receiver: EID | None = None

    def required_named_data(self) -> set[str]:
        """
        Get a set of all named data inputs required by this job.

        Returns:
            set[str]: Set of required named data identifiers.
        """
        required_names = [self.wasm_module]
        if self.stdin_file:
            required_names.append(self.stdin_file)
        required_names.extend(self.data.values())
        return set(required_names)


@dataclass
class Job:
    """
    Container for a job's metadata and associated binary data.

    The Job class combines job specification with optional binary data.
    When submitted to an Executor, the provided data will be stored in the Executor's cache,
    the JobInfo will be queued for execution, and any missing data will be requested from datastores.

    Attributes:
        metadata (JobInfo): Complete job specification with named data references.
        data (dict[str, bytes]): Mapping of named data identifiers to their binary content.
            Keys should match the named references in JobInfo fields.
            Any data provided here will be cached by the Executor and won't need to be fetched from datastores.
    """

    metadata: JobInfo
    data: dict[str, bytes]

    def has_all_data(self) -> bool:
        """
        Check if all required data has been provided with the job.

        Returns:
            bool: True if all named references in JobInfo have corresponding binary data.
        """
        required = self.metadata.required_named_data()
        return required.issubset(self.data.keys())

    def missing_data(self) -> set[str]:
        """
        Get the set of named data references that are not included with this job.

        Returns:
            set[str]: Named data identifiers that the Executor will need to fetch from datastores.
        """
        required = self.metadata.required_named_data()
        return required - self.data.keys()
