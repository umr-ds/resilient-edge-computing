from __future__ import annotations

import shutil
from dataclasses import dataclass
from enum import IntEnum
from typing import Dict, List, Optional, Set, Union

import psutil

from rec.dtn.eid import EID


class DataType(IntEnum):
    """
    Enumeration of supported data types for job inputs.
    """

    NAMED = 1
    STRING = 2
    BINARY = 3


@dataclass
class Data:
    """
    Container for typed data.

    Attributes:
        type (DataType): The type of data.
        value (Union[str, bytes]): The actual data payload.
    """

    type: DataType
    value: Union[str, bytes]


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
        wasm_module (Data): The WebAssembly module to execute.
        argv (List[str]): Program arguments.
        env (Dict[str, str]): Environment variables for the execution environment.
        stdin_file (Optional[Data]): Optional stdin data for the module.
        dirs (List[str]): Directory paths that will be created in the execution environment before the WASM module is executed.
        data (Dict[str, Data]): Mapping of execution environment paths to data that should be placed at those locations before execution.
        stdout_file (Optional[str]): Path in the execution environment where stdout should be written.
        stderr_file (Optional[str]): Path in the execution environment where stderr should be written.
        named_results (Dict[str, str]): Mapping of execution environment paths to result names.
            Files at these paths will be collected after execution.
            If a path is a directory, it will be zipped before storage.
        capabilities (Capabilities): Required system resource capabilities to run this job.
        result_receiver (Optional[EID]): Optional endpoint to send results to.
    """

    wasm_module: Data
    argv: List[str]
    env: Dict[str, str]
    stdin_file: Optional[Data]
    dirs: List[str]
    data: Dict[str, Data]
    stdout_file: Optional[str]
    stderr_file: Optional[str]
    named_results: Dict[str, str]
    capabilities: Capabilities
    result_receiver: Optional[EID]

    def required_named_data(self) -> Set[str]:
        """
        Get a set of all named data inputs required by this job.

        Returns:
            Set[str]: Set of required named data identifiers.
        """
        required_names = []
        if self.wasm_module.type == DataType.NAMED:
            required_names.append(self.wasm_module.value)
        if self.stdin_file and self.stdin_file.type == DataType.NAMED:
            required_names.append(self.stdin_file.value)
        for _, data in self.data.items():
            if data.type == DataType.NAMED:
                required_names.append(data.value)
        return set(required_names)
