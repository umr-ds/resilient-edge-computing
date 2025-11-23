from __future__ import annotations

import shutil
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from uuid import UUID, uuid4

import psutil
from aiofiles import open
from ormsgpack import packb, unpackb
from tomlkit import dumps, loads

from rec.dtn.eid import EID
from rec.dtn.messages import MSGPACK_MAXINT


@dataclass(frozen=True)
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

    def __post_init__(self) -> None:
        if self.cpu_cores <= 0:
            raise ValueError("Must have a positive number of CPU cores")
        if self.free_cpu_capacity < 0:
            raise ValueError("Must have a non-negative amount of free CPU capacity")
        if self.free_memory < 0:
            raise ValueError("Must have a non-negative amount of free memory")
        if self.free_disk_space < 0:
            raise ValueError("Must have a non-negative amount of free disk space")
        if (
            self.cpu_cores > MSGPACK_MAXINT
            or self.free_cpu_capacity > MSGPACK_MAXINT
            or self.free_memory > MSGPACK_MAXINT
            or self.free_disk_space > MSGPACK_MAXINT
        ):
            raise ValueError(f"Values may not be larger than {MSGPACK_MAXINT}")

    @classmethod
    def from_system(cls) -> Capabilities:
        """
        Create a Capabilities instance from current system resource usage.

        Returns:
            Capabilities: Current system capabilities.
        """
        cpu_count = psutil.cpu_count() or 1
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

    def dictify(self) -> dict:
        return {key: value for key, value in self.__dict__.items() if value}

    @classmethod
    def from_dict(cls, data: dict) -> Capabilities:
        return cls(**data)

    def dumps(self) -> str:
        return dumps(self.dictify())

    async def dump(self, filename: str) -> None:
        async with open(filename, "w") as f:
            await f.write(self.dumps())

    @classmethod
    def loads(cls, data: str) -> Capabilities:
        return cls.from_dict(loads(data).unwrap())

    @classmethod
    async def load(cls, filename: str) -> Capabilities:
        async with open(filename, "r") as f:
            data = await f.read()
            return cls.loads(data)

    def serialize(self) -> bytes:
        return packb(self.dictify())

    @classmethod
    def deserialize(cls, serialized: bytes) -> Capabilities:
        deserialized = unpackb(serialized)
        return cls.from_dict(deserialized)

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


@dataclass(eq=False, frozen=True)
class JobInfo:
    """
    Complete specification for a WebAssembly job execution request.

    Attributes:
        job_id (UUID): Globally unique identifier - generate with uuid.uuid4()
        submitter (EID): EndpointID of the node which submitted this job.
        wasm_module (str): Named reference to the WebAssembly module to execute.
        capabilities (Capabilities): Required system resource capabilities to run this job.
        results_receiver EID: Optional endpoint to send the results zip to.
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
    """

    job_id: UUID
    wasm_module: str
    capabilities: Capabilities
    submitter: EID = EID.none()
    results_receiver: EID = EID.none()
    argv: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    stdin_file: str | None = None
    dirs: list[str] = field(default_factory=list)
    data: dict[str, str] = field(default_factory=dict)
    stdout_file: str | None = None
    stderr_file: str | None = None
    results: list[str] = field(default_factory=list)
    named_results: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.job_id:
            raise ValueError("Job needs an ID")
        if not self.wasm_module:
            raise ValueError("Job needs a wasm module")

    def __str__(self) -> str:
        return self.job_id.hex

    def __eq__(self, other) -> bool:
        if isinstance(other, JobInfo):
            return self.job_id == other.job_id
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.job_id)

    def dictify(self) -> dict:
        data = {key: value for key, value in self.__dict__.items() if value}
        data["job_id"] = str(self.job_id)
        data["capabilities"] = self.capabilities.dictify()
        return data

    @classmethod
    def from_dict(cls, data: dict) -> JobInfo:
        data["job_id"] = UUID(data["job_id"])
        data["capabilities"] = Capabilities.from_dict(data["capabilities"])
        if "submitter" in data and not isinstance(data["submitter"], EID):
            data["submitter"] = EID(data["submitter"])
        if "results_receiver" in data and not isinstance(data["results_receiver"], EID):
            data["results_receiver"] = EID(data["results_receiver"])
        return cls(**data)

    def dumps(self) -> str:
        return dumps(self.dictify())

    async def dump(self, filename: str) -> None:
        async with open(filename, "w") as f:
            await f.write(self.dumps())

    @classmethod
    def loads(cls, data: str) -> JobInfo:
        parsed = loads(data).unwrap()
        return cls.from_dict(parsed)

    @classmethod
    async def load(cls, filename: str) -> JobInfo:
        async with open(filename, "r") as f:
            data = await f.read()
            return cls.loads(data)

    def serialize(self) -> bytes:
        return packb(self.dictify())

    @classmethod
    def deserialize(cls, serialized: bytes) -> JobInfo:
        deserialized = unpackb(serialized)
        return cls.from_dict(deserialized)

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


def dictify_job_infos(jobs: Iterable[JobInfo]) -> list[dict]:
    return [job.dictify() for job in jobs]


def job_infos_from_dicts(job_dicts: Iterable[dict]) -> list[JobInfo]:
    return [JobInfo.from_dict(job_dict) for job_dict in job_dicts]


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

    def dictify(self) -> dict:
        return {
            "metadata": self.metadata.dictify(),
            "data": self.data,
        }

    @classmethod
    def from_dict(cls, data: dict) -> Job:
        data["metadata"] = JobInfo.from_dict(data["metadata"])
        return cls(**data)

    def serialize(self) -> bytes:
        return packb(self.dictify())

    @classmethod
    def deserialize(cls, serialized: bytes) -> Job:
        deserialized = unpackb(serialized)
        return cls.from_dict(deserialized)

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


@dataclass
class JobResult:
    """
    Represents the result of a completed job if that job had a results receiver.

    Attributes:
        metadata (JobInfo): The job's specification.
        results_data (bytes): The binary content of the job's results.
    """

    metadata: JobInfo
    results_data: bytes = b""

    def dictify(self) -> dict:
        return {
            "metadata": self.metadata.dictify(),
            "results_data": self.results_data,
        }

    @classmethod
    def from_dict(cls, data: dict) -> JobResult:
        data["metadata"] = JobInfo.from_dict(data["metadata"])
        return cls(**data)

    def serialize(self) -> bytes:
        return packb(self.dictify())

    @classmethod
    def deserialize(cls, serialized: bytes) -> JobResult:
        deserialized = unpackb(serialized)
        return cls.from_dict(deserialized)


@dataclass
class LazyJob:
    """
    A lazy-loading wrapper for jobs with file-based data.

    This class represents a job whose binary data hasn't been loaded into memory yet.
    Instead of storing the actual binary data,
    it maintains file paths and only loads the data when `as_job()` is called.

    This is used with execution plans where jobs are defined with references to files on disk
    and should only be loaded when ready to submit to the broker.

    Attributes:
        metadata (JobInfo): The job's specification.
        data (dict[str, Path]): Mapping from named data identifiers to their filesystem paths.
            Keys should match the named references in the JobInfo.
    """

    metadata: JobInfo
    data: dict[str, Path]

    def validate_paths(self) -> list[str]:
        """
        Check if all referenced data files exist on disk.

        Returns:
            list[str]: List of missing file paths.
                Empty list if all files exist.
        """
        missing = []
        for path in self.data.values():
            if not path.exists():
                missing.append(str(path))
        return missing

    async def as_job(self) -> Job:
        """
        Load the job's binary data from disk and return a Job instance.

        Returns:
            Job: Job instance with metadata and loaded binary data.

        Raises:
            FileNotFoundError: If any referenced data file does not exist.
            PermissionError: If any data file cannot be read due to permissions.
            OSError: If any other I/O error occurs while reading data files.
        """
        loaded_data = {}
        for name, path in self.data.items():
            async with open(path, "rb") as f:
                loaded_data[name] = await f.read()
        return Job(metadata=self.metadata, data=loaded_data)


@dataclass
class ExecutionPlan:
    """
    Represents a complete execution plan with named data and jobs to execute.

    An execution plan is the structure for defining a batch of jobs.
    It contains:
    - Named data that will be published and can be referenced by multiple jobs (avoiding duplication)
    - A list of jobs to be executed, each with their own metadata and data references.

    Attributes:
        named_data (dict[str, Path]): Shared data files that will be published and can be referenced by multiple jobs.
            Maps named identifiers to filesystem paths.
        jobs (list[LazyJob]): List of jobs to be executed as part of this plan.
    """

    named_data: dict[str, Path]
    jobs: list[LazyJob]

    @classmethod
    async def from_toml(cls, toml_path: Path) -> ExecutionPlan:
        """
        Load an execution plan from a TOML file.

        File paths in the TOML can be relative (resolved relative to the TOML file's directory) or absolute.

        Args:
            toml_path (Path): Path to the TOML file containing the execution plan.

        Returns:
            ExecutionPlan: Parsed execution plan with resolved file paths.

        Raises:
            FileNotFoundError: If the TOML file does not exist.
            TOMLDecodeError: If the TOML file is malformed.
            KeyError: If required fields are missing from the TOML structure.
            ValueError: If the TOML structure is invalid.
        """
        base_dir = toml_path.parent

        async with open(toml_path, "r") as f:
            content = await f.read()
            data = loads(content).unwrap()

        # Parse Named Data
        named_data = {}
        for name, path_str in data.get("named_data", {}).items():
            path = Path(path_str)
            if not path.is_absolute():
                path = base_dir / path
            named_data[name] = path

        # Parse Jobs
        jobs = []
        for job_data in data.get("jobs", []):
            metadata_dict = job_data.get("metadata", {})
            job_data_dict = job_data.get("data", {})

            # Generate new UUID for the job
            metadata_dict["job_id"] = str(uuid4())
            # Submitter will be set by the client upon submission
            metadata_dict["submitter"] = EID.none()

            # Parse metadata
            metadata = JobInfo.from_dict(metadata_dict)

            # Parse job-specific data paths
            job_paths = {}
            for name, path_str in job_data_dict.items():
                path = Path(path_str)
                if not path.is_absolute():
                    path = base_dir / path
                job_paths[name] = path

            jobs.append(LazyJob(metadata=metadata, data=job_paths))

        return cls(named_data=named_data, jobs=jobs)

    def validate_all_paths(self) -> list[str]:
        """
        Validate that all referenced files exist on disk.

        Checks both named_data paths and all job-specific data paths.

        Returns:
            list[str]: List of missing file paths.
                Empty list if all files exist.
        """
        missing: list[str] = []

        # Check named_data
        for path in self.named_data.values():
            if not path.exists():
                missing.append(str(path))

        # Check each job's data
        for job in self.jobs:
            job_missing = job.validate_paths()
            if job_missing:
                missing.extend(job_missing)

        return missing
