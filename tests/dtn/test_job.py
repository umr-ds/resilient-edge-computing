from __future__ import annotations

from dataclasses import replace
from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest

from rec.dtn.eid import EID
from rec.dtn.job import Capabilities, Job, JobInfo


@pytest.fixture
def system_caps_sufficient() -> Capabilities:
    return Capabilities(
        cpu_cores=4,
        free_cpu_capacity=300,
        free_memory=8 * 1024 * 1024 * 1024,
        free_disk_space=64 * 1024 * 1024 * 1024,
    )


@pytest.fixture
def system_caps_insufficient_cpu_cores() -> Capabilities:
    return Capabilities(
        cpu_cores=1,
        free_cpu_capacity=300,
        free_memory=8 * 1024 * 1024 * 1024,
        free_disk_space=64 * 1024 * 1024 * 1024,
    )


@pytest.fixture
def system_caps_insufficient_cpu_capacity() -> Capabilities:
    return Capabilities(
        cpu_cores=1,
        free_cpu_capacity=100,
        free_memory=8 * 1024 * 1024 * 1024,
        free_disk_space=64 * 1024 * 1024 * 1024,
    )


@pytest.fixture
def system_caps_insufficient_memory() -> Capabilities:
    return Capabilities(
        cpu_cores=1,
        free_cpu_capacity=100,
        free_memory=2 * 1024 * 1024 * 1024,
        free_disk_space=64 * 1024 * 1024 * 1024,
    )


@pytest.fixture
def system_caps_insufficient_disk_space() -> Capabilities:
    return Capabilities(
        cpu_cores=1,
        free_cpu_capacity=100,
        free_memory=2 * 1024 * 1024 * 1024,
        free_disk_space=16 * 1024 * 1024 * 1024,
    )


@pytest.fixture
def job_caps() -> Capabilities:
    return Capabilities(
        cpu_cores=2,
        free_cpu_capacity=150,
        free_memory=4 * 1024 * 1024 * 1024,
        free_disk_space=32 * 1024 * 1024 * 1024,
    )


@pytest.fixture
def job_caps_toml() -> str:
    return """cpu_cores = 2
free_cpu_capacity = 150
free_memory = 4294967296
free_disk_space = 34359738368
"""


@pytest.fixture
def full_job_info() -> JobInfo:
    return JobInfo(
        job_id=UUID("12345678123456781234567812345678"),
        submitter=EID("dtn:none"),
        wasm_module="wasm-module",
        capabilities=Capabilities(),
        argv=["arg1", "arg2"],
        env={"VAR": "value"},
        stdin_file="stdin",
        dirs=["/tmp", "/output"],
        data={"/input.txt": "input", "/config.txt": "config"},
        stdout_file="/output/stdout.txt",
        stderr_file="/output/stderr.txt",
        results=[],
        named_results={"/output/result.txt": "result"},
        results_receiver=EID.dtn("node"),
    )


@pytest.fixture
def full_job_info_toml() -> str:
    return """job_id = "12345678-1234-5678-1234-567812345678"
wasm_module = "wasm-module"
results_receiver = "dtn://node/"
argv = ["arg1", "arg2"]
stdin_file = "stdin"
dirs = ["/tmp", "/output"]
stdout_file = "/output/stdout.txt"
stderr_file = "/output/stderr.txt"

[capabilities]
cpu_cores = 1

[env]
VAR = "value"

[data]
"/input.txt" = "input"
"/config.txt" = "config"

[named_results]
"/output/result.txt" = "result"
"""


class TestCapabilities:
    def test_dumps(self, job_caps: Capabilities, job_caps_toml: str) -> None:
        dumped = job_caps.dumps()
        assert dumped == job_caps_toml

    def test_loads(self, job_caps: Capabilities, job_caps_toml: str) -> None:
        loaded = Capabilities.loads(job_caps_toml)
        assert job_caps == loaded

    def test_capabilities_defaults(self):
        caps = Capabilities()
        assert caps.cpu_cores == 1
        assert caps.free_cpu_capacity == 0
        assert caps.free_memory == 0
        assert caps.free_disk_space == 0

    def test_capabilities_custom_values(self):
        caps = Capabilities(
            cpu_cores=4,
            free_cpu_capacity=300,
            free_memory=8 * 1024 * 1024 * 1024,
            free_disk_space=64 * 1024 * 1024 * 1024,
        )
        assert caps.cpu_cores == 4
        assert caps.free_cpu_capacity == 300
        assert caps.free_memory == 8 * 1024 * 1024 * 1024
        assert caps.free_disk_space == 64 * 1024 * 1024 * 1024

    @patch("psutil.cpu_count")
    @patch("psutil.cpu_percent")
    @patch("psutil.virtual_memory")
    @patch("shutil.disk_usage")
    def test_from_system_basic(
        self, mock_disk_usage, mock_virtual_memory, mock_cpu_percent, mock_cpu_count
    ):
        mock_cpu_count.return_value = 4
        mock_cpu_percent.return_value = 25.0
        mock_virtual_memory.return_value = MagicMock(available=8 * 1024 * 1024 * 1024)
        mock_disk_usage.return_value = MagicMock(free=64 * 1024 * 1024 * 1024)

        caps = Capabilities.from_system()

        assert caps.cpu_cores == 4
        assert caps.free_cpu_capacity == 300
        assert caps.free_memory == 8 * 1024 * 1024 * 1024
        assert caps.free_disk_space == 64 * 1024 * 1024 * 1024

    def test_is_capable_of_sufficient_resources(
        self, system_caps_sufficient: Capabilities, job_caps: Capabilities
    ):
        assert system_caps_sufficient.is_capable_of(job_caps)

    def test_is_capable_of_insufficient_cpu_cores(
        self, system_caps_insufficient_cpu_cores: Capabilities, job_caps: Capabilities
    ):
        assert not system_caps_insufficient_cpu_cores.is_capable_of(job_caps)

    def test_is_capable_of_insufficient_cpu_capacity(
        self,
        system_caps_insufficient_cpu_capacity: Capabilities,
        job_caps: Capabilities,
    ):
        assert not system_caps_insufficient_cpu_capacity.is_capable_of(job_caps)

    def test_is_capable_of_insufficient_memory(
        self, system_caps_insufficient_memory: Capabilities, job_caps: Capabilities
    ):
        assert not system_caps_insufficient_memory.is_capable_of(job_caps)

    def test_is_capable_of_insufficient_disk_space(
        self, system_caps_insufficient_disk_space: Capabilities, job_caps: Capabilities
    ):
        assert not system_caps_insufficient_disk_space.is_capable_of(job_caps)

    def test_is_capable_of_exact_match(self, system_caps_sufficient: Capabilities):
        assert system_caps_sufficient.is_capable_of(system_caps_sufficient)


class TestJobInfo:
    def test_dumps(self, full_job_info: JobInfo, full_job_info_toml: str) -> None:
        dumped = full_job_info.dumps()
        assert dumped == full_job_info_toml

    def test_loads(self, full_job_info: JobInfo, full_job_info_toml: str) -> None:
        loaded = JobInfo.loads(full_job_info_toml)
        assert full_job_info == loaded

    def test_required_named_data_basic(self, full_job_info: JobInfo):
        required = full_job_info.required_named_data()
        expected = {"wasm-module", "stdin", "input", "config"}
        assert required == expected

    def test_required_named_data_no_stdin(self, full_job_info: JobInfo):
        job = replace(full_job_info, stdin_file=None)

        required = job.required_named_data()
        expected = {"wasm-module", "input", "config"}
        assert required == expected

    def test_required_named_data_no_data_files(self, full_job_info: JobInfo):
        job = replace(full_job_info, data={})

        required = job.required_named_data()
        expected = {"wasm-module", "stdin"}
        assert required == expected

    def test_required_named_data_minimal(self, full_job_info: JobInfo):
        job = replace(
            full_job_info,
            wasm_module="wasm-module",
            stdin_file=None,
            data={},
        )

        required = job.required_named_data()
        expected = {"wasm-module"}
        assert required == expected

    def test_required_named_data_duplicates(self, full_job_info: JobInfo):
        job = replace(
            full_job_info,
            wasm_module="shared",
            stdin_file="shared",
            data={
                "/file1.txt": "shared",
                "/file2.txt": "unique",
                "/file3.txt": "shared",
            },
        )

        required = job.required_named_data()
        expected = {"shared", "unique"}
        assert required == expected


class TestJob:
    def test_has_all_data(self, full_job_info: JobInfo):
        job = Job(
            metadata=full_job_info,
            data={
                "wasm-module": b"wasm-content",
                "stdin": b"stdin-content",
                "input": b"input-content",
                "config": b"config-content",
            },
        )

        assert job.has_all_data() is True
        assert job.missing_data() == set()

    def test_has_all_data_missing(self, full_job_info: JobInfo):
        job = Job(
            metadata=full_job_info,
            data={},
        )

        assert job.has_all_data() is False
        assert job.missing_data() == {"wasm-module", "stdin", "input", "config"}

    def test_has_extra_data(self, full_job_info: JobInfo):
        job = Job(
            metadata=full_job_info,
            data={
                "wasm-module": b"wasm-content",
                "stdin": b"stdin-content",
                "input": b"input-content",
                "config": b"config-content",
                "extra": b"not-required",
            },
        )

        assert job.has_all_data() is True
        assert job.missing_data() == set()
