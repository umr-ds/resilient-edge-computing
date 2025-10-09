from __future__ import annotations

from dataclasses import replace
from unittest.mock import MagicMock, patch

import pytest

from rec.dtn.eid import EID
from rec.dtn.job import Capabilities, Job, JobInfo


@pytest.fixture
def system_caps() -> Capabilities:
    return Capabilities(
        cpu_cores=4,
        free_cpu_capacity=300,
        free_memory=8 * 1024 * 1024 * 1024,
        free_disk_space=64 * 1024 * 1024 * 1024,
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
def full_job_info() -> JobInfo:
    return JobInfo(
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


class TestCapabilities:
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
        self, system_caps: Capabilities, job_caps: Capabilities
    ):
        assert system_caps.is_capable_of(job_caps)

    def test_is_capable_of_insufficient_cpu_cores(
        self, system_caps: Capabilities, job_caps: Capabilities
    ):
        system_caps.cpu_cores = 2
        job_caps.cpu_cores = 4

        assert not system_caps.is_capable_of(job_caps)

    def test_is_capable_of_insufficient_cpu_capacity(
        self, system_caps: Capabilities, job_caps: Capabilities
    ):
        system_caps.free_cpu_capacity = 100
        job_caps.free_cpu_capacity = 200

        assert not system_caps.is_capable_of(job_caps)

    def test_is_capable_of_insufficient_memory(
        self, system_caps: Capabilities, job_caps: Capabilities
    ):
        system_caps.free_memory = 2 * 1024 * 1024 * 1024
        job_caps.free_memory = 4 * 1024 * 1024 * 1024

        assert not system_caps.is_capable_of(job_caps)

    def test_is_capable_of_insufficient_disk_space(
        self, system_caps: Capabilities, job_caps: Capabilities
    ):
        system_caps.free_disk_space = 8 * 1024 * 1024 * 1024
        job_caps.free_disk_space = 16 * 1024 * 1024 * 1024

        assert not system_caps.is_capable_of(job_caps)

    def test_is_capable_of_exact_match(self, system_caps: Capabilities):
        assert system_caps.is_capable_of(system_caps)


class TestJobInfo:
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
