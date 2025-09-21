from __future__ import annotations

from unittest.mock import MagicMock, patch

from rec.dtn.eid import EID
from rec.dtn.job import Capabilities, Data, DataType, JobInfo


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

    def test_is_capable_of_sufficient_resources(self):
        system_caps = Capabilities(
            cpu_cores=4,
            free_cpu_capacity=300,
            free_memory=8 * 1024 * 1024 * 1024,
            free_disk_space=64 * 1024 * 1024 * 1024,
        )

        job_requirements = Capabilities(
            cpu_cores=2,
            free_cpu_capacity=150,
            free_memory=4 * 1024 * 1024 * 1024,
            free_disk_space=32 * 1024 * 1024 * 1024,
        )

        assert system_caps.is_capable_of(job_requirements)

    def test_is_capable_of_insufficient_cpu_cores(self):
        system_caps = Capabilities(
            cpu_cores=2,
            free_cpu_capacity=200,
            free_memory=8 * 1024 * 1024 * 1024,
            free_disk_space=64 * 1024 * 1024 * 1024,
        )
        job_requirements = Capabilities(
            cpu_cores=4,
            free_cpu_capacity=100,
            free_memory=4 * 1024 * 1024 * 1024,
            free_disk_space=32 * 1024 * 1024 * 1024,
        )

        assert not system_caps.is_capable_of(job_requirements)

    def test_is_capable_of_insufficient_cpu_capacity(self):
        """Test is_capable_of() when system lacks CPU capacity."""
        system_caps = Capabilities(
            cpu_cores=4,
            free_cpu_capacity=100,
            free_memory=8 * 1024 * 1024 * 1024,
            free_disk_space=64 * 1024 * 1024 * 1024,
        )
        job_requirements = Capabilities(
            cpu_cores=4,
            free_cpu_capacity=200,
            free_memory=4 * 1024 * 1024 * 1024,
            free_disk_space=32 * 1024 * 1024 * 1024,
        )

        assert not system_caps.is_capable_of(job_requirements)

    def test_is_capable_of_insufficient_memory(self):
        system_caps = Capabilities(
            cpu_cores=4,
            free_cpu_capacity=300,
            free_memory=2 * 1024 * 1024 * 1024,
            free_disk_space=64 * 1024 * 1024 * 1024,
        )
        job_requirements = Capabilities(
            cpu_cores=4,
            free_cpu_capacity=200,
            free_memory=4 * 1024 * 1024 * 1024,
            free_disk_space=32 * 1024 * 1024 * 1024,
        )

        assert not system_caps.is_capable_of(job_requirements)

    def test_is_capable_of_insufficient_disk_space(self):
        """Test is_capable_of() when system lacks disk space."""
        system_caps = Capabilities(
            cpu_cores=4,
            free_cpu_capacity=300,
            free_memory=8 * 1024 * 1024 * 1024,
            free_disk_space=16 * 1024 * 1024 * 1024,
        )
        job_requirements = Capabilities(
            cpu_cores=4,
            free_cpu_capacity=200,
            free_memory=4 * 1024 * 1024 * 1024,
            free_disk_space=32 * 1024 * 1024 * 1024,
        )

        assert not system_caps.is_capable_of(job_requirements)

    def test_is_capable_of_exact_match(self):
        """Test is_capable_of() when resources exactly match."""
        caps = Capabilities(
            cpu_cores=2,
            free_cpu_capacity=150,
            free_memory=4 * 1024 * 1024 * 1024,
            free_disk_space=32 * 1024 * 1024 * 1024,
        )

        assert caps.is_capable_of(caps)


class TestJobInfo:
    def test_required_named_data_empty(self):
        job = JobInfo(
            wasm_module=Data(type=DataType.BINARY, value=b"wasm-binary"),
            argv=["arg1", "arg2"],
            env={"VAR": "value"},
            stdin_file=Data(type=DataType.STRING, value="input"),
            dirs=["/tmp", "/output"],
            data={"/input.txt": Data(type=DataType.STRING, value="file content")},
            stdout_file="/output/stdout.txt",
            stderr_file="/output/stderr.txt",
            named_results={"/output/result.txt": "my-result"},
            capabilities=Capabilities(),
            result_receiver=EID.dtn("node"),
        )

        required = job.required_named_data()
        assert required == set()

    def test_required_named_data_wasm_module_only(self):
        job = JobInfo(
            wasm_module=Data(type=DataType.NAMED, value="wasm-module"),
            argv=[],
            env={},
            stdin_file=None,
            dirs=[],
            data={},
            stdout_file=None,
            stderr_file=None,
            named_results={},
            capabilities=Capabilities(),
            result_receiver=None,
        )

        required = job.required_named_data()
        assert required == {"wasm-module"}

    def test_required_named_data_stdin_only(self):
        job = JobInfo(
            wasm_module=Data(type=DataType.BINARY, value=b"wasm"),
            argv=[],
            env={},
            stdin_file=Data(type=DataType.NAMED, value="input-data"),
            dirs=[],
            data={},
            stdout_file=None,
            stderr_file=None,
            named_results={},
            capabilities=Capabilities(),
            result_receiver=None,
        )

        required = job.required_named_data()
        assert required == {"input-data"}

    def test_required_named_data_data_files_only(self):
        job = JobInfo(
            wasm_module=Data(type=DataType.BINARY, value=b"wasm"),
            argv=[],
            env={},
            stdin_file=None,
            dirs=[],
            data={
                "/file1.txt": Data(type=DataType.NAMED, value="named-file-1"),
                "/file2.txt": Data(type=DataType.STRING, value="string content"),
                "/file3.txt": Data(type=DataType.NAMED, value="named-file-2"),
            },
            stdout_file=None,
            stderr_file=None,
            named_results={},
            capabilities=Capabilities(),
            result_receiver=None,
        )

        required = job.required_named_data()
        assert set(required) == {"named-file-1", "named-file-2"}

    def test_required_named_data_all_sources(self):
        job = JobInfo(
            wasm_module=Data(type=DataType.NAMED, value="wasm-module"),
            argv=[],
            env={},
            stdin_file=Data(type=DataType.NAMED, value="stdin-data"),
            dirs=[],
            data={
                "/input1.txt": Data(type=DataType.NAMED, value="input-file-1"),
                "/input2.txt": Data(type=DataType.BINARY, value=b"binary"),
                "/input3.txt": Data(type=DataType.NAMED, value="input-file-2"),
            },
            stdout_file=None,
            stderr_file=None,
            named_results={},
            capabilities=Capabilities(),
            result_receiver=None,
        )

        required = job.required_named_data()
        assert set(required) == {
            "wasm-module",
            "stdin-data",
            "input-file-1",
            "input-file-2",
        }

    def test_required_named_data_duplicates(self):
        job = JobInfo(
            wasm_module=Data(type=DataType.NAMED, value="shared-data"),
            argv=[],
            env={},
            stdin_file=Data(type=DataType.NAMED, value="shared-data"),
            dirs=[],
            data={
                "/file1.txt": Data(type=DataType.NAMED, value="shared-data"),
                "/file2.txt": Data(type=DataType.NAMED, value="unique-data"),
            },
            stdout_file=None,
            stderr_file=None,
            named_results={},
            capabilities=Capabilities(),
            result_receiver=None,
        )

        required = job.required_named_data()

        # Should contain each unique name only once
        assert set(required) == {"shared-data", "unique-data"}
        assert len(required) == len(set(required))
