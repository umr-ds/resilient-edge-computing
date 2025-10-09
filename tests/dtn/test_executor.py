from __future__ import annotations

import io
import zipfile
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from rec.dtn.eid import EID
from rec.dtn.executor import Executor, WasmTrapError, _run_wasi_module
from rec.dtn.job import Capabilities, Job, JobInfo
from rec.dtn.storage import NoSuchNameError

HERE = Path(__file__).resolve().parent
WASM = HERE / "artifacts" / "wasi-smoke.wasm"


@pytest.fixture(scope="session")
def wasm_path() -> Path:
    assert WASM.is_file(), f"Missing test artifact: {WASM}"
    return WASM


@pytest.fixture
def test_eid() -> EID:
    return EID.dtn("executor")


@pytest.fixture
def executor(test_eid: EID, tmp_path: Path) -> Executor:
    with patch.object(Executor, "_register", new_callable=AsyncMock):
        executor = Executor(
            node_id=test_eid,
            dtn_agent_socket="/tmp/executor_test.sock",
            root_dir=tmp_path / "executor_root",
        )
        return executor


async def populate_cache(executor: Executor, data: dict[str, bytes]) -> None:
    for name, content in data.items():
        await executor._storage.store_data(name=name, data=content)


@pytest.fixture
def job_dirs(tmp_path: Path) -> tuple[Path, Path]:
    job_dir = tmp_path / "job"
    data_dir = job_dir / "data"
    data_dir.mkdir(parents=True)
    return job_dir, data_dir


@pytest.fixture
def minimal_job_info() -> JobInfo:
    return JobInfo(
        wasm_module="wasm-module",
        capabilities=Capabilities(),
    )


@pytest.fixture
def sample_job(wasm_path: Path) -> Job:
    with open(wasm_path, "rb") as f:
        wasm_data = f.read()

    job_info = JobInfo(
        wasm_module="wasm-module",
        capabilities=Capabilities(),
        argv=["a", "b", "c"],
        env={"FOO": "bar"},
        stdin_file="stdin",
        dirs=["/output", "/temp"],
        data={
            "/infile.txt": "infile",
            "/data.bin": "databin",
        },
        stdout_file="/output/stdout.log",
        stderr_file="/output/stderr.log",
        results=["/out.txt", "/output"],
        named_results={
            "/out.txt": "wasm_output_file",
            "/output": "output_archive",
        },
        results_receiver=EID.dtn("client", "results"),
    )

    return Job(
        metadata=job_info,
        data={
            "wasm-module": wasm_data,
            "stdin": b"line1\nline2",
            "infile": b"hello-from-host",
            "databin": b"\x00\x01\x02\x03",
        },
    )


class TestExecutorWasmModule:
    @pytest.mark.asyncio
    async def test_full_io_and_exit(self, wasm_path: Path, tmp_path: Path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "infile.txt").write_text("hello-from-host")

        stdin_file = tmp_path / "stdin.txt"
        stdin_file.write_text("line1\nline2")

        out_dir = tmp_path / "out"
        stdout_file = out_dir / "stdout.txt"
        stderr_file = out_dir / "stderr.txt"

        exit_code = await _run_wasi_module(
            exec_file=wasm_path,
            argv=["a", "b", "c"],
            env={"FOO": "bar", "EXIT": "7"},
            stdin_file=stdin_file,
            data_dir=data_dir,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
        )

        assert exit_code == 7

        # stdout assertions
        stdout = stdout_file.read_text()
        assert "ARGS=a,b,c" in stdout
        assert "ENV_FOO=bar" in stdout
        assert "STDIN=line1\\nline2" in stdout
        assert "DATA_READ=hello-from-host" in stdout

        # fs write
        assert (data_dir / "out.txt").read_text() == "hello-from-wasi"

        # stderr assertion
        assert "TO_STDERR" in stderr_file.read_text()

    @pytest.mark.asyncio
    async def test_minimal_execution_no_io_files(self, wasm_path: Path, tmp_path: Path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        exit_code = await _run_wasi_module(
            exec_file=wasm_path,
            argv=[],
            env={},
            stdin_file=None,
            data_dir=data_dir,
            stdout_file=None,
            stderr_file=None,
        )

        assert exit_code == 0

        # fs write
        assert (data_dir / "out.txt").read_text() == "hello-from-wasi"

    @pytest.mark.asyncio
    async def test_no_data_directory_raises_wasm_trap_error(self, wasm_path: Path):
        with pytest.raises(WasmTrapError):
            await _run_wasi_module(
                exec_file=wasm_path,
                argv=[],
                env={},
                stdin_file=None,
                data_dir=None,
                stdout_file=None,
                stderr_file=None,
            )


class TestExecutorPrepareWasiEnvironment:
    @pytest.mark.asyncio
    async def test_prepare_wasm_environment(
        self, executor: Executor, job_dirs: tuple[Path, Path], sample_job: Job
    ):
        job_dir, _data_dir = job_dirs

        await populate_cache(executor, sample_job.data)

        wasm_path, stdin_path, data_dir = await executor._prepare_wasi_environment(
            sample_job.metadata, job_dir
        )

        # Check WASM module was written
        assert wasm_path.exists()
        assert wasm_path.name == "module.wasm"

        # Check stdin file was created
        assert stdin_path is not None
        assert stdin_path.exists()
        assert stdin_path.read_text() == "line1\nline2"

        # Check data directory structure
        assert data_dir.exists()
        assert (data_dir / "infile.txt").read_text() == "hello-from-host"
        assert (data_dir / "data.bin").read_bytes() == b"\x00\x01\x02\x03"

        # Check directories were created
        assert (data_dir / "output").is_dir()
        assert (data_dir / "temp").is_dir()

    @pytest.mark.asyncio
    async def test_prepare_missing_named_data_raises_error(
        self, executor: Executor, job_dirs: tuple[Path, Path], sample_job: Job
    ):
        job_dir, _data_dir = job_dirs

        # Don't populate the cache
        with pytest.raises(NoSuchNameError):
            await executor._prepare_wasi_environment(sample_job.metadata, job_dir)

    @pytest.mark.asyncio
    async def test_prepare_path_escape_security(
        self, executor: Executor, job_dirs: tuple[Path, Path], sample_job: Job
    ):
        job_dir, _data_dir = job_dirs

        malicious_job = replace(
            sample_job.metadata,
            dirs=["../../../etc"],
        )

        await populate_cache(executor, sample_job.data)

        with pytest.raises(ValueError):
            await executor._prepare_wasi_environment(malicious_job, job_dir)

    @pytest.mark.asyncio
    async def test_prepare_stdout_stderr_directories(
        self, executor: Executor, job_dirs: tuple[Path, Path], sample_job: Job
    ):
        job_dir, _data_dir = job_dirs

        job = replace(
            sample_job.metadata,
            stdout_file="/logs/output.log",
            stderr_file="/logs/error.log",
        )

        await populate_cache(executor, sample_job.data)

        _, _, data_dir = await executor._prepare_wasi_environment(job, job_dir)

        # Check that parent directories were created
        assert (data_dir / "logs").is_dir()


class TestExecutorCollectResults:
    @pytest.mark.asyncio
    async def test_collect_empty(
        self, executor: Executor, job_dirs: tuple[Path, Path], minimal_job_info: JobInfo
    ):
        job_dir, _data_dir = job_dirs

        job = replace(
            minimal_job_info,
            results=[],
            results_receiver=EID.dtn("client", "results"),
        )

        results = await executor._collect_results(job, job_dir)

        # Should return empty zip when results=[] but receiver exists
        assert results is not None
        with zipfile.ZipFile(io.BytesIO(results), "r") as zf:
            assert len(zf.namelist()) == 0

    @pytest.mark.asyncio
    async def test_collect_no_receiver(
        self, executor: Executor, job_dirs: tuple[Path, Path], minimal_job_info: JobInfo
    ):
        job_dir, _data_dir = job_dirs

        job = replace(
            minimal_job_info,
            results_receiver=None,
        )

        results = await executor._collect_results(job, job_dir)

        assert results is None

    @pytest.mark.asyncio
    async def test_collect_missing_results_skipped(
        self, executor: Executor, job_dirs: tuple[Path, Path], minimal_job_info: JobInfo
    ):
        job_dir, _data_dir = job_dirs

        job = replace(
            minimal_job_info,
            results=["/nonexistent.txt"],
            results_receiver=EID.dtn("client", "results"),
        )

        results = await executor._collect_results(job, job_dir)

        # Should return empty zip when files are missing but receiver exists
        assert results is not None
        with zipfile.ZipFile(io.BytesIO(results), "r") as zf:
            assert len(zf.namelist()) == 0

    @pytest.mark.asyncio
    async def test_collect_path_escape_security(
        self, executor: Executor, job_dirs: tuple[Path, Path], minimal_job_info: JobInfo
    ):
        job_dir, data_dir = job_dirs

        # Create a file outside data directory
        (data_dir.parent / "secret.txt").write_text("hello-from-host")

        job = replace(
            minimal_job_info,
            results=["../secret.txt"],
            results_receiver=EID.dtn("client", "results"),
        )

        results = await executor._collect_results(job, job_dir)

        # Should return empty zip when files are skipped for security but receiver exists
        assert results is not None
        with zipfile.ZipFile(io.BytesIO(results), "r") as zf:
            assert len(zf.namelist()) == 0

    @pytest.mark.asyncio
    async def test_collect(
        self, executor: Executor, job_dirs: tuple[Path, Path], minimal_job_info: JobInfo
    ):
        job_dir, data_dir = job_dirs

        # Create test output files and directories
        (data_dir / "result.txt").write_text("hello-from-wasi")
        (data_dir / "output.bin").write_bytes(b"\x01\x02\x03")
        (data_dir / "uninteresting.txt").write_text("ignore me")
        (data_dir / "subdir").mkdir()
        (data_dir / "subdir" / "file1.txt").write_text("line1\nline2")
        (data_dir / "subdir" / "file2.txt").write_bytes(b"\x03\x04\x05")
        (data_dir / "unrelated").mkdir()
        (data_dir / "unrelated" / "data.txt").write_text("not included")

        job = replace(
            minimal_job_info,
            results=["/result.txt", "/output.bin", "/subdir"],
            results_receiver=EID.dtn("client", "results"),
        )

        results = await executor._collect_results(job, job_dir)

        with zipfile.ZipFile(io.BytesIO(results), "r") as zf:
            files = zf.namelist()
            assert len(files) == 4
            assert "result.txt" in files
            assert "output.bin" in files
            assert "subdir/file1.txt" in files
            assert "subdir/file2.txt" in files
            assert zf.read("result.txt") == b"hello-from-wasi"
            assert zf.read("output.bin") == b"\x01\x02\x03"
            assert zf.read("subdir/file1.txt") == b"line1\nline2"
            assert zf.read("subdir/file2.txt") == b"\x03\x04\x05"


class TestExecutorCollectNamedResults:
    @pytest.mark.asyncio
    async def test_collect_empty(
        self, executor: Executor, job_dirs: tuple[Path, Path], minimal_job_info: JobInfo
    ):
        job_dir, _data_dir = job_dirs

        job = replace(
            minimal_job_info,
            named_results={},
        )

        results = await executor._collect_named_results(job, job_dir)

        assert results == {}

    @pytest.mark.asyncio
    async def test_collect_missing_results_skipped(
        self, executor: Executor, job_dirs: tuple[Path, Path], minimal_job_info: JobInfo
    ):
        job_dir, _data_dir = job_dirs

        job = replace(
            minimal_job_info,
            named_results={"/nonexistent.txt": "missing_result"},
        )

        results = await executor._collect_named_results(job, job_dir)

        assert results == {}

    @pytest.mark.asyncio
    async def test_collect_path_escape_security(
        self, executor: Executor, job_dirs: tuple[Path, Path], minimal_job_info: JobInfo
    ):
        job_dir, data_dir = job_dirs

        # Create a file outside data directory
        (data_dir.parent / "secret.txt").write_text("hello-from-host")

        job = replace(
            minimal_job_info,
            named_results={"../secret.txt": "escaped_file"},
        )

        results = await executor._collect_named_results(job, job_dir)

        assert results == {}

    @pytest.mark.asyncio
    async def test_collect(
        self, executor: Executor, job_dirs: tuple[Path, Path], minimal_job_info: JobInfo
    ):
        job_dir, data_dir = job_dirs

        # Create test output files and directories
        (data_dir / "result.txt").write_text("hello-from-wasi")
        (data_dir / "output.bin").write_bytes(b"\x01\x02\x03")
        (data_dir / "uninteresting.txt").write_text("ignore me")
        (data_dir / "subdir").mkdir()
        (data_dir / "subdir" / "file1.txt").write_text("line1\nline2")
        (data_dir / "subdir" / "file2.txt").write_bytes(b"\x03\x04\x05")
        (data_dir / "unrelated").mkdir()
        (data_dir / "unrelated" / "data.txt").write_text("not included")

        job = replace(
            minimal_job_info,
            named_results={
                "/result.txt": "text_result",
                "/output.bin": "binary_result",
                "/subdir": "archive",
            },
        )

        results = await executor._collect_named_results(job, job_dir)

        assert len(results) == 3
        assert results["text_result"] == b"hello-from-wasi"
        assert results["binary_result"] == b"\x01\x02\x03"

        zip_data = results["archive"]
        with zipfile.ZipFile(io.BytesIO(zip_data), "r") as zf:
            files = zf.namelist()
            assert len(files) == 2
            assert "subdir/file1.txt" in files
            assert "subdir/file2.txt" in files
            assert zf.read("subdir/file1.txt") == b"line1\nline2"
            assert zf.read("subdir/file2.txt") == b"\x03\x04\x05"


class TestExecutorRunJob:
    @pytest.mark.asyncio
    async def test_run_job(self, executor: Executor, sample_job: Job):
        await populate_cache(executor, sample_job.data)

        results_zip, named_results = await executor._run_job(sample_job.metadata)

        # Verify results were collected
        assert results_zip is not None
        with zipfile.ZipFile(io.BytesIO(results_zip), "r") as zf:
            files = zf.namelist()
            assert len(files) == 3

            # Check the WASM output file
            assert "out.txt" in files
            assert zf.read("out.txt") == b"hello-from-wasi"

            # Check output directory contents
            assert "output/stdout.log" in files
            stdout_content = zf.read("output/stdout.log").decode()
            assert "ARGS=a,b,c" in stdout_content
            assert "ENV_FOO=bar" in stdout_content
            assert "STDIN=line1\\nline2" in stdout_content
            assert "DATA_READ=hello-from-host" in stdout_content

            assert "output/stderr.log" in files
            stderr_content = zf.read("output/stderr.log").decode()
            assert "TO_STDERR" in stderr_content

        # Verify named results were collected
        assert len(named_results) == 2
        assert "wasm_output_file" in named_results
        assert "output_archive" in named_results

        # Check the WASM output file
        assert named_results["wasm_output_file"].decode() == "hello-from-wasi"

        # Check output directory contents
        output_zip_data = named_results["output_archive"]
        with zipfile.ZipFile(io.BytesIO(output_zip_data), "r") as zf:
            files = zf.namelist()
            assert len(files) == 2

            assert "output/stdout.log" in files
            stdout_content = zf.read("output/stdout.log").decode()
            assert "ARGS=a,b,c" in stdout_content
            assert "ENV_FOO=bar" in stdout_content
            assert "STDIN=line1\\nline2" in stdout_content
            assert "DATA_READ=hello-from-host" in stdout_content

            assert "output/stderr.log" in files
            stderr_content = zf.read("output/stderr.log").decode()
            assert "TO_STDERR" in stderr_content

        # Check that the job directory was cleaned up
        job_dirs = list(executor._root_dir.glob("job-*"))
        assert len(job_dirs) == 0
