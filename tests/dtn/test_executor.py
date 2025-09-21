from __future__ import annotations

from pathlib import Path

import pytest

from rec.dtn.executor import WasmTrapError, run_wasi_module

HERE = Path(__file__).resolve().parent
WASM = HERE / "artifacts" / "wasi-smoke.wasm"


@pytest.fixture(scope="session")
def wasm_path() -> Path:
    assert WASM.is_file(), f"Missing test artifact: {WASM}"
    return WASM


@pytest.mark.asyncio
async def test_full_io_and_exit(wasm_path: Path, tmp_path: Path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "infile.txt").write_text("hello-from-host")

    stdin_file = tmp_path / "stdin.txt"
    stdin_file.write_text("line1\nline2")

    out_dir = tmp_path / "out"
    stdout_file = out_dir / "stdout.txt"
    stderr_file = out_dir / "stderr.txt"

    exit_code = await run_wasi_module(
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
async def test_minimal_execution_no_io_files(wasm_path: Path, tmp_path: Path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    exit_code = await run_wasi_module(
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
async def test_no_data_directory_raises_wasm_trap_error(wasm_path: Path):
    with pytest.raises(WasmTrapError):
        await run_wasi_module(
            exec_file=wasm_path,
            argv=[],
            env={},
            stdin_file=None,
            data_dir=None,
            stdout_file=None,
            stderr_file=None,
        )
