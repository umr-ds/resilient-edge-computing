import asyncio
import os
from pathlib import Path
from typing import Dict, List, Optional

from wasmtime import ExitTrap, Linker, Module, Store, Trap, WasiConfig, WasmtimeError

from rec.util.log import LOG


class WasmSetupError(RuntimeError):
    """Raised when Wasmtime/WASI setup or instantiation fails."""


class WasmTrapError(RuntimeError):
    """Raised when the module traps during execution (excluding proc_exit)."""


async def run_wasi_module(
    exec_file: Path,
    argv: List[str],
    env: Dict[str, str],
    stdin_file: Optional[Path] = None,
    data_dir: Optional[Path] = None,
    stdout_file: Optional[Path] = None,
    stderr_file: Optional[Path] = None,
) -> int:
    """
    Execute a WASI WebAssembly module asynchronously and return its exit code.

    This offloads blocking Wasmtime work to the default thread executor so the event loop stays responsive.

    Args:
        exec_file (Path): Path to the `.wasm` binary.
        argv (List[str]): Program arguments (include argv[0] if your guest expects it).
        env (Dict[str, str]): Environment variables for the guest.
        stdin_file (Optional[Path]): Optional file to wire to WASI stdin.
        data_dir (Optional[Path]): Optional host directory to preopen as "/" in WASI.
        stdout_file (Optional[Path]): Optional file path to capture stdout.
        stderr_file (Optional[Path]): Optional file path to capture stderr.

    Returns:
        int: The exit code of the program.

    Raises:
        FileNotFoundError: If `exec_file` or `stdin_file` does not exist.
        NotADirectoryError: If `data_dir` exists but is not a directory.
        WasmSetupError: On compile/instantiate/WASI config failures.
        WasmTrapError: On runtime traps other than `proc_exit`.
        OSError: If output directories/files cannot be prepared.
        ValueError: If output file paths exist but are not regular files, or on other invalid arguments.
    """
    # Resolve paths early
    exec_file = exec_file.resolve()
    data_dir = data_dir.resolve() if data_dir else None
    stdin_file = stdin_file.resolve() if stdin_file else None
    stdout_file = stdout_file.resolve() if stdout_file else None
    stderr_file = stderr_file.resolve() if stderr_file else None

    # Validate exec_file
    if not exec_file.is_file():
        raise FileNotFoundError(f"WASM binary not found: {exec_file}")

    # Validate stdin_file
    if stdin_file and not stdin_file.is_file():
        raise FileNotFoundError(f"stdin file not found: {stdin_file}")

    # Validate data_dir
    if data_dir and not data_dir.exists():
        data_dir.mkdir(parents=True, exist_ok=True)
    if data_dir and not data_dir.is_dir():
        raise NotADirectoryError(f"data_dir is not a directory: {data_dir}")

    # Validate stdout_file
    if stdout_file:
        stdout_file.parent.mkdir(parents=True, exist_ok=True)
        if stdout_file.exists() and not stdout_file.is_file():
            raise ValueError(f"stdout_file is not a regular file: {stdout_file}")

    # Validate stderr_file
    if stderr_file:
        stderr_file.parent.mkdir(parents=True, exist_ok=True)
        if stderr_file.exists() and not stderr_file.is_file():
            raise ValueError(f"stderr_file is not a regular file: {stderr_file}")

    # Read WASM bytes
    with open(exec_file, "rb") as f:
        wasm_bytes = f.read()

    LOG.debug("Launching WASI module: %s", exec_file)

    loop = asyncio.get_running_loop()
    exit_code: int = await loop.run_in_executor(
        None,  # default thread pool
        _run_wasi_module_sync,
        wasm_bytes,
        argv,
        env,
        stdin_file,
        data_dir,
        stdout_file,
        stderr_file,
    )

    LOG.debug(
        "Finished WASI module: exit=%s, stdout=%s, stderr=%s",
        exit_code,
        stdout_file,
        stderr_file,
    )
    return exit_code


def _run_wasi_module_sync(
    wasm_bytes: bytes,
    argv: List[str],
    env: Dict[str, str],
    stdin_file: Optional[Path],
    data_dir: Optional[Path],
    stdout_file: Optional[Path],
    stderr_file: Optional[Path],
) -> int:
    """
    Run the module synchronously once and return its exit code.

    Args:
        wasm_bytes (bytes): The WebAssembly binary data.
        argv (List[str]): Program arguments (include argv[0] if your guest expects it).
        env (Dict[str, str]): Environment variables for the guest.
        stdin_file (Optional[Path]): Optional file to wire to WASI stdin.
        data_dir (Optional[Path]): Optional host directory to preopen as "/" in WASI.
        stdout_file (Optional[Path]): Optional file path to capture stdout.
        stderr_file (Optional[Path]): Optional file path to capture stderr.

    Returns:
        int: The exit code of the program.

    Raises:
        WasmSetupError: On compile/instantiate/WASI config failures.
        WasmTrapError: On runtime traps other than `proc_exit`.
    """
    try:
        store = Store()
        wasi = WasiConfig()

        # Arguments & environment
        wasi.argv = list(argv)
        wasi.env = [(str(k), str(v)) for k, v in env.items()]

        # Preopen the given host directory as "/"
        if data_dir:
            wasi.preopen_dir(str(data_dir), "/")

        # I/O redirection
        if stdin_file:
            wasi.stdin_file = str(stdin_file)
        wasi.stdout_file = str(stdout_file) if stdout_file else os.devnull
        wasi.stderr_file = str(stderr_file) if stderr_file else os.devnull

        store.set_wasi(wasi)

        # Compile and instantiate
        module = Module(store.engine, wasm_bytes)
        linker = Linker(store.engine)
        linker.define_wasi()
        instance = linker.instantiate(store, module)

        exports = instance.exports(store)
        start = exports.get("_start")
        if start is None:
            raise WasmSetupError("The module does not export a `_start` function.")

        start(store)

        # If the program returns normally, treat as success
        return 0
    except ExitTrap as et:
        return et.code
    except Trap as t:
        raise WasmTrapError(str(t)) from t
    except WasmtimeError as we:
        raise WasmSetupError(str(we)) from we
    except Exception as e:
        raise WasmTrapError(f"Unexpected error: {e}") from e
