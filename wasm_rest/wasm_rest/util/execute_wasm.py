import multiprocessing
import os
from typing import Optional

from wasmtime import Store, Module, Linker, WasiConfig, Trap, WasmtimeError

from wasm_rest.exceptions import WasmRestException


def run_webassembly(exec_path: str, data_path: str, stdin_file: Optional[str],
                    argv: list[str], env: dict[str, str],
                    out_path: str) -> None:
    error = multiprocessing.Queue(1)
    wasm = multiprocessing.Process(target=run_wasmtime,
                                   args=(exec_path, data_path,
                                         stdin_file, argv, env,
                                         out_path,
                                         error), daemon=True)
    wasm.start()
    err_msg = error.get(block=True)
    wasm.join()
    wasm.close()
    if err_msg != "":
        raise WasmRestException(err_msg)


def run_wasmtime(exec_path: str, data_path: str, stdin_file: Optional[str],
                 argv: list[str], env: dict[str, str],
                 out_path: str, error: multiprocessing.Queue) -> None:
    try:
        with open(exec_path, "br") as wasm_file:
            wasm = wasm_file.read()
        wasi_env_var = []
        for key, value in env.items():
            wasi_env_var.append([key, value])
        store = Store()
        wasi = WasiConfig()
        wasi.preopen_dir(data_path, "/")
        wasi.argv = argv
        wasi.env = wasi_env_var
        if stdin_file:
            wasi.stdin_file = stdin_file
        wasi.stdout_file = os.path.join(out_path, "stdout.txt")
        wasi.stderr_file = os.path.join(out_path, "stderr.txt")
        store.set_wasi(wasi)
        module = Module(store.engine, wasm)

        linker = Linker(store.engine)
        linker.define_wasi()

        instance = linker.instantiate(store, module)
        function = instance.exports(store)["_start"]
        try:
            function(store)
            error.put("")
        except Trap as t:
            error.put(f"Encountered Webassembly Trap: {t.message}")
            # raise WasmRestException(f"Encountered Webassembly Trap {t.message}")
    except WasmtimeError as e:
        error.put(f"Failed to set up Webassembly Runtime: {e}")
        # raise WasmRestException("Failed to set up Webassembly Runtime")
    except OSError:
        error.put("Failed to open binary")
        # raise WasmRestException("Failed to open binary")
