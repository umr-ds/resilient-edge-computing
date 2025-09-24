import asyncio
import io
import os
import shutil
import zipfile
from collections import deque
from pathlib import Path
from typing import override

import msgpack
from wasmtime import ExitTrap, Linker, Module, Store, Trap, WasiConfig, WasmtimeError

from rec.dtn.eid import EID
from rec.dtn.job import Capabilities, Job, JobInfo
from rec.dtn.messages import (
    DATASTORE_MULTICAST_ADDRESS,
    BundleCreate,
    BundleData,
    BundleType,
    MessageType,
    NodeType,
)
from rec.dtn.node import Node
from rec.dtn.storage import Storage
from rec.util.log import LOG


class WasmSetupError(RuntimeError):
    """Raised when Wasmtime/WASI setup or instantiation fails."""


class WasmTrapError(RuntimeError):
    """Raised when the module traps during execution (excluding proc_exit)."""


class Executor(Node):
    _root_dir: Path
    _storage: Storage
    _pending_jobs: deque[JobInfo]
    _job_ready_cv: asyncio.Condition

    def __init__(self, node_id: EID, dtn_agent_socket: str, root_dir: Path) -> None:
        super().__init__(
            node_id=node_id,
            dtn_agent_socket=dtn_agent_socket,
            node_type=NodeType.EXECUTOR,
        )

        self._root_dir = root_dir
        self._root_dir.mkdir(parents=True, exist_ok=True)

        db_path = self._root_dir / "database.db"
        blob_directory = self._root_dir / "blobs"
        self._storage = Storage(db_path, blob_directory)

        self._pending_jobs: deque[JobInfo] = deque()
        self._job_ready_cv = asyncio.Condition()

    @override
    async def run(self) -> None:
        LOG.info("Starting executor")
        await self._register()
        # Run bundle handler & scheduler concurrently
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._handle_bundles())
            tg.create_task(self._scheduler())

    async def _handle_bundles(self) -> None:
        LOG.info("Starting bundle handler")
        while True:
            LOG.debug("Bundle handler going to sleep")
            await asyncio.sleep(10)

            LOG.debug("Running bundle handler")
            try:
                LOG.debug("Retrieving bundles")
                bundles = await self._get_new_bundles()
                if bundles:
                    LOG.debug(f"Bundles: {bundles}")
                    for bundle in bundles:
                        await self._handle_bundle(bundle=bundle)
                else:
                    LOG.debug("No new bundles")
            except Exception as err:
                LOG.exception("Error fetching bundles: %s", err)

    async def _handle_bundle(self, bundle: BundleData) -> None:
        to_send: list[BundleData] = []

        if BundleType.BROKER_ANNOUNCE <= bundle.type <= BundleType.BROKER_ACK:
            to_send = await self._handle_discovery(bundle=bundle)
        if bundle.type == BundleType.JOB_SUBMIT:
            to_send = await self._handle_job(bundle=bundle)
        if bundle.type == BundleType.NDATA_GET:
            await self._handle_data(bundle=bundle)

        if to_send:
            try:
                LOG.debug("Sending bundles")
                dtnd_responses = await self._send_bundles(bundles=to_send)
                for dtnd_response in dtnd_responses:
                    if not dtnd_response.success:
                        LOG.exception("dtnd sent error: %s", dtnd_response.error)
            except Exception as err:
                LOG.exception("error communicating with dtnd: %s", err, exc_info=True)

    async def _handle_job(self, bundle: BundleData) -> list[BundleData]:
        LOG.debug(f"Received JobBundle: {bundle}")
        to_send: list[BundleData] = []

        job: Job = msgpack.unpackb(bundle.payload)

        for name, data in job.data.items():
            await self._storage.store_data(name=name, data=data)
        missing = await self._storage.find_missing(job.metadata.required_named_data())

        async with self._state_mutex.writer_lock:
            self._pending_jobs.append(job.metadata)
        async with self._job_ready_cv:
            self._job_ready_cv.notify_all()

        # TODO: send ACK?

        if missing:
            LOG.info(f"Job is missing named data: {missing}")

            # Request missing named data from datastores
            request = BundleData(
                type=BundleType.NDATA_GET,
                source=self.node_id,
                destination=DATASTORE_MULTICAST_ADDRESS,
                payload=b"",
                named_data=list(missing),
            )
            to_send.append(request)

        return to_send

    async def _handle_data(self, bundle: BundleData) -> None:
        LOG.debug(f"Received NamedData: {bundle}")

        if isinstance(bundle.named_data, str):
            bundle.named_data = [bundle.named_data]

        for name in bundle.named_data:
            await self._storage.store_data(name=name, data=bundle.payload)

        async with self._job_ready_cv:
            self._job_ready_cv.notify_all()

        # TODO: send ACK?

    async def _job_has_all_inputs(self, job: JobInfo) -> bool:
        """
        Check if all required named data for a job is available in the cache.

        Args:
            job (JobInfo): The job to check.

        Returns:
            bool: True if all required data is cached, False otherwise.
        """
        missing = await self._storage.find_missing(job.required_named_data())
        return len(missing) == 0

    def _system_can_run(self, job: JobInfo) -> bool:
        current = Capabilities.from_system()
        return current.is_capable_of(job.capabilities)

    async def _has_runnable_job(self) -> bool:
        async with self._state_mutex.reader_lock:
            for j in self._pending_jobs:
                if await self._job_has_all_inputs(j) and self._system_can_run(j):
                    return True
            return False

    async def _pop_next_runnable_job(self) -> JobInfo | None:
        async with self._state_mutex.writer_lock:
            for _ in range(len(self._pending_jobs)):
                job = self._pending_jobs.popleft()
                if await self._job_has_all_inputs(job) and self._system_can_run(job):
                    return job
                # Not runnable yet
                self._pending_jobs.append(job)
            return None

    async def _scheduler(self) -> None:
        LOG.info("Starting scheduler")
        while True:
            # Wait until at least one job is runnable
            async with self._job_ready_cv:
                while not await self._has_runnable_job():
                    await self._job_ready_cv.wait()

            job = await self._pop_next_runnable_job()
            if job is None:
                # State changed between predicate and pop
                continue

            try:
                results, named_results = await self._run_job(job)
                await self._send_results(job, results)
                await self._store_named_results(named_results)
                await self._send_named_results(named_results)
            except Exception:
                LOG.exception("Job failed: %s", job)
            finally:
                # Allow new jobs to be scheduled
                async with self._job_ready_cv:
                    self._job_ready_cv.notify_all()

    async def _send_results(self, job: JobInfo, results: bytes | None) -> None:
        if not results:
            LOG.info("No results to send")
            return
        if not job.results_receiver:
            LOG.info("No result receiver specified, skipping sending results")
            return

        bundle = BundleData(
            type=BundleType.JOB_RESULT,
            source=self.node_id,
            destination=job.results_receiver,
            payload=results,
        )
        message = BundleCreate(type=MessageType.CREATE, bundle=bundle)

        try:
            dtnd_response = await self._send_message(message)
            if not dtnd_response.success:
                LOG.error("dtnd sent error: %s", dtnd_response.error)
        except Exception as err:
            LOG.exception("error communicating with dtnd: %s", err, exc_info=True)

    async def _store_named_results(self, results: dict[str, bytes]) -> None:
        if not results:
            LOG.info("No named results to store")
            return

        for name, data in results.items():
            await self._storage.store_data(name=name, data=data)

    async def _send_named_results(self, results: dict[str, bytes]) -> None:
        if not results:
            LOG.info("No named results to send")
            return

        for name, data in results.items():
            bundle = BundleData(
                type=BundleType.NDATA_PUT,
                source=self.node_id,
                destination=DATASTORE_MULTICAST_ADDRESS,
                payload=data,
                named_data=name,
            )
            message = BundleCreate(type=MessageType.CREATE, bundle=bundle)

            try:
                dtnd_response = await self._send_message(message)
                if not dtnd_response.success:
                    LOG.error("dtnd sent error: %s", dtnd_response.error)
            except Exception as err:
                LOG.exception("error communicating with dtnd: %s", err, exc_info=True)

    async def _run_job(self, job: JobInfo) -> tuple[bytes | None, dict[str, bytes]]:
        LOG.info("Starting job: %s", job)
        async with self._state_mutex.writer_lock:
            job_dir = (self._root_dir / f"job-{id(job)}").resolve()
            job_dir.mkdir(parents=True, exist_ok=True)
            wasm_file, stdin_path, data_dir = await self._prepare_wasi_environment(
                job, job_dir
            )

        try:
            exit_code = await _run_wasi_module(
                exec_file=wasm_file,
                argv=job.argv,
                env=job.env,
                stdin_file=stdin_path,
                data_dir=data_dir,
                stdout_file=(
                    (data_dir / job.stdout_file.lstrip("/"))
                    if job.stdout_file
                    else None
                ),
                stderr_file=(
                    (data_dir / job.stderr_file.lstrip("/"))
                    if job.stderr_file
                    else None
                ),
            )
            LOG.info("Job exit code: %s", exit_code)

            results = await self._collect_results(job, job_dir)
            named_results = await self._collect_named_results(job, job_dir)
            return results, named_results
        except Exception as e:
            LOG.exception("Job failed: %s", e)
            return None, {}
        finally:
            if job_dir and job_dir.exists():
                shutil.rmtree(job_dir, ignore_errors=True)

    async def _prepare_wasi_environment(
        self, job: JobInfo, base_dir: Path
    ) -> tuple[Path, Path | None, Path]:
        """
        Prepare the filesystem and environment for a WASI job execution.

        Args:
            job (JobInfo): The job specification.
            base_dir (Path): The base directory where the job's filesystem will be set up.

        Returns:
            tuple[Path, Path | None, Path]: Paths to the WASM module, stdin file (if any), and the directory to preopen as "/".

        Raises:
            NoSuchNameError: If any named data is missing.
            OSError: If directories or files cannot be created.
            ValueError: If any paths are invalid.
        """
        base_dir.mkdir(parents=True, exist_ok=True)

        # Write WASM module to file
        wasm_path = (base_dir / "module.wasm").resolve()
        await self._storage.copy_to_file(job.wasm_module, wasm_path)

        # Prepare stdin file if provided
        stdin_path: Path | None = None
        if job.stdin_file:
            stdin_path = (base_dir / "stdin.bin").resolve()
            await self._storage.copy_to_file(job.stdin_file, stdin_path)

        # Create data directory
        data_dir = (base_dir / "data").resolve()
        data_dir.mkdir(parents=True, exist_ok=True)

        # Create specified directories
        for dir_path in job.dirs:
            abs_dir_path = (data_dir / dir_path.lstrip("/")).resolve()
            if not abs_dir_path.is_relative_to(data_dir):
                raise ValueError(f"Directory path escapes data directory: {dir_path}")
            abs_dir_path.mkdir(parents=True, exist_ok=True)

        # Write data files
        for file_path, data in job.data.items():
            abs_file_path = (data_dir / file_path.lstrip("/")).resolve()
            if not abs_file_path.is_relative_to(data_dir):
                raise ValueError(f"Data file path escapes data directory: {file_path}")
            abs_file_path.parent.mkdir(parents=True, exist_ok=True)

            await self._storage.copy_to_file(data, abs_file_path)

        # Prepare stdout parent directory if specified
        if job.stdout_file:
            stdout_path = (data_dir / job.stdout_file.lstrip("/")).resolve()
            if not stdout_path.is_relative_to(data_dir):
                raise ValueError(
                    f"stdout_file path escapes data directory: {job.stdout_file}"
                )
            stdout_path.parent.mkdir(parents=True, exist_ok=True)

        # Prepare stderr parent directory if specified
        if job.stderr_file:
            stderr_path = (data_dir / job.stderr_file.lstrip("/")).resolve()
            if not stderr_path.is_relative_to(data_dir):
                raise ValueError(
                    f"stderr_file path escapes data directory: {job.stderr_file}"
                )
            stderr_path.parent.mkdir(parents=True, exist_ok=True)

        return wasm_path, stdin_path, data_dir

    async def _collect_results(self, job: JobInfo, base_dir: Path) -> bytes | None:
        if not job.results_receiver:
            return None

        data_dir = (base_dir / "data").resolve()
        zip_path = (base_dir / "results.zip").resolve()

        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for path in job.results:
                abs_path = (data_dir / path.lstrip("/")).resolve()
                if not abs_path.is_relative_to(data_dir):
                    LOG.error(f"Result path escapes data directory: {path}, skipping")
                    continue
                if not abs_path.exists():
                    LOG.error(f"Result path does not exist: {abs_path}, skipping")
                    continue

                if abs_path.is_file():
                    arcname = abs_path.relative_to(data_dir)
                    zf.write(abs_path, arcname)
                elif abs_path.is_dir():
                    for p in abs_path.rglob("*"):
                        arcname = p.relative_to(data_dir)
                        if p.is_file():
                            zf.write(p, arcname)
                else:
                    LOG.error(
                        f"Result path is neither file nor directory: {abs_path}, skipping"
                    )

        with open(zip_path, "rb") as f:
            return f.read()

    async def _collect_named_results(
        self, job: JobInfo, base_dir: Path
    ) -> dict[str, bytes]:
        results: dict[str, bytes] = {}
        data_dir = (base_dir / "data").resolve()

        for path, name in job.named_results.items():
            abs_path = (data_dir / path.lstrip("/")).resolve()
            if not abs_path.is_relative_to(data_dir):
                LOG.error(f"Result path escapes data directory: {path}, skipping")
                continue
            if not abs_path.exists():
                LOG.error(f"Result path does not exist: {abs_path}, skipping")
                continue

            if abs_path.is_file():
                with open(abs_path, "rb") as f:
                    results[name] = f.read()
            elif abs_path.is_dir():
                # Zip the directory
                with io.BytesIO() as buf:
                    with zipfile.ZipFile(
                        buf, "w", compression=zipfile.ZIP_DEFLATED
                    ) as zf:
                        for p in abs_path.rglob("*"):
                            arcname = p.relative_to(abs_path.parent)
                            if p.is_file():
                                zf.write(p, arcname)
                    buf.seek(0)
                    results[name] = buf.read()
            else:
                LOG.error(
                    f"Result path is neither file nor directory: {abs_path}, skipping"
                )

        return results


async def _run_wasi_module(
    exec_file: Path,
    argv: list[str],
    env: dict[str, str],
    stdin_file: Path | None = None,
    data_dir: Path | None = None,
    stdout_file: Path | None = None,
    stderr_file: Path | None = None,
) -> int:
    """
    Execute a WASI WebAssembly module asynchronously and return its exit code.

    This offloads blocking Wasmtime work to the default thread executor so the event loop stays responsive.

    Args:
        exec_file (Path): Path to the `.wasm` binary.
        argv (list[str]): Program arguments (include argv[0] if your guest expects it).
        env (dict[str, str]): Environment variables for the guest.
        stdin_file (Path | None): Optional file to wire to WASI stdin.
        data_dir (Path | None): Optional host directory to preopen as "/" in WASI.
        stdout_file (Path | None): Optional file path to capture stdout.
        stderr_file (Path | None): Optional file path to capture stderr.

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
    argv: list[str],
    env: dict[str, str],
    stdin_file: Path | None = None,
    data_dir: Path | None = None,
    stdout_file: Path | None = None,
    stderr_file: Path | None = None,
) -> int:
    """
    Run the module synchronously once and return its exit code.

    Args:
        wasm_bytes (bytes): The WebAssembly binary data.
        argv (list[str]): Program arguments (include argv[0] if your guest expects it).
        env (dict[str, str]): Environment variables for the guest.
        stdin_file (Path | None): Optional file to wire to WASI stdin.
        data_dir (Path | None): Optional host directory to preopen as "/" in WASI.
        stdout_file (Path | None): Optional file path to capture stdout.
        stderr_file (Path | None): Optional file path to capture stderr.

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
