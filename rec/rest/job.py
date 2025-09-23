import glob
import os
from typing import IO, Callable, Optional
from uuid import UUID
from zipfile import ZipFile

from rec.rest.model import JobInfo
from rec.rest.nodetypes.broker import Broker
from rec.util.exceptions import WasmRestException
from rec.util.execute_wasm import run_webassembly
from rec.util.fs import (
    prevent_breakout,
    try_download_file,
    try_store_named_data,
    zip_folder,
)
from rec.util.log import LOG


class ClientJob:
    id: UUID

    def __init__(self, job_id: UUID) -> None:
        self.id = job_id

    def job_data_name(self, name: str) -> str:
        return f"{self.id}/{name}"

    def upload_job_file(self, name: str, path: str, broker: Broker) -> bool:
        return try_store_named_data(self.job_data_name(name), path, broker)

    def transform_job_info_broker(self, job_info: JobInfo) -> None:
        try:
            if not job_info.wasm_bin_is_named:
                if type(job_info.wasm_bin) is str:
                    job_info.wasm_bin = self.job_data_name("exec.wasm")
                elif type(job_info.wasm_bin) is tuple:
                    job_info.wasm_bin = (
                        self.job_data_name(job_info.wasm_bin[1]),
                        job_info.wasm_bin[1],
                    )
                else:
                    raise WasmRestException("Invalid Formatting in wasm_bin")
            if not job_info.stdin_is_named:
                if type(job_info.stdin) is str:
                    job_info.job_data[self.job_data_name(job_info.stdin[1])] = "stdin"
                    job_info.stdin = "stdin"
                elif type(job_info.stdin) is tuple:
                    if job_info.stdin[0] != "":
                        job_info.job_data[self.job_data_name(job_info.stdin[1])] = (
                            job_info.stdin[1]
                        )
                        job_info.stdin = (
                            job_info.job_data[self.job_data_name(job_info.stdin[1])],
                            job_info.stdin[1],
                        )
                else:
                    raise WasmRestException("Invalid Formatting in stdin")

            job_info.job_data = {
                self.job_data_name(path): path for _, path in job_info.job_data.items()
            }

        except ValueError as e:
            raise WasmRestException("Invalid Formatting") from e

    def poll_finished(self, file: Optional[IO[bytes]], broker: Broker) -> bool:
        return broker.get_data(file, f"{self.id}/result", self.id)

    def delete(self, broker: Broker):
        broker.delete_job(self.id)


send_retry = 10
send_timeout = 10


class ExecutorInternalJobInfo:
    wasm_bin: tuple[str, str]

    stdin: Optional[str] = None
    env: dict[str, str]
    args: list[str]

    job_data: dict[str, str]
    named_data: dict[str, str]

    to_store_named: dict[str, str]
    zip_results: dict[str, str]
    named_results: dict[str, str]

    def __init__(self, job_info: JobInfo, job: "ExecutorJob") -> None:
        try:
            if type(job_info.wasm_bin) is str:
                self.wasm_bin = (
                    job_info.wasm_bin,
                    os.path.join(job.code_dir, "exec.wasm"),
                )
            elif type(job_info.wasm_bin) is tuple:
                self.wasm_bin = (
                    job_info.wasm_bin[0],
                    os.path.join(job.code_dir, prevent_breakout(job_info.wasm_bin[1])),
                )
            else:
                raise WasmRestException("Invalid Formatting in wasm_bin")

            if type(job_info.stdin) is str:
                if job_info.stdin != "":
                    self.stdin = job.data_path(job_info.stdin)
            elif type(job_info.stdin) is tuple:
                if job_info.stdin[0] != "":
                    if job_info.stdin_is_named:
                        self.job_data[job_info.stdin[0]] = job_info.stdin[1]
                    else:
                        self.job_data[job.job_data_name(job_info.stdin[0])] = (
                            job_info.stdin[1]
                        )
                    self.stdin = job.data_path(job_info.stdin[1])
            else:
                raise WasmRestException("Invalid Formatting in stdin")

            self.job_data = {
                name: job.data_path(path) for name, path in job_info.job_data.items()
            }

            self.named_data = {
                name: job.data_path(path) for name, path in job_info.named_data.items()
            }

            self.zip_results = {
                job.data_path(host_path): prevent_breakout(zip_path)
                for host_path, zip_path in job_info.zip_results.items()
            }

            self.named_results = {
                job.data_path(host_path): name
                for host_path, name in job_info.named_results.items()
            }

            self.to_store_named = {
                path: name for path, name in self.named_results.items()
            }

            self.env = job_info.env
            self.args = list(
                job_info.args
            )  # in case of no arguments to not pollute the default list
            self.args.insert(
                0, os.path.basename(self.wasm_bin[1])
            )  # first argument program name
        except ValueError as e:
            raise WasmRestException("Invalid Formatting") from e


class ExecutorJob:
    id: UUID
    dir: str
    code_dir: str
    data_dir: str
    out_dir: str
    result_path: str
    job_info: JobInfo
    internal_job_info: ExecutorInternalJobInfo
    __on_complete: Callable[["ExecutorJob"], None]

    def __init__(
        self,
        root_dir: str,
        job_id: UUID,
        job_info: JobInfo,
        on_complete: Callable[["ExecutorJob"], None],
    ) -> None:
        self.__on_complete = on_complete
        self.job_info = job_info
        self.id = job_id
        self.dir = os.path.join(root_dir, str(self.id))
        self.code_dir = os.path.join(self.dir, "code")
        self.data_dir = os.path.join(self.dir, "data")
        self.out_dir = os.path.join(self.dir, "out")
        self.result_path = os.path.join(self.dir, str(self.id) + ".zip")
        try:
            os.makedirs(self.code_dir)
            os.makedirs(self.data_dir)
            os.makedirs(self.out_dir)
        except OSError:
            raise WasmRestException("failed create")
        self.mkdirs()
        self.internal_job_info = ExecutorInternalJobInfo(job_info, self)

    def mkdirs(self) -> None:
        for dir_to_create in [
            self.data_path(directory) for directory in self.job_info.directories
        ]:
            os.makedirs(dir_to_create, exist_ok=True)

    def resolve_glob_data(self, broker: Broker):
        to_add = {}
        to_remove = []
        for name, path in self.internal_job_info.named_data.items():
            if name.endswith("*") and path.endswith("*"):
                names = broker.get_data_glob(name[:-1])
                if names:
                    for full_name in names:
                        to_add[full_name] = path[:-1] + full_name[len(name) - 1 :]
                to_remove.append(name)
        for name, path in to_add.items():
            self.internal_job_info.named_data[name] = path
        for name in to_remove:
            self.internal_job_info.named_data.pop(name, None)

    def resolve_glob_results(self):
        to_add = {}
        to_remove = []
        for path, name in self.internal_job_info.named_results.items():
            if name.endswith("*") and path.endswith("*") and path.count("*") == 1:
                paths = glob.glob(path)
                for full_path in paths:
                    to_add[full_path] = name[:-1] + full_path[len(path) - 1 :]
                to_remove.append(path)
        for path, name in to_add.items():
            self.internal_job_info.named_results[path] = name
            self.internal_job_info.to_store_named[path] = name
        for path in to_remove:
            self.internal_job_info.named_results.pop(path, None)
            self.internal_job_info.to_store_named.pop(path, None)

    def try_download_files(self, broker: Broker) -> bool:
        if not try_download_file(
            self.internal_job_info.wasm_bin[0],
            self.internal_job_info.wasm_bin[1],
            broker,
        ):
            return False

        for name, path in self.internal_job_info.job_data.items():
            if not try_download_file(name, path, broker, self.id):
                return False

        for name, path in self.internal_job_info.named_data.items():
            if not try_download_file(name, path, broker):
                return False
        return True

    def data_path(self, path: str) -> str:
        return os.path.join(self.data_dir, prevent_breakout(path))

    def job_data_name(self, name: str) -> str:
        return f"{self.id}/{name}"

    def send_result(self) -> None:
        self.__on_complete(self)

    def delete(self) -> bool:
        try:
            for root, _, files in os.walk(self.dir, topdown=False):
                for file in files:
                    os.remove(os.path.join(root, file))
                os.removedirs(root)
        except OSError:
            return False
        return True

    def store_named(self, broker: Broker) -> bool:
        to_remove = []
        if len(self.internal_job_info.to_store_named) == 0:
            return True
        for name, path in self.internal_job_info.to_store_named.items():
            if try_store_named_data(path, name, broker):
                to_remove.append(name)
            else:
                break
        if len(to_remove) == len(self.internal_job_info.to_store_named):
            self.internal_job_info.to_store_named.clear()
            return True
        else:
            for name in to_remove:
                self.internal_job_info.to_store_named.pop(name, None)
            return False

    def run(self) -> None:
        try:
            run_webassembly(
                self.internal_job_info.wasm_bin[1],
                self.data_dir,
                self.internal_job_info.stdin,
                self.internal_job_info.args,
                self.internal_job_info.env,
                self.out_dir,
            )
            LOG.debug(f"Finished executing job {self.id}")
        except WasmRestException as e:
            with open(os.path.join(self.out_dir, "stderr.txt"), "a") as file:
                file.write(e.msg)
            LOG.error(f"Failed to execute job {self.id}: {e.msg}")
        try:
            with ZipFile(self.result_path, "w") as zip_file:
                zip_file.write(os.path.join(self.out_dir, "stdout.txt"), "stdout.txt")
                zip_file.write(os.path.join(self.out_dir, "stderr.txt"), "stderr.txt")
                for host_file, to_zip in self.internal_job_info.zip_results.items():
                    if os.path.isfile(host_file):
                        zip_file.write(host_file, to_zip)
                    if os.path.isdir(host_file):
                        zip_folder(zip_file, host_file, to_zip)
        except OSError:
            try:
                with ZipFile(self.result_path, "w") as zip_file:
                    zip_file.writestr("err.txt", "Failed to save result")
            except OSError:
                raise WasmRestException("Failed to save")

        self.resolve_glob_results()
        self.send_result()
