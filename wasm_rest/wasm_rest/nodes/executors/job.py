import os
import threading
import time
from typing import Callable
from zipfile import ZipFile

from wasm_rest.exceptions import WasmRestException
from wasm_rest.model import JobInfo, Address
from wasm_rest.nodetypes.broker import Broker
from wasm_rest.nodetypes.client import Client
from wasm_rest.util.execute_wasm import run_webassembly
from wasm_rest.util.util import prevent_breakout, zip_folder, try_download_file, try_store_named_data

send_retry = 10
send_timeout = 10


class Job:
    id: str
    dir: str
    code_dir: str
    data_dir: str
    out_dir: str
    result_path: str
    job_info: JobInfo
    __to_store_named: dict[str, str] = {}
    __on_complete: Callable[['Job', bool], None]

    # status: JobStatus

    def __init__(self, root_dir: str, job_id: str, job_info: JobInfo,
                 on_complete: Callable[['Job', bool], None]) -> None:
        self.job_info = job_info
        self.__on_complete = on_complete
        self.id = job_id
        self.dir = os.path.join(root_dir, self.id)
        self.code_dir = os.path.join(self.dir, "code")
        self.data_dir = os.path.join(self.dir, "data")
        self.out_dir = os.path.join(self.dir, "out")
        self.result_path = os.path.join(self.dir, self.id + ".zip")
        # self.status = JobStatus.INIT
        try:
            os.makedirs(self.code_dir)
            os.makedirs(self.data_dir)
            os.makedirs(self.out_dir)
        except OSError:
            # self.status = JobStatus.ERROR
            raise WasmRestException("failed create")
        self.__transform_job_info()
        self.mkdirs()

    def __transform_job_info(self) -> None:
        try:
            if type(self.job_info.wasm_bin) is str:
                self.job_info.wasm_bin = (self.job_info.wasm_bin, os.path.join(self.code_dir, "exec.wasm"))
            elif type(self.job_info.wasm_bin) is tuple:
                self.job_info.wasm_bin = (self.job_info.wasm_bin[0],
                                          os.path.join(self.code_dir, prevent_breakout(self.job_info.wasm_bin[1])))
            else:
                raise WasmRestException("oops")  # TODO

            if type(self.job_info.stdin) is str:
                if self.job_info.stdin != "":
                    self.job_info.stdin = self.data_path(self.job_info.stdin)
            elif type(self.job_info.stdin) is tuple:
                if self.job_info.stdin_is_named:
                    self.job_info.job_data[self.job_info.stdin[0]] = self.job_info.stdin[1]
                else:
                    self.job_info.job_data[self.job_data_name(self.job_info.stdin[0])] = self.job_info.stdin[1]
                self.job_info.stdin = self.data_path(self.job_info.stdin[1])
            else:
                raise WasmRestException("oops")  # TODO

            self.job_info.directories = [self.data_path(directory) for directory in self.job_info.directories]

            self.job_info.job_data = {name: self.data_path(path)
                                      for name, path in self.job_info.job_data.items()}

            for name, path in self.job_info.named_data.items():
                self.job_info.named_data[name] = self.data_path(path)

            self.job_info.zip_results = {self.data_path(host_path): prevent_breakout(zip_path)
                                         for host_path, zip_path in self.job_info.zip_results.items()}

            self.job_info.named_results = {self.data_path(host_path): name
                                           for host_path, name in self.job_info.named_results.items()}

            self.__to_store_named = {name: path for name, path in self.job_info.named_results.items()}
            self.job_info.args.insert(0, "")  # first argument typically program name empty here
        except ValueError as e:
            raise WasmRestException("Invalid Formatting") from e

    def mkdirs(self, dirs: list[str] = None) -> None:
        for dir_to_create in dirs if dirs is not None else self.job_info.directories:
            os.makedirs(dir_to_create, exist_ok=True)

    def try_download_files(self, broker: Broker) -> bool:
        if not try_download_file(*self.job_info.wasm_bin, broker):
            return False

        for name, path in self.job_info.job_data.items():
            if not try_download_file(name, path, broker, self.id):
                return False

        for name, path in self.job_info.named_data.items():
            if not try_download_file(name, path, broker):
                return False
        return True

    def data_path(self, path: str) -> str:
        return os.path.join(self.data_dir, prevent_breakout(path))

    def job_data_name(self, name: str) -> str:
        return self.id + "/" + name

    def start(self) -> bool:
        if not os.path.exists(self.job_info.wasm_bin[1]):
            return False
        threading.Thread(target=self.__exec_wasm).start()
        return True

    def send_result(self, address: Address) -> None:
        sent = False
        if address.host != "":
            client = Client(address=address, id='')
            for _ in range(0, send_retry):
                with open(self.result_path, "br") as file:
                    if not client.send_result(self.id, file):
                        time.sleep(send_timeout)
                        continue
                    else:
                        sent = True
                        break
        self.__on_complete(self, sent)

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
        if len(self.__to_store_named) == 0:
            return True
        for name, path in self.job_info.named_results.items():
            if try_store_named_data(path, name, broker):
                to_remove.append(name)
            else:
                break
        if len(to_remove) == len(self.__to_store_named):
            self.__to_store_named.clear()
            return True
        else:
            for name in to_remove:
                del self.__to_store_named[name]
            return False

    def __exec_wasm(self) -> None:  # TODO save errors
        try:
            # self.status = JobStatus.RUNNING
            run_webassembly(self.job_info.wasm_bin[1], self.data_dir, self.job_info.stdin,
                            self.job_info.args, self.job_info.env, self.out_dir)
            try:
                with ZipFile(self.result_path, "w") as zip_file:
                    zip_file.write(os.path.join(self.out_dir, "stdout.txt"), "stdout.txt")
                    zip_file.write(os.path.join(self.out_dir, "stderr.txt"), "stderr.txt")
                    for host_file, to_zip in self.job_info.zip_results.items():
                        if os.path.isfile(host_file):
                            zip_file.write(host_file, to_zip)
                        if os.path.isdir(host_file):
                            zip_folder(zip_file, host_file, to_zip)
            except OSError:
                # self.status = JobStatus.ERROR
                raise WasmRestException("Failed to save")
        except WasmRestException as e:
            with open(os.path.join(self.out_dir, "stderr.txt"), "a") as file:
                file.write(e.msg)

        # if wasm.exitcode != 0:
        #    self.status = JobStatus.ERROR
        # else:
        #    self.status = JobStatus.DONE
        self.send_result(self.job_info.result_addr)
