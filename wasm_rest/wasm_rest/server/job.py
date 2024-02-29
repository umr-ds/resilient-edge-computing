import base64
import multiprocessing
import os
import random
import threading
import time
from typing import IO
from zipfile import ZipFile

import requests

from ..exceptions import WasmRestException
from ..execute_wasm import run_wasmtime
from ..model import JobStatus, Server as ServerModel, RunRequest
from ..util import prevent_break, put_file, zip_folder

send_retry = 10
send_timeout = 10


class Job:
    id: str
    dir: str
    code_dir: str
    wasm_binary: str
    data_dir: str
    out_dir: str
    result_path: str
    status: JobStatus

    def __init__(self, root_dir: str, file: IO[bytes], filename: str):
        while True:
            self.id = "id" + base64.urlsafe_b64encode(random.randbytes(32)).decode()[:-1]
            self.dir = os.path.join(root_dir, self.id)
            self.code_dir = os.path.join(self.dir, "code")
            self.data_dir = os.path.join(self.dir, "data")
            self.out_dir = os.path.join(self.dir, "out")
            self.result_path = os.path.join(self.dir, self.id + ".zip")
            self.wasm_binary = os.path.join(self.code_dir, filename)
            self.status = JobStatus.INIT
            try:
                os.makedirs(self.code_dir)
                os.makedirs(self.data_dir)
                os.makedirs(self.out_dir)
                break
            except OSError:
                pass  # TODO
        if not put_file(file, self.wasm_binary):
            self.status = JobStatus.ERROR

    def mkdirs(self, dirs: list[str]):
        for dir_to_create in dirs:
            dir_to_create = prevent_break(dir_to_create)
            path = os.path.join(self.data_dir, dir_to_create)
            os.makedirs(path, exist_ok=True)

    def run(self, cmd: RunRequest) -> bool:
        cmd.job_id = self.id
        if not os.path.exists(self.wasm_binary):
            return False

        new_res = {}
        for host_file, zip_path in cmd.results.items():
            new_res[os.path.join(self.data_dir, prevent_break(host_file))] = prevent_break(zip_path)
        cmd.results = new_res
        cmd.args.insert(0, "")
        threading.Thread(target=self._exec_wasm, kwargs={"cmd": cmd}).start()
        return True

    def send_result(self, server: ServerModel):
        if server.host != "":
            for _ in range(0, send_retry):
                try:
                    with open(self.result_path, "br") as file:
                        res = requests.put(f"http://{server.host}:{server.port}/result/{self.id}",
                                           files={"data": file})
                except requests.exceptions.RequestException:
                    time.sleep(send_timeout)
                    continue
                if res.ok or res.status_code == 404:
                    break
                time.sleep(send_timeout)



    def put_data(self, path: str, data: IO[bytes]) -> bool:
        return put_file(data, os.path.join(self.data_dir, path))

    def delete(self) -> bool:
        try:
            for root, _, files in os.walk(self.dir, topdown=False):
                for file in files:
                    os.remove(os.path.join(root, file))
                os.removedirs(root)
        except OSError:
            return False
        return True

    def _exec_wasm(self, cmd: RunRequest):  # TODO save errors

        stdin_file = os.path.join(self.data_dir, prevent_break(cmd.stdin_file)) if cmd.stdin_file else None

        self.status = JobStatus.RUNNING
        error = multiprocessing.Queue(1)
        wasm = multiprocessing.Process(target=run_wasmtime,
                                       args=(self.wasm_binary, self.data_dir,
                                             stdin_file, cmd.args, cmd.env,
                                             self.out_dir,
                                             error), daemon=True)
        wasm.start()
        err_msg = error.get(block=True)  # TODO display error
        wasm.join()

        try:
            with ZipFile(self.result_path, "w") as zip_file:
                zip_file.write(os.path.join(self.out_dir, "stdout.txt"), "stdout.txt")
                zip_file.write(os.path.join(self.out_dir, "stderr.txt"), "stderr.txt")
                for host_file, to_zip in cmd.results.items():
                    if os.path.isfile(host_file):
                        zip_file.write(host_file, to_zip)
                    if os.path.isdir(host_file):
                        zip_folder(zip_file, host_file, to_zip)
        except OSError:
            self.status = JobStatus.ERROR
            raise WasmRestException("Failed to save")

        if wasm.exitcode != 0:
            self.status = JobStatus.ERROR
        else:
            self.status = JobStatus.DONE
        self.send_result(cmd.res_destination)
