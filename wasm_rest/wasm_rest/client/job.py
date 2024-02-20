import json
import os

import requests

from ..exceptions import ServerException, raise_server_error
from ..model import Capabilities, Server

from typing import IO


class Job(Server):
    job_id: str

    def upload(self, file: IO[bytes], path: str):
        try:
            res = requests.put(f"http://{self.host}:{self.port}/upload/{self.job_id}/{path}",
                               files={"data": file})
        except requests.exceptions.RequestException:
            raise ServerException("Failed to communicate with server")
        if not res.ok:
            raise_server_error(res.content, "upload")

    def mkdir(self, dirs: list[str]):
        try:
            res = requests.put(f"http://{self.host}:{self.port}/mkdir/{self.job_id}",
                               data=json.dumps(dirs))
        except requests.exceptions.RequestException:
            raise ServerException("Failed to communicate with server")
        if not res.ok:
            raise_server_error(res.content, "mkdir")

    def run(self, stdin_file: str, args: list[str], env: dict[str, str], ndn_data: dict[str, str],
            results: dict[str, str], req: Capabilities, result_addr: Server):
        try:
            res = requests.put(f"http://{self.host}:{self.port}/run/{self.job_id}",
                               data=json.dumps(
                                   {"cmd":
                                       {
                                           "stdin_file": stdin_file,
                                           "args": args,
                                           "env": env,
                                           "ndn": ndn_data,
                                           "results": results,
                                           "res_destination": result_addr.model_dump()},
                                       "req": req.model_dump(),
                                   }))
        except requests.exceptions.RequestException:
            raise ServerException("Failed to communicate with server")
        if not res.ok:
            raise_server_error(res.content, "run")

    def result(self, path: str):
        file_path = path
        if os.path.isdir(file_path) or file_path.endswith(os.path.sep):
            file_path = os.path.join(file_path, f"{self.job_id}.zip")
        try:
            res = requests.get(f"http://{self.host}:{self.port}/result/{self.job_id}", stream=True)
            if res.status_code == 200:
                with open(file_path, "bw") as file:
                    for chunk in res.iter_content(chunk_size=65536):
                        file.write(chunk)
                return file_path
        except requests.exceptions.RequestException:
            raise ServerException("Failed to communicate with server")
        return None

    def delete(self) -> bool:
        try:
            res = requests.delete(f"http://{self.host}:{self.port}/delete/{self.job_id}")
        except requests.exceptions.RequestException:
            raise ServerException("Failed to communicate with server")
        return res.ok
