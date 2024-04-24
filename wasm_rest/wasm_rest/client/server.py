from typing import Optional
import requests

from .job import Job
from ..exceptions import ServerException, raise_server_error
from ..model import Capabilities
from ..server.executor import Executor


class Server:
    executor: Executor

    def __int__(self, executor: Executor):
        self.executor = executor

    def submit(self, path: str) -> Job:
        try:
            with open(path, "rb") as file:
                res = requests.put(f"http://{self.executor.node.address.host}:{self.executor.node.address.port}/submit",
                                   files={"binary": file})
        except requests.exceptions.RequestException:
            raise ServerException("Failed to communicate with server")
        if res.ok:
            return Job(host=self.executor.node.address.host, port=self.executor.node.address.port,
                       job_id=res.content.decode()[1:-1])
        raise_server_error(res.content, "submit")

    def caps(self) -> Optional[Capabilities]:
        return self.executor.update_capabilities()
