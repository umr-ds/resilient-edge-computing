import requests

from .job import Job
from ..exceptions import ServerException, raise_server_error
from ..model import Server as ServerModel, Capabilities


class Server(ServerModel):

    def submit(self, path: str):
        try:
            with open(path, "rb") as file:
                res = requests.put(f"http://{self.host}:{self.port}/submit",
                                   files={"binary": file})
        except requests.exceptions.RequestException:
            raise ServerException("Failed to communicate with server")
        if res.ok:
            return Job(host=self.host, port=self.port, job_id=res.content.decode()[1:-1])
        raise_server_error(res.content, "submit")

    def caps(self):
        try:
            res = requests.get(f"http://{self.host}:{self.port}/caps")
            if res.ok:
                return Capabilities.model_validate_json(res.content)
        except requests.exceptions.RequestException:
            raise ServerException("Failed to communicate with server")
        return None
