from typing import Optional

import requests
from pydantic import BaseModel

from wasm_rest.model import Capabilities, Command, Node
from wasm_rest.server.database import Database
from wasm_rest.server.executor import Executor


class Broker(BaseModel):
    node: Node
    execs: int = 0

    def register_executor(self, executor: Executor) -> bool:
        try:
            return requests.put(self.node.address.http_str("/register"),
                                data=executor.model_dump_json()).ok
        except requests.exceptions.RequestException:
            return False

    def get_executor(self, capabilities: Capabilities) -> Optional[Executor]:
        try:
            res = requests.get(self.node.address.http_str("/executor"),
                               data=capabilities.model_dump_json())
        except requests.exceptions.RequestException:
            return None
        if res.ok:
            return Executor.model_validate_json(res.content)
        else:
            return None

    def get_data_location(self, name: str) -> Optional[Database]:
        try:
            res = requests.get(self.node.address.http_str(f"/database/{name}"))
        except requests.exceptions.RequestException:
            return None
        if res.ok:
            return Database.model_validate_json(res.content)
        else:
            return None

    def get_database_for_storage(self, required_storage: int) -> Optional[Database]:
        try:
            res = requests.get(self.node.address.http_str("/database"),
                               params={"required_storage", required_storage})
        except requests.exceptions.RequestException:
            return None
        if res.ok:
            return Database.model_validate_json(res.content)
        else:
            return None

    def queue_job_for_execution(self, command: Command) -> Optional[str]:
        try:
            res = requests.put(self.node.address.http_str("/job"),
                               data=command.model_dump_json())
            if res.ok:
                return res.content.decode()
            return None
        except requests.exceptions.RequestException:
            return None

    def poll_job_result(self, job_id):
        try:
            res = requests.get(self.node.address.http_str(f"/job/{job_id}"))
            if res.ok:
                return res.content.decode()
            return None
        except requests.exceptions.RequestException:
            return None

    def put_job_result_location(self, job_id: str, name: str):
        try:
            res = requests.put(self.node.address.http_str(f"/job/{job_id}"), data={"name" : name})
            return res.ok
        except requests.exceptions.RequestException:
            return False
