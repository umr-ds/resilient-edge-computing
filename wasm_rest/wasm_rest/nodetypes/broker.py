import json
from typing import Optional, IO
from uuid import UUID

import requests

from wasm_rest.model import JobInfo, Capabilities
from wasm_rest.nodetypes.executor import Executor
from wasm_rest.nodetypes.node import Node


class Broker(Node):

    def register_executor(self, hosts: list[str], executor: Executor) -> bool:
        data = f"{{\"hosts\": {json.dumps(hosts)}, \"executor\": {executor.model_dump_json()}}}"
        res = self.put("/executors/register", data=data)
        return res is not None and res.ok

    def heartbeat_executor(self, exec_id: UUID, capabilities: Capabilities) -> bool:  # still connected
        res = self.put(f"/executors/heartbeat/{exec_id}", data=capabilities.model_dump_json())
        return res is not None and res.ok

    def executor_count(self) -> int:
        res = self.get("/executors/count")
        if res is None:
            return -1
        if res.ok:
            return int(res.content)

    def store_data(self, file: IO[bytes], name: str) -> bool:
        res = self.put(f"/data/{name}", files={"data": file})
        return res is not None and res.ok

    def get_data(self, file: IO[bytes], name: str, job_id: Optional[UUID] = None) -> bool:
        params = {}
        if job_id:
            params["job_id"] = job_id
        res = self.get(f"/data/{name}", params={}, stream=True)
        if res:
            try:
                for chunk in res.iter_content(65536):
                    file.write(chunk)
            except requests.RequestException:
                return False
            return True
        return False

    def get_data_glob(self, name: str) -> list[str]:
        res = self.get(f"/list/data/{name}")
        if res is not None and res.ok:
            return json.loads(res.content.decode())
        return []

    def send_result(self, job_id: UUID, data: IO[bytes]) -> bool:
        res = self.put(f"/result/{job_id}", files={"data": data})
        return res is not None and res.ok

    def submit_job(self, job_info: JobInfo, job_id: UUID, wait_for: Optional[set[UUID]] = None) -> Optional[UUID]:
        try:
            data = f"{{\"job_info\": {job_info.model_dump_json()}, \"wait_for\": {json.dumps([str(it) for it in wait_for])}}}"
            res = self.put(f"/job/submit/{job_id}", data=data)
            if res is not None and res.ok:
                return UUID(json.loads(res.content.decode()))
            return None
        except requests.exceptions.RequestException:
            return None

    def job_done(self, job_id: UUID) -> bool:
        res = self.put(f"/job/done/{job_id}")
        return res is not None and res.ok

    def delete_job(self, job_id: UUID) -> bool:
        res = self.delete(f"/job/{job_id}")
        return res is not None and res.ok


class BrokerProxy(Broker):
    broker: Broker

    def __init__(self, broker: Broker):
        super().__init__(address=broker.address, id=broker.id)
        self.broker = broker

    def set_broker(self, broker: Broker):
        self.address = broker.address
        self.id = broker.id
        self.broker = broker

    def register_executor(self, hosts: list[str], executor: Executor) -> bool:
        return self.broker.register_executor(hosts, executor)

    def heartbeat_executor(self, exec_id: UUID, capabilities: Capabilities) -> bool:  # still connected
        return self.broker.heartbeat_executor(exec_id, capabilities)

    def executor_count(self) -> int:
        return self.broker.executor_count()

    def store_data(self, file: IO[bytes], name: str) -> bool:
        return self.broker.store_data(file, name)

    def get_data(self, file: IO[bytes], name: str, job_id: Optional[UUID] = None) -> bool:
        return self.broker.get_data(file, name, job_id)

    def get_data_glob(self, name: str) -> list[str]:
        return self.broker.get_data_glob(name)

    def send_result(self, job_id: UUID, data: IO[bytes]) -> bool:
        return self.broker.send_result(job_id, data)

    def submit_job(self, job_info: JobInfo, job_id: UUID, wait_for: Optional[set[UUID]] = None) -> Optional[UUID]:
        return self.broker.submit_job(job_info, job_id, wait_for)

    def job_done(self, job_id: UUID) -> bool:
        return self.broker.job_done(job_id)

    def delete_job(self, job_id: UUID) -> bool:
        return self.broker.delete_job(job_id)
