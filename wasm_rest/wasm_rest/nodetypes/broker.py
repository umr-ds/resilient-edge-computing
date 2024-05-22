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
            return 0
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

    def send_result(self, job_id: UUID, data: IO[bytes]):
        res = self.put(f"/result/{job_id}", files={"data": data})
        return res is not None and res.ok

    def submit_job(self, job_info: JobInfo, job_id: UUID) -> Optional[UUID]:
        try:
            res = self.put(f"/job/submit/{job_id}", data=job_info.model_dump_json())
            if res is not None and res.ok:
                return UUID(json.loads(res.content.decode()))
            return None
        except requests.exceptions.RequestException:
            return None

    def delete_job(self, job_id: UUID) -> bool:
        res = self.delete(f"/job/{job_id}")
        return res is not None and res.ok
