import json
from typing import Optional

import requests

from wasm_rest.model import JobInfo, Capabilities
from wasm_rest.nodetypes.datastore import Datastore
from wasm_rest.nodetypes.executor import Executor
from wasm_rest.nodetypes.node import Node


class Broker(Node):

    def register_executor(self, executor: Executor) -> bool:
        res = self.put("/executors/register", data=executor.model_dump_json())
        return res is not None and res.ok

    def heartbeat_executor(self, exec_id: str, capabilities: Capabilities) -> bool:  # still connected
        res = self.put(f"/executors/heartbeat/{exec_id}", data=capabilities.model_dump_json())
        return res is not None and res.ok

    def executor_count(self) -> int:
        res = self.get("/executors/count")
        if res is None:
            return 0
        if res.ok:
            return int(res.content)

    def data_location(self, name: str, job_id: str = '', invalidate: bool = False) -> Optional[Datastore]:
        try:
            res = self.get(f"/datastore/{name}", params={"job_id": job_id, "invalidate": invalidate})
        except requests.exceptions.RequestException:
            return None
        if res is not None and res.ok:
            return Datastore.model_validate_json(res.content)
        else:
            return None

    def datastore_for_storage(self, required_storage: int) -> Optional[Datastore]:
        try:
            res = self.get("/datastore", params={"required_storage": required_storage})
        except requests.exceptions.RequestException:
            return None
        if res is not None and res.ok:
            return Datastore.model_validate_json(res.content)
        else:
            return None

    def submit_job(self, job_info: JobInfo, job_id: str) -> Optional[str]:
        try:
            res = self.put(f"/job/submit/{job_id}", data=job_info.model_dump_json())
            if res is not None and res.ok:
                return json.loads(res.content.decode())
            return None
        except requests.exceptions.RequestException:
            return None
