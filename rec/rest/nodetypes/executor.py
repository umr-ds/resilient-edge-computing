import json
import time
from typing import Optional
from uuid import UUID

from rec.rest.model import Capabilities, JobInfo
from rec.rest.nodetypes.node import Node


class Executor(Node):
    cur_caps: Capabilities = Capabilities()  # cache
    last_update: float = 0

    def update_capabilities(self) -> Optional[Capabilities]:
        res = self.get("/capabilities")
        if res is None or not res.ok:
            return None
        self.cur_caps = Capabilities.model_validate_json(res.content)
        self.last_update = time.time()
        return self.cur_caps

    def submit_job(self, job_id: UUID, job_info: JobInfo) -> bool:
        res = self.put(f"/submit/{job_id}", data=job_info.model_dump_json())
        return res is not None and res.ok

    def job_list(self) -> list[str]:
        res = self.get("/job/list")
        if res is not None and res.ok:
            return json.loads(res.content)
        return []

    def job_delete(self, job_id: UUID) -> bool:
        res = self.delete(f"/job/{job_id}")
        return res is not None and res.ok
