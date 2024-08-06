from typing import IO
from uuid import UUID

from urban_compute_platform.nodetypes.node import Node


class Client(Node):

    def send_result(self, job_id: UUID, data: IO[bytes]) -> bool:
        res = self.put(f"/result/{job_id}", files={"data": data})
        return res is not None and (res.ok or res.status_code == 404)
