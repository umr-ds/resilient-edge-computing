from typing import IO

from wasm_rest.nodetypes.node import Node


class Client(Node):

    def send_result(self, job_id: str, data: IO[bytes]) -> bool:
        res = self.put(f"/result/{job_id}", files={"data": data})
        return res is not None and (res.ok or res.status_code == 404)
