import json
from typing import Optional
from uuid import UUID

import requests
from pydantic import BaseModel

from rec.rest.model import Address


class Node(BaseModel):
    address: Address
    id: UUID

    def get(self, path: str, **kwargs) -> Optional[requests.Response]:
        try:
            return requests.get(
                f"http://{self.address.host}:{self.address.port}{path}", **kwargs
            )
        # except requests.exceptions.ConnectTimeout:
        #    raise ConnectionTimeoutException("Could not reach Server")
        except requests.exceptions.RequestException:
            return None

    def put(self, path: str, **kwargs) -> Optional[requests.Response]:
        try:
            return requests.put(
                f"http://{self.address.host}:{self.address.port}{path}", **kwargs
            )
        except requests.exceptions.RequestException:
            return None

    def delete(self, path: str, **kwargs) -> Optional[requests.Response]:
        try:
            return requests.delete(
                f"http://{self.address.host}:{self.address.port}{path}", **kwargs
            )
        except requests.exceptions.RequestException:
            return None

    def ping(self, timeout_s: int = 10) -> Optional[str]:
        res = self.get("/ping", timeout=timeout_s)
        if res is not None and res.ok:
            return json.loads(res.content)
        return None
