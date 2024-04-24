from typing import Optional

import requests
from pydantic import BaseModel

from wasm_rest.model import Address


class Node(BaseModel):
    address: Address
    id: str

    def get(self, path: str, **kwargs) -> Optional[requests.Response]:
        try:
            return requests.get(f"http://{self.address.host}:{self.address.port}{path}", **kwargs)
        except requests.exceptions.RequestException:
            return None

    def put(self, path: str, **kwargs) -> Optional[requests.Response]:
        try:
            return requests.put(f"http://{self.address.host}:{self.address.port}{path}", **kwargs)
        except requests.exceptions.RequestException:
            return None

    def delete(self, path: str, **kwargs) -> Optional[requests.Response]:
        try:
            return requests.delete(f"http://{self.address.host}:{self.address.port}{path}", **kwargs)
        except requests.exceptions.RequestException:
            return None
