import json
from typing import IO

import requests
from pydantic import BaseModel

from wasm_rest.model import Node


class Database(BaseModel):
    node: Node
    data_names: set[str] = set()
    free_storage: int = 0

    def store_data(self, file: IO[bytes], name: str) -> bool:
        try:
            res = requests.put(self.node.address.http_str(f"/data/{name}"),
                               files={"data": file})
        except requests.exceptions.RequestException:
            return False
        return res.ok

    def get_data(self, file: IO[bytes], name: str) -> bool:
        try:
            res = requests.get(self.node.address.http_str(f"/data/{name}"), stream=True)
            if res.status_code == 200:
                for chunk in res.iter_content(chunk_size=65536):
                    file.write(chunk)
                return True
        except requests.exceptions.RequestException:
            return False
        return False

    def delete_data(self, name: str) -> bool:
        try:
            res = requests.delete(self.node.address.http_str(f"/data/{name}"))
        except requests.exceptions.RequestException:
            return False
        return res.ok

    def get_data_list(self) -> set[str]:
        try:
            res = requests.get(self.node.address.http_str("/list"))
        except requests.exceptions.RequestException:
            return set()
        if res.ok:
            self.data_names = set(json.loads(res.content))
            return self.data_names
        return set()

    def free_space(self) -> int:
        try:
            res = requests.delete(self.node.address.http_str("/free"))
        except requests.exceptions.RequestException:
            return 0
        if res.ok:
            self.free_storage = int(res.content)
            return self.free_storage
        return 0
