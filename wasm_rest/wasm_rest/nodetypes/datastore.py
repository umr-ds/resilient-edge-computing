import json
from typing import IO

import requests
from fastapi_pagination import Page

from wasm_rest.nodetypes.node import Node


class Datastore(Node):

    def store_data(self, file: IO[bytes], name: str) -> bool:
        res = self.put(f"/data/{name}", files={"data": file})
        if res is None:
            return False
        return res.ok

    def get_data(self, file: IO[bytes], name: str) -> bool:
        res = self.get(f"/data/{name}", stream=True)
        if res is None:
            return False
        if res.status_code == 200:
            try:
                for chunk in res.iter_content(chunk_size=65536):
                    file.write(chunk)
            except requests.RequestException:
                return False
            return True
        return False

    def delete_data(self, name: str) -> bool:
        res = self.delete(f"/data{name}")
        if res is None:
            return False
        return res.ok

    def get_data_list(self) -> list[str]:
        res = self.get("/list")
        if res is None:
            return []
        if res.ok:
            return json.loads(res.content)
        return []

    def paginate_data_list(self, name: str = '', job_id: str = '', page_number: int = 1, page_size: int = 50) -> Page:
        params = {"page": page_number, "size": page_size}
        if job_id != '':
            params["job_id"] = job_id
        res = self.get(f"/list/{name}", params=params)
        if res is None:
            return Page()
        if res.ok:
            return Page.model_validate_json(res.content)
        return Page()

    def free_space(self) -> int:
        res = self.get("/free")
        if res is None:
            return 0
        if res.ok:
            return int(res.content)
        return 0
