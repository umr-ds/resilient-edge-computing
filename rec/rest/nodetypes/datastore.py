import json
from typing import IO, Iterator, Optional
from uuid import UUID

import requests
from fastapi_pagination import Page

from rec.rest.nodetypes.node import Node


class Datastore(Node):

    def store_data(self, file: IO[bytes], name: str) -> bool:
        res = self.put(f"/data/{name}", files={"data": file})
        return res is not None and res.ok

    def get_data(self, file: IO[bytes], name: str) -> bool:
        try:
            it = self.get_data_iterator(name)
            if it:
                for chunk in it:
                    file.write(chunk)
        except requests.RequestException:
            return False
        return True

    def get_data_iterator(self, name: str) -> Optional[Iterator]:
        res = self.get(f"/data/{name}", stream=True)
        if res is not None and res.status_code == 200:
            return res.iter_content(65536)
        return None

    def delete_data(self, name: str) -> bool:
        res = self.delete(f"/data{name}")
        return res is not None and res.ok

    def delete_job_data(self, job_id: UUID) -> bool:
        res = self.delete(f"/job_data/{job_id}")
        return res is not None and res.ok

    def get_data_list(self) -> list[str]:
        res = self.get("/list")
        if res is None:
            return []
        if res.ok:
            return json.loads(res.content)
        return []

    def paginate_data_list(
        self,
        name: str = "",
        job_id: Optional[UUID] = None,
        page_number: int = 1,
        page_size: int = 50,
    ) -> Page:
        params: dict = {"page": page_number, "size": page_size}
        if job_id:
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
            return -1
        if res.ok:
            return int(res.content)
        return 0
