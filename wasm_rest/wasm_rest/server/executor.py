import time
from typing import Optional

from pydantic import BaseModel

from ..model import Capabilities, Node, Command

import requests


class Executor(BaseModel):
    node: Node
    cur_caps: Capabilities = Capabilities()  # cache
    last_update: float = 0

    def update_capabilities(self) -> Optional[Capabilities]:
        try:
            res = requests.get(self.node.address.http_str("/caps"))
            if not res.ok:
                return None
            self.cur_caps = Capabilities.model_validate_json(res.content)
            self.last_update = time.time()
            return self.cur_caps
        except requests.exceptions.RequestException:
            return None

    def submit_stored_job(self, command: Command) -> Optional[str]:
        try:
            res = requests.put(self.node.address.http_str("/submit-stored"), data=command.model_dump_json())
            if res.ok:
                return res.content.decode()
            return None
        except requests.exceptions.RequestException:
            return None
