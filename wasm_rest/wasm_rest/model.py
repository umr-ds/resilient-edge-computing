from enum import Enum

from pydantic import BaseModel


class Address(BaseModel):
    host: str = ""
    port: int = 0

    def http_str(self, path: str):
        return f"http://{self.host}:{self.port}{path}"


class Node(BaseModel):
    address: Address
    id: str


class RunRequest(BaseModel):
    job_id: str = ""
    stdin_file: str = ""
    args: list[str] = []
    env: dict[str, str] = {}
    results: dict[str, str] = {"/": "/"}
    res_destination: Address = Address(host="", port=0)


class Capabilities(BaseModel):
    memory: int = 0
    disk: int = 0
    cpu_load: float = 100
    cpu_cores: int = 1
    cpu_freq: float = 0
    has_battery: bool = True
    power: float = 0

    def is_capable(self, caps: 'Capabilities') -> bool:
        res = caps.memory <= self.memory and caps.disk <= self.disk
        res = res and caps.cpu_load >= self.cpu_load
        res = res and caps.cpu_cores <= self.cpu_cores
        res = res and caps.cpu_freq <= self.cpu_freq
        res = False if (not caps.has_battery) and self.has_battery else res
        return res and (caps.power <= self.power if self.has_battery else True)


class Command(BaseModel):
    wasm_bin: str
    stdin: dict[
        str, str] = {}  # local means take file at specified path, which already exists (e.g. uploaded through data field)
    data: dict[str, str] = {}  # result%x:p means take file p from result of exec x
    args: list[str] = []
    env: dict[str, str] = {}
    results: dict[str, str] = {"/": "/"}
    capabilities: Capabilities = Capabilities()
    result_path: str = "."
    result_addr: Address = Address(host="", port=8003)


class NodeRole(Enum):
    EXIT = 0,
    BROKER = 1,
    EXECUTOR = 2


class JobStatus(Enum):
    INIT = 0
    RUNNING = 1
    DONE = 2
    ERROR = 3
