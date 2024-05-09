from enum import Enum
from typing import Union

from pydantic import BaseModel


class Address(BaseModel):
    host: str = ""
    port: int = 0


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


class JobInfo(BaseModel):
    wasm_bin: Union[str, tuple[str, str]]  # name of bin
    wasm_bin_is_named: bool = False
    stdin: Union[str, tuple[str, str]] = ("", "")  # name, path
    stdin_is_named: bool = False
    job_data: dict[str, str] = {}
    named_data: dict[str, str] = {}
    directories: list[str] = []
    args: list[str] = []
    env: dict[str, str] = {}
    zip_results: dict[str, str] = {"/": "/"}
    named_results: dict[str, str] = {}
    capabilities: Capabilities = Capabilities()
    result_addr: Address = Address()
    delete: bool = True


class Execution(BaseModel):
    cmd: str
    wait: set[str] = set()


class ExecutionPlan(BaseModel):
    exec: list[Execution]
    cmds: dict[str, JobInfo]
    named_data: dict[str, str] = []


class NodeRole(Enum):
    EXIT = 0,
    BROKER = 1,
    EXECUTOR = 2,
    DATASTORE = 3,
    CLIENT = 4,
    AUTO = 5
