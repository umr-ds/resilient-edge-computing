from abc import ABC, abstractmethod
from enum import IntEnum


class NodeType(IntEnum):
    BROKER = 1
    EXECUTOR = 2
    DATASTORE = 3
    CLIENT = 4


class Node(ABC):
    dtn_id: str
    dtn_agent_socket: str

    @abstractmethod
    async def run(self) -> None:
        pass
