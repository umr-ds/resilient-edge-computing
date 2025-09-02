from abc import ABC, abstractmethod


class Node(ABC):
    dtn_id: str
    dtn_agent_socket: str

    @abstractmethod
    def run(self) -> None:
        pass
