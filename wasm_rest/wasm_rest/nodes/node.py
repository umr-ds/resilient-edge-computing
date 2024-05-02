import socket
from typing import Any, Optional
from uuid import UUID

from fastapi import FastAPI
from uvicorn import Server as UVServer, Config as UVConfig
from zeroconf import Zeroconf, ServiceInfo

from wasm_rest.model import Address
from wasm_rest.util.util import generate_unique_id


class Node:
    fastapi_app: FastAPI
    uvicorn_server: Optional[UVServer]
    zeroconf: Zeroconf
    address: Address
    id: UUID
    service_type: str

    def __init__(self, host: str, port: int, service_type: Optional[str] = None, fastapi_app: Optional[FastAPI] = None,
                 uvicorn_args: dict[str, Any] = None) -> None:
        self.service_type = service_type
        if uvicorn_args is None:
            uvicorn_args = {}
        self.fastapi_app = fastapi_app
        uvicorn_args["app"] = self.fastapi_app
        uvicorn_args["host"] = host
        uvicorn_args["port"] = port
        if fastapi_app is None:
            self.uvicorn_server = None
        else:
            config = UVConfig(**uvicorn_args)
            self.uvicorn_server = UVServer(config)
        self.zeroconf = Zeroconf()
        self.address = Address(host=host, port=port)
        self.id = generate_unique_id()

    def generate_service_info(self) -> ServiceInfo:
        return ServiceInfo(
            Node.zeroconf_service_type(self.service_type),
            Node.zeroconf_service_name(self.service_type, self.id),
            addresses=[socket.inet_aton(self.address.host)],
            port=self.address.port,
            server=Node.zeroconf_service_name(self.service_type, self.id))

    def run(self) -> None:
        if self.service_type is not None:
            self.zeroconf.register_service(self.generate_service_info())
        if self.uvicorn_server is not None:
            self.uvicorn_server.run()
        if self.service_type is not None:
            self.zeroconf.unregister_service(self.generate_service_info())

    def stop(self):
        self.uvicorn_server.should_exit = True

    @classmethod
    def zeroconf_service_type(cls, service_type: str) -> str:
        return f"_{service_type}._wasm-rest._tcp.local."

    @classmethod
    def zeroconf_service_name(cls, service_type: str, node_id: UUID) -> str:
        return f"_{service_type}{node_id}._wasm-rest._tcp.local."

    @classmethod
    def id_from_name(cls, name: str) -> UUID:
        return UUID(name[-59:-23])


if __name__ == '__main__':
    print(Node.id_from_name(Node("127.0.0.1", 8000, "test", None).generate_service_info().name))
