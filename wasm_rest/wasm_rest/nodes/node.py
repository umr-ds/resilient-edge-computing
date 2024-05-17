import socket
import threading
import time
from typing import Any, Optional, Union
from uuid import UUID

from fastapi import FastAPI
from uvicorn import Server as UVServer, Config as UVConfig
from uvicorn.server import ServerState
from zeroconf import Zeroconf, ServiceInfo, ServiceListener

import wasm_rest.nodetypes.node
from wasm_rest.model import Address
from wasm_rest.util.log import LOG
from wasm_rest.util.util import generate_unique_id


class Node:
    fastapi_app: FastAPI
    uvicorn_server: Optional[UVServer]
    zeroconf: Zeroconf
    addresses: list[str] = []
    port: int
    id: UUID
    service_type: str
    __listeners: list[(str, ServiceListener)] = []
    __listen_lock = threading.Lock()

    def __init__(self, host: Union[str, list[str]], port: int, service_type: Optional[str] = None, fastapi_app: Optional[FastAPI] = None,
                 uvicorn_args: dict[str, Any] = None) -> None:
        self.service_type = service_type
        if uvicorn_args is None:
            uvicorn_args = {}
        self.fastapi_app = fastapi_app
        uvicorn_args["app"] = self.fastapi_app
        if type(host) is str:
            uvicorn_args["host"] = host
            self.addresses.append(host)
        elif type(host) is list:
            uvicorn_args["host"] = "0.0.0.0"
            self.addresses.extend(host)
        uvicorn_args["port"] = port
        self.port = port
        if fastapi_app is None:
            self.uvicorn_server = None
        else:
            @fastapi_app.get("/ping")
            def ping() -> str:
                return Node.zeroconf_service_name(self.service_type, self.id)
            config = UVConfig(**uvicorn_args)
            self.uvicorn_server = UVServer(config)
        self.zeroconf = Zeroconf()
        self.id = generate_unique_id()

    def generate_service_info(self) -> ServiceInfo:
        return ServiceInfo(
            Node.zeroconf_service_type(self.service_type),
            Node.zeroconf_service_name(self.service_type, self.id),
            addresses=[socket.inet_aton(address) for address in self.addresses],
            port=self.port,
            server=Node.zeroconf_service_name(self.service_type, self.id))

    def add_service_listener(self, _type: str, listener: ServiceListener):
        if self.uvicorn_server.started:
            self.zeroconf.add_service_listener(_type, listener)
        with self.__listen_lock:
            self.__listeners.append((_type, listener))

    def run(self) -> None:
        LOG.debug(f"starting {self.service_type}: {self.id}")
        if self.uvicorn_server is not None:
            threading.Thread(target=self.after_start, daemon=True, name="after_start").start()
            self.uvicorn_server.run()
        self.zeroconf.remove_all_service_listeners()
        if self.service_type is not None:
            self.zeroconf.unregister_service(self.generate_service_info())

    def start(self):
        threading.Thread(target=self.run, name=f"Node {self.id}").start()

    def stop(self):
        self.uvicorn_server.should_exit = True

    def after_start(self):
        for _ in range(10):
            if self.uvicorn_server.started:
                if self.service_type is not None:
                    self.zeroconf.register_service(self.generate_service_info())
                with self.__listen_lock:
                    for _type, listener in self.__listeners:
                        self.zeroconf.add_service_listener(_type, listener)
                break
            time.sleep(10)

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
