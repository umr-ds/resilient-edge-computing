import socket
import threading
import time
from typing import Any, Optional
from uuid import UUID, uuid4

from fastapi import FastAPI
from uvicorn import Config as UVConfig
from uvicorn import Server as UVServer
from zeroconf import ServiceInfo, ServiceListener, Zeroconf

from rec.util.log import LOG


class Node:
    fastapi_app: FastAPI
    uvicorn_server: UVServer
    zeroconf: Zeroconf
    addresses: list[str]
    port: int
    id: UUID
    service_type: str
    should_stop: bool = False
    __zeroconf_listeners: list[tuple[str, ServiceListener]]
    __zeroconf_listen_lock: threading.Lock

    def __init__(
        self,
        host: list[str],
        port: int,
        service_type: Optional[str] = None,
        uvicorn_args: dict[str, Any] = None,
    ) -> None:
        self.service_type = service_type
        if uvicorn_args is None:
            uvicorn_args = {}
        self.fastapi_app = FastAPI()
        self.add_endpoints()
        uvicorn_args["app"] = self.fastapi_app
        self.addresses = []
        if len(host) == 1:
            uvicorn_args["host"] = host[0]
        elif type(host) is list:
            uvicorn_args["host"] = "0.0.0.0"
        self.addresses.extend(host)
        uvicorn_args["port"] = port
        self.port = port

        @self.fastapi_app.get("/ping")
        def ping() -> str:
            return Node.zeroconf_service_name(self.service_type, self.id)

        config = UVConfig(**uvicorn_args)
        self.uvicorn_server = UVServer(config)
        self.zeroconf = Zeroconf()
        self.id = uuid4()
        self.__zeroconf_listeners = []
        self.__zeroconf_listen_lock = threading.Lock()

    def add_endpoints(self):
        pass

    def generate_service_info(self) -> ServiceInfo:
        return ServiceInfo(
            Node.zeroconf_service_type(self.service_type),
            Node.zeroconf_service_name(self.service_type, self.id),
            addresses=[socket.inet_aton(address) for address in self.addresses],
            port=self.port,
            server=Node.zeroconf_service_name(self.service_type, self.id),
        )

    def add_service_listener(self, _type: str, listener: ServiceListener):
        if self.uvicorn_server.started:
            self.zeroconf.add_service_listener(_type, listener)
        with self.__zeroconf_listen_lock:
            self.__zeroconf_listeners.append((_type, listener))

    def do_run(self) -> None:
        LOG.debug(f"starting {self.service_type}: {self.id}")
        threading.Thread(
            target=self.after_start, daemon=True, name="after_start"
        ).start()
        self.uvicorn_server.run()
        self.stop()
        self.zeroconf.remove_all_service_listeners()
        if self.service_type is not None:
            self.zeroconf.unregister_service(self.generate_service_info())

    def start(self):
        threading.Thread(target=self.do_run, name=f"Node {self.id}").start()

    def stop(self):
        self.uvicorn_server.should_exit = True
        self.should_stop = True

    def after_start(self):
        for _ in range(10):
            if self.uvicorn_server.started:
                if self.service_type is not None:
                    self.zeroconf.register_service(self.generate_service_info())
                with self.__zeroconf_listen_lock:
                    for _type, listener in self.__zeroconf_listeners:
                        self.zeroconf.add_service_listener(_type, listener)
                break
            time.sleep(10)
        while not self.should_stop:
            self.zeroconf.send(
                self.zeroconf.generate_service_broadcast(
                    self.generate_service_info(), None
                )
            )
            time.sleep(10)

    @classmethod
    def zeroconf_service_type(cls, service_type: str) -> str:
        return f"_{service_type}._urban-cp._tcp.local."

    @classmethod
    def zeroconf_service_name(cls, service_type: str, node_id: UUID) -> str:
        return f"_{service_type}{node_id}._urban-cp._tcp.local."

    @classmethod
    def id_from_name(cls, name: str) -> UUID:
        return UUID(name[-58:-22])


if __name__ == "__main__":
    print(
        Node.id_from_name(
            Node(["127.0.0.1"], 8000, "test", None).generate_service_info().name
        )
    )
