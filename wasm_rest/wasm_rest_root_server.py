#! /usr/bin/env python3

import base64
import random
import socket
import threading

import requests
import uvicorn
from fastapi import FastAPI, HTTPException
from zeroconf import Zeroconf, ServiceInfo

from wasm_rest.model import Executor, Server, NodeRole, Capabilities
from wasm_rest.util import wait_online

app = FastAPI()
executors: set[str] = set()
zeroconf = Zeroconf()


def is_server(server: Server) -> bool:
    try:
        return requests.get(f"http://{server.host}:{server.port}/caps").ok
    except requests.exceptions.RequestException:
        return False


@app.put("/register")
def register_executor(executor: Executor):
    if is_server(executor):
        executors.add(executor.model_dump_json())
    else:
        raise HTTPException(400, "Could not request Capabilities")


@app.get("/executor")
def get_executor(caps: Capabilities) -> Executor:
    rem = set()
    online = []
    for executor in executors:
        exec_obj = Executor.model_validate_json(executor)
        if not is_server(exec_obj):
            rem.add(executor)
        elif exec_obj.max_caps.is_capable(caps):
            online.append(exec_obj)
    executors.difference_update(rem)
    return select_executor(online)


def select_executor(execs: list[Executor]):
    return execs[0]


def gen_node_id() -> str:
    return base64.urlsafe_b64encode(random.randbytes(32)).decode()[:-1]


def register_as_service(host: str, port: int, info: ServiceInfo):
    wait_online(Server(host=host, port=port), "register", 16, 2)
    zeroconf.register_service(info)


def start(host: str, port: int) -> NodeRole:
    info = ServiceInfo(
        "_broker._tcp.local.",
        f"_broker{gen_node_id()}._broker._tcp.local.",
        addresses=[socket.inet_aton(socket.gethostbyname(socket.getfqdn()))],
        # socket.inet_aton(socket.gethostbyname(host))
        port=port,
        properties={"execs": len(executors).to_bytes(1, "big")}
    )
    threading.Thread(target=register_as_service, kwargs={"host": host, "port": port, "info": info}).start()
    # zeroconf.async_update_service()  # TODO update when exec count changes?
    uvicorn.run(app, host=host, port=port)

    zeroconf.unregister_service(info)
    return NodeRole.EXIT
