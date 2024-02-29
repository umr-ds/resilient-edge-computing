#! /usr/bin/env python3
import json
import random
import socket
import threading
from typing import cast

import requests
import uvicorn
from fastapi import FastAPI, HTTPException
from zeroconf import Zeroconf, ServiceInfo, ServiceBrowser, ServiceListener

from wasm_rest.model import Executor, Server, NodeRole, Capabilities, Database, Command
from wasm_rest.util import wait_online, gen_node_id

app = FastAPI()
executors: dict[str, Executor] = {}
databases: dict[str, Database] = {}
job_queue: list[Command]
results: dict[str, str] = {}
zeroconf = Zeroconf()
node_id = gen_node_id()
address: Server = None
service_browser: ServiceBrowser


def is_server(server: Server) -> bool:
    try:
        return requests.get(f"http://{server.host}:{server.port}/caps").ok
    except requests.exceptions.RequestException:
        return False


def gen_service_info() -> ServiceInfo:
    return ServiceInfo(
        "_broker_wasm-rest._tcp.local.",
        f"_broker{node_id}._wasm-rest._tcp.local.",
        addresses=[socket.inet_aton(address.host)],
        # socket.inet_aton(socket.gethostbyname(host))
        port=address.port,
        server=f"_broker{node_id}._wasm-rest._tcp.local.",
        properties={"execs": len(executors).to_bytes(1, "big"), "node_id": node_id}
    )


@app.put("/register")
def register_executor(executor: Executor) -> None:
    if is_server(executor):
        executors[executor.node_id] = executor
        info = gen_service_info()
        zeroconf.update_service(info)
    else:
        raise HTTPException(400, "Could not request Capabilities")


@app.get("/executor")
def get_executor(caps: Capabilities) -> Executor:
    executor = select_executor(list(executors.values()), caps)
    if executor is None:
        raise HTTPException(503, "No capable executor")
    return executor


@app.get("/database/{name}")
def get_data_location(name: str) -> Server:
    for database in databases.values():
        if name in database.data_names:
            return Server(host=database.host, port=database.port)


@app.get("/database")
def get_database_for_storage(required_storage: int) -> Server:
    cap_databases = [database for database in databases.values() if database.free_storage > required_storage]
    if len(cap_databases) != 0:
        database = random.choice(cap_databases)
        return Server(host=database.host, port=database.port)
    else:
        raise HTTPException(503, "No database able to hold file")


@app.put("/job")
def queue_job_for_execution(command: Command) -> bool:
    job_queue.append(command)


@app.put("/suspect/{node_type}")
def suspect_node_down(server: Server, node_type: str):
    if node_type == "database":
        pass
    elif node_type == "executor":
        pass


def try_start_queued_jobs():
    for job in job_queue:
        cap_execs = [executor for executor in executors.values() if executor.cur_caps.is_capable(job.capabilities)]
        if len(cap_execs) == 0:
            continue
        executor = random.choice(cap_execs)
        executor


def select_executor(execs: list[Executor], caps: Capabilities):
    capable_executors = [executor for executor in execs if executor.cur_caps.is_capable(caps)]
    if len(capable_executors) != 0:
        return random.choice(capable_executors)
    else:
        return None


def register_as_service(host: str, port: int, info: ServiceInfo):
    wait_online(Server(host=host, port=port), "register", 16, 2)
    zeroconf.register_service(info)


class ExecutorListener(ServiceListener):

    def update_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        pass

    def remove_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        info = zeroconf.get_service_info(type_, name)
        if info:
            if info.decoded_properties["node_id"] in executors.keys():
                del executors[info.decoded_properties["node_id"]]

    def add_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        info = zeroconf.get_service_info(type_, name)
        if info:
            executor = Executor.model_validate_json(info.decoded_properties["exec"])
            if executor.node_id in executors.keys():
                executors[node_id] = executor
                print(executors)


class DatabaseListener(ServiceListener):

    def update_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        pass

    def remove_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        del databases[name]

    def add_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        info = zeroconf.get_service_info(type_, name)
        if info:
            database_id = info.decoded_properties["node_id"]
            addresses = info.parsed_scoped_addresses()
            databases[name] = Database(node_id=database_id, host=addresses[0], port=cast(int, info.port),
                                       data_names=set(json.loads(info.decoded_properties["data"])),
                                       free_storage=info.decoded_properties["free_storage"])


def start(host: str, port: int) -> NodeRole:
    global address, service_browser
    address = Server(host=host, port=port)
    service_info = gen_service_info()
    threading.Thread(target=register_as_service, kwargs={"host": host, "port": port, "info": service_info}).start()
    zeroconf.add_service_listener("_database_wasm-rest._tcp.local.", DatabaseListener())
    zeroconf.add_service_listener("_executor_wasm-rest._tcp.local.", ExecutorListener())
    uvicorn.run(app, host=host, port=port)

    zeroconf.unregister_service(service_info)
    return NodeRole.EXIT
