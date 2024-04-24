#! /usr/bin/env python3
import json
import random
import socket
import threading
import time
from queue import Queue

from typing import cast, Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Form
from zeroconf import Zeroconf, ServiceInfo, ServiceListener

from wasm_rest.model import Address, NodeRole, Capabilities, Command, Node
from wasm_rest.server.broker import Broker
from wasm_rest.util import wait_online, gen_unique_id
from wasm_rest.server.executor import Executor
from wasm_rest.server.database import Database

app = FastAPI()
zeroconf = Zeroconf()

executors: dict[str, Executor] = {}
databases: dict[str, Database] = {}
job_queue: Queue[tuple[str, Command]] = Queue()
running_jobs: dict[str, tuple[str, Command]]
results: dict[str, str] = {}

self_object: Broker


def gen_service_info() -> ServiceInfo:
    self_object.execs = len(executors)
    return ServiceInfo(
        "_broker_wasm-rest._tcp.local.",
        f"_broker{self_object.node.id}._wasm-rest._tcp.local.",
        addresses=[socket.inet_aton(self_object.node.address.host)],
        # socket.inet_aton(socket.gethostbyname(host))
        port=self_object.node.address.port,
        server=f"_broker{self_object.node.id}._wasm-rest._tcp.local.",
        properties={"execs": len(executors).to_bytes(1, "big"), "node_id": self_object.node.id}
    )


@app.put("/register")
def register_executor(executor: Executor) -> None:
    if executor.update_capabilities() is not None:
        executors[executor.node.id] = executor
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
def get_data_location(name: str) -> Database:
    for database in databases.values():
        if name in database.data_names:
            return Database(node=database.node)


@app.get("/database")
def get_database_for_storage(required_storage: int) -> Database:
    cap_databases = [database for database in databases.values() if database.free_storage > required_storage]
    if len(cap_databases) != 0:
        database = random.choice(cap_databases)
        return Database(node=database.node)
    else:
        raise HTTPException(503, "No database able to hold file")


@app.put("/job")
def queue_job_for_execution(command: Command) -> str:
    broker_side_job_id = gen_unique_id()
    job_queue.put((broker_side_job_id, command))
    return broker_side_job_id


@app.get("/job/{job_id}")
def poll_job_result(job_id: str):
    if job_id in results.keys():
        return results[job_id]
    raise HTTPException(503, "Job has not been executed yet")


@app.put("/job/{job_id}")
def put_job_result_location(job_id: str, name: str = Form()) -> bool:
    if job_id not in running_jobs.keys():
        raise HTTPException(400, "No such Job was running")
    if job_id in results.keys():
        raise HTTPException(409, "Result path already set")
    results[job_id] = name
    return True


@app.put("/suspect/{node_type}")
def suspect_node_down(server: Address, node_type: str):
    if node_type == "database":
        pass
    elif node_type == "executor":
        pass


def try_start_queued_jobs():
    while True:
        broker_side_id, job = job_queue.get()
        cap_execs = [executor for executor in executors.values() if executor.cur_caps.is_capable(job.capabilities)]
        if len(cap_execs) == 0:
            job_queue.put((broker_side_id, job))
            continue
        executor = random.choice(cap_execs)
        job_id = executor.submit_stored_job(job)
        if job_id is None:
            job_queue.put((broker_side_id, job))
            continue
        running_jobs[broker_side_id] = (job_id, job)
        time.sleep(60)


def select_executor(execs: list[Executor], caps: Capabilities) -> Optional[Executor]:
    capable_executors = [executor for executor in execs if executor.cur_caps.is_capable(caps)]
    if len(capable_executors) != 0:
        return random.choice(capable_executors)
    else:
        return None


def register_as_service(host: str, port: int, info: ServiceInfo) -> None:
    wait_online(Address(host=host, port=port), "register", 16, 2)
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
            if executor.node.id in executors.keys():
                executors[executor.node.id] = executor
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
            databases[name] = Database(node=Node(id=database_id,
                                                 address=Address(host=addresses[0], port=cast(int, info.port))),
                                       data_names=set(json.loads(info.decoded_properties["data"])),
                                       free_storage=info.decoded_properties["free_storage"])


def start(host: str, port: int) -> NodeRole:
    global self_object
    self_object = Broker(node=Node(id=gen_unique_id(), address=Address(host=host, port=port)), execs=0)
    service_info = gen_service_info()
    threading.Thread(target=register_as_service, kwargs={"host": host, "port": port, "info": service_info}).start()
    threading.Thread(target=try_start_queued_jobs).start()
    zeroconf.add_service_listener("_database_wasm-rest._tcp.local.", DatabaseListener())
    zeroconf.add_service_listener("_executor_wasm-rest._tcp.local.", ExecutorListener())
    uvicorn.run(app, host=host, port=port)

    zeroconf.unregister_service(service_info)
    return NodeRole.EXIT
