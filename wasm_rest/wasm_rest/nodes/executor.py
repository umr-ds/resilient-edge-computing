import argparse
import os
import random
import sched
import threading
import time
from contextlib import asynccontextmanager
from io import BytesIO
from typing import Any

import psutil
from fastapi import FastAPI, HTTPException

from wasm_rest.exceptions import WasmRestException
from wasm_rest.model import JobInfo, NodeRole, Capabilities
from wasm_rest.nodes.executors.job import Job
from wasm_rest.nodes.listeners.brokers import BrokerListener
from wasm_rest.nodes.node import Node
from wasm_rest.nodetypes.broker import Broker
from wasm_rest.nodetypes.executor import Executor


@asynccontextmanager
async def lifespan(_: FastAPI):
    threading.Thread(target=register_with_broker, daemon=True, name="register").start()
    yield


heartbeat_scheduler = sched.scheduler()
fastapi_app = FastAPI(lifespan=lifespan)
node_object: Node
self_object: Executor

broker_listener = BrokerListener()
broker: Broker

jobs_lock = threading.Lock()
jobs: dict[str, Job] = {}
root_dir: str = ""


def register_with_broker() -> None:
    global broker
    registered = False
    waited = 0
    while not registered:
        broker = select_broker()
        if broker is None:
            if waited == 10:
                raise SystemExit(1)  # TODO handle finding no broker
            else:
                time.sleep(10)
                waited += 1
        else:
            for _ in range(0, 10):
                update_capabilities()
                if broker.register_executor(self_object):
                    registered = True
                    heartbeat_scheduler.enter(60, 1, heartbeat)
                    threading.Thread(target=heartbeat_scheduler.run, daemon=True, name="heartbeat").start()
                    break
                time.sleep(2)


def select_broker() -> Broker:
    with broker_listener.lock:
        return random.choice(list(broker_listener.brokers.values())) if len(broker_listener.brokers) else None


@fastapi_app.put("/submit/{job_id}")
def submit_job(job_id: str, job_info: JobInfo) -> None:
    try:
        job = Job(root_dir, job_id, job_info, store_job_in_datastore)
    except WasmRestException as e:
        raise HTTPException(503, e.msg)
    start_job(job)


@fastapi_app.get("/capabilities")
def server_capabilities() -> Capabilities:
    return update_capabilities()


@fastapi_app.get("/job/list")
def job_list() -> list[str]:
    with jobs_lock:
        return [key for key in jobs.keys()]


@fastapi_app.delete("/job/{job_id}")
def job_delete(job_id: str) -> None:
    with jobs_lock:
        job = jobs.get(job_id, None)
        if job:
            job.delete()
            del jobs[job_id]
        raise HTTPException(404, "Job not found")


def start_job(job: Job):
    for _ in range(10):
        if job.try_download_files(broker):
            break
        time.sleep(10)
    if job.start():
        with jobs_lock:
            jobs[job.id] = job


def store_job_in_datastore(job: Job) -> None:
    for _ in range(10):
        if job.store_named(broker):
            break
    if job.job_info.result_addr.host != "":
        for _ in range(10):
            with open(job.result_path, "br") as result_file:
                if broker.send_result(job.id, result_file):
                    return
                time.sleep(10)
    for _ in range(10):
        with open(job.result_path, "br") as result_file:
            if broker.store_data(result_file, f"{job.id}/result"):
                return
            time.sleep(10)
    for _ in range(10):
        if broker.store_data(BytesIO(b"Could not find datastore to store result: too big"), f"{job.id}/result"):
            return
        time.sleep(10)


def update_capabilities() -> Capabilities:
    battery = psutil.sensors_battery()
    self_object.cur_caps = Capabilities(memory=psutil.virtual_memory().available,
                                        disk=psutil.disk_usage(root_dir).free,
                                        cpu_load=(psutil.getloadavg()[1] / psutil.cpu_count() * 100),
                                        # (1, 5, 15) minutes
                                        cpu_cores=psutil.cpu_count(),
                                        has_battery=(not (battery is None) and not battery.power_plugged),
                                        power=battery.percent if battery else 100)
    self_object.last_update = time.time()
    return self_object.cur_caps


def heartbeat() -> None:
    if not broker.heartbeat_executor(self_object.id, update_capabilities()):
        register_with_broker()
        return
    heartbeat_scheduler.enter(60, 1, heartbeat)


def run(host: str, port: int, rootdir: str, uvicorn_args: dict[str, Any] = None) -> NodeRole:
    global node_object, self_object, root_dir
    root_dir = rootdir
    os.makedirs(root_dir, exist_ok=True)
    node_object = Node(host, port, "executor", fastapi_app, uvicorn_args)
    self_object = Executor(id=node_object.id, address=node_object.address)
    update_capabilities()
    node_object.zeroconf.add_service_listener(Node.zeroconf_service_type("broker"), broker_listener)
    node_object.run()
    return NodeRole.EXIT


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", default=8001, type=int)
    args = parser.parse_args()

    run("127.0.0.1", args.port, "../../executor.d")
