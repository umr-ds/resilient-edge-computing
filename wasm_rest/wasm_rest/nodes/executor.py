import argparse
import os
import random
import sched
import threading
import time
from contextlib import asynccontextmanager
from io import BytesIO
from typing import Any, Union
from uuid import UUID

import psutil
import readerwriterlock.rwlock
from fastapi import FastAPI, HTTPException

from wasm_rest.exceptions import WasmRestException
from wasm_rest.model import JobInfo, NodeRole, Capabilities, Address
from wasm_rest.nodes.executors.job import Job
from wasm_rest.nodes.listeners.brokers import BrokerListener
from wasm_rest.nodes.node import Node
from wasm_rest.nodetypes.broker import Broker
from wasm_rest.nodetypes.executor import Executor
from wasm_rest.util.log import LOG


@asynccontextmanager
async def lifespan(_: FastAPI):
    threading.Thread(target=register_with_broker, daemon=True, name="register").start()
    yield


heartbeat_scheduler = sched.scheduler()
fastapi_app = FastAPI(lifespan=lifespan)
node_object: Node
self_object: Executor
addresses: list[str] = []

broker_listener = BrokerListener()
broker: Broker

jobs_lock = readerwriterlock.rwlock.RWLockWrite()
jobs: dict[UUID, Job] = {}
root_dir: str = ""

exit_code = NodeRole.EXIT


def register_with_broker() -> None:
    global broker, exit_code
    registered = False
    waited = 0
    while not registered:
        broker = select_broker()
        if broker is None:
            if waited == 10:
                exit_code = NodeRole.BROKER
                node_object.stop()
            else:
                time.sleep(10)
                waited += 1
        else:
            for _ in range(0, 10):
                update_capabilities()
                if broker.register_executor(addresses, self_object):
                    registered = True
                    heartbeat_scheduler.enter(60, 1, heartbeat)
                    threading.Thread(target=heartbeat_scheduler.run, daemon=True, name="heartbeat").start()
                    return
                time.sleep(2)


def select_broker() -> Broker:
    with broker_listener.lock.gen_rlock():
        for broker in broker_listener.brokers.values():
            if broker.executor_count() == 0:
                return broker
        return random.choice(list(broker_listener.brokers.values())) if len(broker_listener.brokers) else None


@fastapi_app.put("/submit/{job_id}")
def submit_job(job_id: UUID, job_info: JobInfo) -> None:
    LOG.debug(f"Submitting job {job_id}")
    try:
        job = Job(root_dir, job_id, job_info, store_job_in_datastore)
    except WasmRestException as e:
        LOG.error(e.msg)
        raise HTTPException(503, e.msg)
    threading.Thread(target=start_job, args=[job]).start()


@fastapi_app.get("/capabilities")
def server_capabilities() -> Capabilities:
    LOG.debug("Sending Capabilities")
    return update_capabilities()


@fastapi_app.get("/job/list")
def job_list() -> list[UUID]:
    LOG.debug("Sending list of all jobs")
    with jobs_lock.gen_rlock():
        return [key for key in jobs.keys()]


@fastapi_app.delete("/job/{job_id}")
def job_delete(job_id: UUID) -> None:
    LOG.debug(f"Deleting job {job_id}")
    with jobs_lock.gen_wlock():
        job = jobs.get(job_id, None)
        if job:
            job.delete()
            jobs.pop(job_id, None)
        else:
            LOG.error(f"Failed to delete job {job_id}")
            raise HTTPException(404, "Job not found")


def start_job(job: Job) -> None:
    LOG.debug(f"Resolving data globs for job {job.id}")
    job.resolve_glob_data(broker)
    LOG.debug(f"Downloading data for job {job.id}")
    for _ in range(10):
        if job.try_download_files(broker):
            break
        time.sleep(10)
    with jobs_lock.gen_wlock():
        jobs[job.id] = job
    try:
        LOG.debug(f"Starting process for job {job.id}")
        job.run()
    except WasmRestException as e:
        LOG.error(e.msg)
        return
    LOG.debug(f"Finished job {job.id}")


def do_store_in_datastore(job: Job) -> None:
    LOG.debug(f"Storing named results for job {job.id}")
    for _ in range(10):
        if job.store_named(broker):
            break
    if job.job_info.result_addr.host != "":
        LOG.debug(f"Sending result for job {job.id} to client at {job.job_info.result_addr}")
        for _ in range(10):
            with open(job.result_path, "br") as result_file:
                if broker.send_result(job.id, result_file):
                    return
                time.sleep(10)
    for _ in range(10):
        LOG.debug(f"Storing result for job {job.id}")
        with open(job.result_path, "br") as result_file:
            if broker.store_data(result_file, f"{job.id}/result"):
                return
            time.sleep(10)
    LOG.error(f"Failed to store result for job {job.id}")
    for _ in range(10):
        if broker.store_data(BytesIO(b"Could not find datastore to store result: too big"), f"{job.id}/result"):
            return
        time.sleep(10)
    LOG.error(f"Failed to store error for job {job.id}")


def store_job_in_datastore(job: Job) -> None:
    do_store_in_datastore(job)
    if job.job_info.delete:
        broker.delete_job(job.id)


def update_capabilities() -> Capabilities:
    os.makedirs(root_dir, exist_ok=True)
    battery = psutil.sensors_battery()
    self_object.cur_caps = Capabilities(memory=psutil.virtual_memory().available,
                                        disk=psutil.disk_usage(root_dir).free,
                                        cpu_load=(psutil.getloadavg()[1] / psutil.cpu_count() * 100),
                                        # (1, 5, 15) minutes
                                        cpu_cores=psutil.cpu_count(),
                                        has_battery=(battery is not None and not battery.power_plugged),
                                        power=battery.percent if battery else 100)
    self_object.last_update = time.time()
    return self_object.cur_caps


def heartbeat() -> None:
    LOG.debug(f"Trying to send heartbeat to broker {broker.id}")
    if not broker.heartbeat_executor(self_object.id, update_capabilities()):
        register_with_broker()
        return
    LOG.debug(f"Sent heartbeat to broker {broker.id}")
    heartbeat_scheduler.enter(60, 1, heartbeat)


def run(host: Union[str, list[str]], port: int, rootdir: str, uvicorn_args: dict[str, Any] = None) -> NodeRole:
    global node_object, self_object, root_dir
    root_dir = rootdir
    os.makedirs(root_dir, exist_ok=True)
    node_object = Node(host, port, "executor", fastapi_app, uvicorn_args)
    self_object = Executor(id=node_object.id, address=Address(address='', port=port))
    if type(host) is str:
        addresses.append(host)
    elif type(host) is list:
        addresses.extend(host)
    update_capabilities()
    node_object.add_service_listener(Node.zeroconf_service_type("broker"), broker_listener)
    node_object.run()
    return exit_code


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", default=8001, type=int)
    args = parser.parse_args()

    run(["12.32.4.4", "127.0.0.1"], args.port, "../../executor.d")
