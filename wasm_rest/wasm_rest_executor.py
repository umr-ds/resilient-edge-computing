import os
import socket
import tempfile
import threading
import time
import random
from contextlib import asynccontextmanager
from typing import cast, IO

import psutil
import uvicorn
from fastapi import FastAPI, UploadFile, HTTPException
from fastapi.responses import FileResponse
from zeroconf import Zeroconf, ServiceInfo, ServiceListener

from wasm_rest.model import Capabilities, RunRequest, NodeRole, JobStatus, Command, Node, Address
from wasm_rest.server.job import Job
from wasm_rest.server.executor import Executor
from wasm_rest.server.broker import Broker
from wasm_rest.util import prevent_break, gen_unique_id


@asynccontextmanager
async def lifespan(_: FastAPI):
    threading.Thread(target=register).start()
    yield


app = FastAPI(lifespan=lifespan)
zeroconf = Zeroconf()
uv_server: uvicorn.Server = None

root_dir = ""

jobs: dict[str, Job] = {}
jobs_lock = threading.Lock()
brokers: dict[str, Broker] = {}
broker: Broker = False  # start with False so first broker is not first discovered broker

self_object: Executor


def register():
    global broker
    update_caps()
    registered = False
    while not registered:
        broker = select_broker()
        if broker is None:
            raise SystemExit(1)  # TODO handle finding no broker
        else:
            for _ in range(0, 10):
                if broker.register_executor(self_object):
                    registered = True
                    break
                time.sleep(2)

    info = zeroconf.get_service_info("_broker_wasm-rest._tcp.local.",
                                     f"_broker{broker.node.id}._wasm-rest._tcp.local.")


def select_broker() -> Broker:
    return random.choice(list(brokers.values())) if len(brokers) else None


class BrokerListener(ServiceListener):

    def update_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        pass

    def remove_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        broker_id = brokers[name].node.id
        del brokers[name]
        if broker.node.id == broker_id:
            threading.Thread(target=register).start()

    def add_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        info = zeroconf.get_service_info(type_, name)
        if info:
            addresses = info.parsed_scoped_addresses()
            brokers[name] = Broker(
                node=Node(id=info.decoded_properties["node_id"], address=Address(host=addresses[0],
                                                                                 port=cast(int, info.port))),
                execs=int.from_bytes(info.properties[b"execs"], "big"))
            print(brokers[name])
            if broker is None:
                threading.Thread(target=register).start()


@app.put("/submit")
def submit_new_job(binary: UploadFile) -> str:
    job = Job(root_dir, binary.file, binary.filename)
    if job.status != JobStatus.ERROR:
        with jobs_lock:
            jobs[job.id] = job
        return job.id
    raise HTTPException(500, "Failed to create job")


@app.put("/submit-stored")
def submit_stored_job(command: Command) -> str:
    fd, name = tempfile.mkstemp(prefix="wasm_rest")
    with open(fd, "bw") as file:
        get_named_data(file, command.wasm_bin)
    job = Job(root_dir, name, "exec.wasm")
    if job.status != JobStatus.ERROR:
        threading.Thread(target=run_stored_job, args=[job, command]).start()
        return job.id
    raise HTTPException(500, "Failed to create Job")


@app.put("/upload/{job_id}/{path}")
def upload_data(data: UploadFile, job_id: str, path: str) -> None:
    job = jobs.get(job_id)
    if job is None:
        raise HTTPException(404, "Job does not exist")
    if path.endswith("/"):
        path += data.filename
    path = prevent_break(path)
    if job.put_data(path, data.file):
        return
    raise HTTPException(500, "File could not be created")


@app.put("/mkdir/{job_id}")
def make_dir(job_id: str, dirs: list[str]) -> None:
    job = jobs.get(job_id)
    if job is None:
        raise HTTPException(404, "Job does not exist")
    job.mkdirs(dirs)


@app.put("/run/{job_id}")  # race condition
def run_wasm(job_id: str, cmd: RunRequest, req: Capabilities) -> None:
    job = jobs.get(job_id)
    if job is None:
        raise HTTPException(404, "Job does not exist")
    if not update_caps().is_capable(req):
        raise HTTPException(503, "Server is (currently) not capable")
    if job.status == JobStatus.RUNNING:
        raise HTTPException(503, "Job is currently running")

    if not job.run(cmd):
        raise HTTPException(404, "Job executable not found")


@app.get("/caps")
def get_server_caps() -> Capabilities:
    return update_caps()


@app.delete("/delete/{job_id}")
def delete_job(job_id: str) -> None:
    job = jobs.get(job_id)
    if job is None:
        return
    if job.status == JobStatus.RUNNING:
        raise HTTPException(400, "Job is running")
    if not job.delete():
        raise HTTPException(500, "Failed to delete Job")
    with jobs_lock:
        del jobs[job_id]


@app.get("/result/{job_id}")
def get_job_result(job_id: str) -> FileResponse:
    job = jobs.get(job_id)
    if job is None:
        raise HTTPException(404, "")
    if job.status is JobStatus.INIT:
        raise HTTPException(400, "Job has not been started")
    if job.status is JobStatus.RUNNING:
        raise HTTPException(204, "Job is still running")
    if not os.path.exists(job.result_path):
        raise HTTPException(400, "Job failed to generate output")
    filename = job_id + ".zip"
    return FileResponse(job.result_path, media_type="application/zip", filename=filename)


def get_named_data(file: IO[bytes], name: str):
    database = broker.get_data_location(name)
    if database is None:
        raise BaseException("")  # TODO
    if not database.get_data(file, name):
        raise BaseException("")


def run_stored_job(job: Job, command: Command) -> None:
    for name, path in command.data.items():
        with job.open_data_for_writing(path) as file:
            get_named_data(file, name)
    stdin = ""
    for name, stdin_file in command.stdin.items():
        stdin = stdin_file
        if name != "local":
            with job.open_data_for_writing(stdin_file) as file:
                get_named_data(file, name)
        break
    cmd = RunRequest(job_id=job.id, stdin_file=stdin,
                     args=command.args, env=command.env, results=command.results)
    with jobs_lock:
        jobs[job.id] = job
    job.run(cmd)


def update_caps() -> Capabilities:
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


def broadcast_capabilities():
    update_caps()

    info = ServiceInfo(
        "_executor_wasm-rest._tcp.local.",
        f"_executor{self_object.node.id}_wasm-rest._tcp.local.",
        addresses=[socket.inet_aton(self_object.node.address.host)],
        # socket.inet_aton(socket.gethostbyname(host))
        port=self_object.node.address.port,
        server=f"_broker{self_object.node.id}._wasm-rest._tcp.local.",
        properties={"exec": self_object.model_dump_json()}
    )
    zeroconf.update_service(info)


def start(host: str, port: int, rootdir: str) -> NodeRole:
    global root_dir, brokers, broker, uv_server, self_object
    root_dir = os.path.abspath(rootdir)
    os.makedirs(root_dir, exist_ok=True)
    self_object = Executor(node=Node(address=Address(host=host, port=port), id=gen_unique_id()),
                           cur_caps=Capabilities(), last_update=time.time())
    update_caps()
    uv_server = uvicorn.Server(uvicorn.Config(app, host=host, port=port))
    zeroconf.add_service_listener("_broker_wasm-rest._tcp.local.", BrokerListener())
    time.sleep(10)
    if not len(brokers):
        brokers = {}
        return NodeRole.BROKER

    uv_server.run()
    return NodeRole.EXIT
