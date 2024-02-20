import asyncio
import os
# import socket
import threading
import time
import random
from contextlib import asynccontextmanager
from typing import cast

import psutil
import requests
import uvicorn
from fastapi import FastAPI, UploadFile, HTTPException
from fastapi.responses import FileResponse
from zeroconf import Zeroconf, ServiceBrowser, ServiceStateChange

from wasm_rest.model import Capabilities, Executor, RunRequest, Broker, NodeRole, JobStatus
from wasm_rest.ndn import ndn_app
from wasm_rest.server.job import Job
from wasm_rest.util import prevent_break


def register():
    global broker
    self_object = Executor(host=host, port=port, max_caps=update_caps(),
                           cur_caps=update_caps())  # TODO max caps update time
    broker = select_broker()
    while broker is not None:
        try:
            tries = 0
            while not requests.put(f"http://{broker.host}:{broker.port}/register",
                                   data=self_object.model_dump_json()).ok:
                if tries >= 30:
                    raise requests.exceptions.RequestException()
                tries += 1
                time.sleep(2)
            break
        except requests.exceptions.RequestException:
            # signal.raise_signal(signal.SIGINT)
            # raise WasmRestException("Could not reach root server")
            broker = select_broker()
    asyncio.set_event_loop(asyncio.new_event_loop())
    ndn_app.connect(broker.host)


def select_broker() -> Broker:
    return random.choice(list(brokers.values())) if len(brokers) else None


def found_broker(
        zeroconf: Zeroconf, service_type: str, name: str, state_change: ServiceStateChange
) -> None:
    global broker
    if state_change is ServiceStateChange.Added:
        info = zeroconf.get_service_info(service_type, name)
        if info:
            addresses = info.parsed_scoped_addresses()
            brokers[name] = Broker(id=name[7:-20], host=addresses[0], port=cast(int, info.port),
                                   execs=int.from_bytes(info.properties[b"execs"], "big"))
            print(brokers[name])
            if broker is None:
                threading.Thread(target=register).start()
    elif state_change is ServiceStateChange.Removed:
        print("huh")
        del brokers[name]
        if broker.id == name[7:-20]:
            threading.Thread(target=register).start()


@asynccontextmanager
async def lifespan(_: FastAPI):
    global root_dir
    os.makedirs(root_dir, exist_ok=True)
    threading.Thread(target=register).start()
    yield
    ndn_app.shutdown()


app = FastAPI(lifespan=lifespan)
root_dir = ""
host: str = "localhost"
port: int = 8000
jobs: dict[str, Job] = {}
jobs_lock = threading.Lock()
brokers: dict[str, Broker] = {}
broker: Broker = False  # start with False so first broker is not first discovered broker
uv_server: uvicorn.Server = None


@app.put("/submit")
def submit_new_job(binary: UploadFile) -> str:
    job = Job(root_dir, binary.file, binary.filename)
    if job.status != JobStatus.ERROR:
        with jobs_lock:
            jobs[job.id] = job
        return job.id
    raise HTTPException(500, "Failed to create job")


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


@app.put("/run/{job_id}") # race condition
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


def update_caps() -> Capabilities:
    battery = psutil.sensors_battery()
    return Capabilities(memory=psutil.virtual_memory().available,
                        disk=psutil.disk_usage(root_dir).free,
                        cpu_load=(psutil.getloadavg()[1] / psutil.cpu_count() * 100),  # (1, 5, 15) minutes
                        cpu_cores=psutil.cpu_count(),
                        has_battery=(not (battery is None) and not battery.power_plugged),
                        power=battery.percent if battery else 100)

#def max_caps() -> Capabilities:
#    psutil.virtual_memory().total
#    psutil.disk_usage(root_dir).total


def start(_host: str, _port: int, rootdir: str) -> NodeRole:
    global root_dir, brokers, broker, host, port, uv_server
    host = _host
    port = _port
    zeroconf = Zeroconf()
    services = ["_broker._tcp.local."]
    browser = ServiceBrowser(zeroconf, services, handlers=[found_broker])
    time.sleep(random.random() * 30)
    if not len(brokers):
        browser.cancel()
        brokers = {}
        return NodeRole.BROKER
    root_dir = os.path.abspath(rootdir)
    uv_server = uvicorn.Server(uvicorn.Config(app, host=host, port=port))
    uv_server.run()
    return NodeRole.EXIT
