import json
import os
import random
import threading
import time
from typing import Optional
from uuid import UUID

from fastapi import FastAPI, UploadFile, HTTPException
from pydantic import ValidationError
from zeroconf import Zeroconf

from wasm_rest.exceptions import WasmRestException
from wasm_rest.model import JobInfo
from wasm_rest.nodes.clients.job import Job
from wasm_rest.nodes.listeners.brokers import BrokerListener
from wasm_rest.nodes.node import Node
from wasm_rest.nodetypes.broker import Broker
from wasm_rest.util.util import generate_unique_id, put_file

broker: Broker
node_obj: Node
broker_listener = BrokerListener()
zeroconf = Zeroconf()
fastapi_app = FastAPI()
result_dir = ''
pending_results: dict[UUID, JobInfo] = {}
all_queued = False


@fastapi_app.put("/result/{job_id}")
def receive_result(job_id: UUID, data: UploadFile):
    if pending_results.pop(job_id, None) is None:
        raise HTTPException(404, "Result not expected")
    if not put_file(data.file, os.path.join(result_dir, f"{job_id}.zip")):
        raise HTTPException(500, "File could not be stored")
    if all_queued and len(pending_results) == 0:
        node_obj.stop()


def select_broker() -> Optional[Broker]:
    with broker_listener.lock:
        return random.choice(list(broker_listener.brokers.values())) if len(broker_listener.brokers) else None


def files_to_upload(job_info: JobInfo) -> dict[str, str]:
    to_upload = {}
    try:
        if type(job_info.wasm_bin) is str:
            to_upload[job_info.wasm_bin] = "exec.wasm"
        elif type(job_info.wasm_bin) is tuple[str, str]:
            to_upload[job_info.wasm_bin[0]] = job_info.wasm_bin[1]
        else:
            raise WasmRestException("oops")  # TODO
        if type(job_info.stdin) is str:
            to_upload[job_info.stdin] = "stdin"
        elif type(job_info.stdin) is tuple:
            if job_info.stdin_is_named:
                pass
            else:
                to_upload[job_info.stdin[0]] = job_info.stdin[1]
        else:
            raise WasmRestException("oops")  # TODO

        for host_path, path in job_info.job_data.items():
            to_upload[host_path] = path
    except ValueError as e:
        raise WasmRestException("Invalid Formatting") from e
    return to_upload


def run_job(job_info: JobInfo) -> None:
    job_id = generate_unique_id()
    job = Job(job_id)
    to_upload = files_to_upload(job_info)
    for path, name in to_upload.items():
        job.upload_job_file(name, path, broker)
    job.transform_job_info_broker(job_info)
    broker.submit_job(job_info, job_id)
    if job_info.result_addr == node_obj.address:
        pending_results[job_id] = job_info


def run(json_path: str, host: str = '', port: int = 8004, _result_dir: str = ''):
    global result_dir, node_obj, all_queued, broker
    result_dir = _result_dir
    node_obj = Node(host, port, fastapi_app=fastapi_app)
    node_obj.zeroconf.add_service_listener(Node.zeroconf_service_type("broker"), broker_listener)

    threading.Thread(target=node_obj.run).start()

    time.sleep(3)
    broker = select_broker()
    print(broker)
    if os.path.isfile(json_path):
        with open(json_path, "r") as file:
            cmds = json.load(file)
        for cmd in cmds:
            try:
                run_job(JobInfo.model_validate(cmd))
            except ValidationError:
                print("Invalid Command")
        if len(pending_results):
            all_queued = True
        else:
            node_obj.stop()


if __name__ == '__main__':
    run("../../resources/command.json", "127.0.0.1", 8004, "../../results")
