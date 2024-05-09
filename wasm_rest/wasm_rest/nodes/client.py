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
from wasm_rest.model import JobInfo, ExecutionPlan, NodeRole
from wasm_rest.nodes.clients.job import Job
from wasm_rest.nodes.listeners.brokers import BrokerListener
from wasm_rest.nodes.node import Node
from wasm_rest.nodetypes.broker import Broker
from wasm_rest.util.log import LOG
from wasm_rest.util.util import generate_unique_id, put_file, try_store_named_data

broker: Broker
node_obj: Node
broker_listener = BrokerListener()
zeroconf = Zeroconf()
fastapi_app = FastAPI()
result_dir = ''
pending_results: dict[UUID, str] = {}
all_queued = False
started_jobs: set[str] = set()


@fastapi_app.put("/result/{job_id}")
def receive_result(job_id: UUID, data: UploadFile):
    if pending_results.get(job_id, None) is None:
        raise HTTPException(404, "Result not expected")
    if not put_file(data.file, os.path.join(result_dir, f"{job_id}.zip")):
        raise HTTPException(500, "File could not be stored")
    del pending_results[job_id]
    if all_queued and len(pending_results) == 0:
        node_obj.stop()


def select_broker() -> Optional[Broker]:
    with broker_listener.lock:
        return random.choice(list(broker_listener.brokers.values())) if len(broker_listener.brokers) else None


def files_to_upload(job_info: JobInfo) -> dict[str, str]:
    to_upload = {}
    try:
        if not job_info.wasm_bin_is_named:
            if type(job_info.wasm_bin) is str:
                to_upload[job_info.wasm_bin] = "exec.wasm"
            elif type(job_info.wasm_bin) is tuple[str, str]:
                to_upload[job_info.wasm_bin[0]] = job_info.wasm_bin[1]
            else:
                raise WasmRestException("Invalid Formatting in wasm_bin")
        if not job_info.stdin_is_named:
            if type(job_info.stdin) is str:
                if job_info.stdin != "":
                    to_upload[job_info.stdin] = "stdin"
            elif type(job_info.stdin) is tuple:
                if job_info.stdin[0] != "":
                    to_upload[job_info.stdin[0]] = job_info.stdin[1]
            else:
                raise WasmRestException("Invalid Formatting in stdin")

        for host_path, path in job_info.job_data.items():
            to_upload[host_path] = path
    except ValueError as e:
        raise WasmRestException("Invalid Formatting") from e
    return to_upload


def run_job(job_name: str, job_info: JobInfo) -> bool:
    job_id = generate_unique_id()
    job = Job(job_id)
    to_upload = files_to_upload(job_info)
    for path, name in to_upload.items():
        if not job.upload_job_file(name, path, broker):
            job.delete(broker)
            return False
    job.transform_job_info_broker(job_info)
    if broker.submit_job(job_info, job_id) == job_id:
        if job_info.result_addr == node_obj.address:
            pending_results[job_id] = job_name
            return True
    return False


def run(json_path: str, host: str = '', port: int = 8004, _result_dir: str = '') -> NodeRole:
    global result_dir, node_obj, all_queued, broker
    result_dir = _result_dir
    node_obj = Node(host, port, fastapi_app=fastapi_app)
    node_obj.zeroconf.add_service_listener(Node.zeroconf_service_type("broker"), broker_listener)

    threading.Thread(target=node_obj.run).start()

    time.sleep(3)
    broker = select_broker()
    if broker is None:
        LOG.error("No broker found")
        node_obj.stop()
        return NodeRole.EXIT
    LOG.info(f"Selected Broker {broker}")

    if os.path.isfile(json_path):
        try:
            with open(json_path, "r") as file:
                plan = ExecutionPlan.model_validate_json(file.read())
        except ValidationError as e:
            LOG.error(f"Invalid Execution Plan: {e}")
            node_obj.stop()
            return NodeRole.EXIT
        for path, name in plan.named_data.items():
            try_store_named_data(name, path, broker)
        for cmd in plan.exec:
            try:
                if not started_jobs.issuperset(cmd.wait):
                    LOG.error("Attempted to wait for Job that was not started")
                while not cmd.wait.isdisjoint(pending_results.values()):
                    time.sleep(10)
                if run_job(cmd.cmd, plan.cmds[cmd.cmd]):
                    started_jobs.add(cmd.cmd)
            except WasmRestException as e:
                LOG.error(f"Invalid Command {cmd.cmd}: {e.msg}")
        if len(pending_results):
            all_queued = True
        else:
            node_obj.stop()

    else:
        node_obj.stop()
        LOG.error("Execution Plan not found")
    return NodeRole.EXIT


if __name__ == '__main__':
    run("../../resources/command2.json", "127.0.0.1", 8004, "../../results")
