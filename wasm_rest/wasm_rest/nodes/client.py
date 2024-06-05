import os
import random
import time
from typing import Optional, Union
from uuid import UUID

import readerwriterlock.rwlock
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
from wasm_rest.util import put_file, generate_unique_id, try_store_named_data

broker: Broker
node_obj: Node
broker_listener = BrokerListener()
zeroconf = Zeroconf()
fastapi_app = FastAPI()
result_dir = ''
pending_results: dict[UUID, str] = {}
result_lock = readerwriterlock.rwlock.RWLockWrite()
all_queued = False
started_jobs: set[str] = set()
name_to_uuid: dict[str, UUID] = {}


@fastapi_app.put("/result/{job_id}")
def receive_result(job_id: UUID, data: UploadFile):
    with result_lock.gen_rlock():
        if pending_results.get(job_id, None) is None:
            LOG.info(f"Received unexpected result for job {job_id}")
            raise HTTPException(404, "Result not expected")
    if not put_file(data.file, os.path.join(result_dir, f"{job_id}.zip")):
        LOG.error(f"Result for job {job_id} could not be stored")
        raise HTTPException(500, "File could not be stored")
    LOG.info(f"Received result of job {job_id}")
    with result_lock.gen_wlock():
        pending_results.pop(job_id, None)
        if all_queued and len(pending_results) == 0:
            node_obj.stop()


def select_broker() -> Broker:
    with broker_listener.lock.gen_rlock():
        brokers = [b for b in broker_listener.brokers.values() if b.executor_count() > 0]
        return random.choice(brokers) if len(brokers) else None


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


def run_job(job_name: str, job_info: JobInfo, wait_for: Optional[set[UUID]] = None) -> Optional[UUID]:
    job_id = generate_unique_id()
    LOG.debug(f"Starting job {job_id}")
    job = Job(job_id)
    to_upload = files_to_upload(job_info)
    for path, name in to_upload.items():
        LOG.debug(f"Uploading {path} for job {job_id}")
        if not job.upload_job_file(name, path, broker):
            job.delete(broker)
            LOG.error(f"Failed to upload {path} for job {job_name} {job_id}")
            return None
    job.transform_job_info_broker(job_info)
    LOG.debug(f"Submitting job {job_id}")
    if broker.submit_job(job_info, job_id, wait_for) == job_id:
        if job_info.result_addr.host in node_obj.addresses or job_info.result_addr.host == "this":
            with result_lock.gen_wlock():
                pending_results[job_id] = job_name
        return job_id
    LOG.error(f"Failed to submit job {job_name}")
    return None


def load_exec_plan(json_path: str) -> Union[ExecutionPlan, NodeRole]:
    try:
        with open(json_path, "r") as file:
            return ExecutionPlan.model_validate_json(file.read())
    except OSError as e:
        LOG.error(f"Problem opening Execution Plan: {e}")
        node_obj.stop()
        return NodeRole.EXIT
    except ValidationError as e:
        LOG.error(f"Invalid Execution Plan: {e}")
        node_obj.stop()
        return NodeRole.EXIT


def run(json_path: str, host: Union[str, list[str]] = '', port: int = 8004, _result_dir: str = '') -> NodeRole:
    global result_dir, node_obj, all_queued, broker
    result_dir = _result_dir
    node_obj = Node(host, port, fastapi_app=fastapi_app)
    node_obj.add_service_listener(Node.zeroconf_service_type("broker"), broker_listener)

    node_obj.start()
    time.sleep(10)
    broker = select_broker()
    while broker is None:
        time.sleep(3)
        broker = select_broker()
    if broker is None:
        LOG.error("No broker found")
        node_obj.stop()
        return NodeRole.EXIT
    LOG.info(f"Selected Broker {broker}")

    plan = load_exec_plan(json_path)
    if type(plan) is NodeRole:
        return plan
    for path, name in plan.named_data.items():
        try_store_named_data(name, path, broker)
    for cmd in plan.exec:
        try:
            if not started_jobs.issuperset(cmd.wait):
                LOG.error("Attempted to wait for Job that was not started")
            if cmd.queue:
                try:
                    wait_for = {name_to_uuid[name] for name in cmd.wait}
                except KeyError:
                    LOG.error("Attempted to wait for Job that was not started")
                    continue  # should never happen
            else:
                wait_for = None
                while True:
                    with result_lock.gen_rlock():
                        if cmd.wait.isdisjoint(pending_results.values()):
                            break
                    time.sleep(10)
            job_id = run_job(cmd.cmd, plan.cmds[cmd.cmd], wait_for)
            if job_id:
                name_to_uuid[cmd.cmd] = job_id
                started_jobs.add(cmd.cmd)
                LOG.info(f"Job {cmd.cmd} was started as {job_id}")
            else:
                LOG.error(f"Job {cmd.cmd} failed to start")
        except WasmRestException as e:
            LOG.error(f"Invalid Command {cmd.cmd}: {e.msg}")
    with result_lock.gen_rlock():
        if len(pending_results):
            all_queued = True
        else:
            node_obj.stop()
    return NodeRole.EXIT


if __name__ == '__main__':
    run("../../resources/command2.json", "127.0.0.1", 8004, "../../results")
