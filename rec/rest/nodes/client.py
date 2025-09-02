import os
import random
import time
from typing import Optional, Union
from uuid import UUID, uuid4

from fastapi import HTTPException, UploadFile
from pydantic import ValidationError
from readerwriterlock.rwlock import RWLockWrite

from rec.rest.job import ClientJob
from rec.rest.model import ExecutionPlan, JobInfo, NodeRole
from rec.rest.nodes.node import Node
from rec.rest.nodes.zeroconf_listeners.brokers import BrokerListener
from rec.rest.nodetypes.broker import Broker
from rec.util.exceptions import WasmRestException
from rec.util.fs import put_file, try_store_named_data
from rec.util.log import LOG


class Client(Node):
    broker: Optional[Broker]
    broker_listener: BrokerListener
    result_dir = ""
    pending_results: dict[UUID, str]
    result_lock: RWLockWrite
    all_queued = False
    started_jobs: set[str]
    name_to_uuid: dict[str, UUID]
    json_path: str

    def __init__(
        self,
        json_path: str,
        host: list[str] = "",
        port: int = 8004,
        _result_dir: str = "",
    ):
        super().__init__(host, port)
        self.result_dir = _result_dir
        self.broker = None
        self.broker_listener = BrokerListener()
        self.pending_results = {}
        self.result_lock = RWLockWrite()
        self.started_jobs = set()
        self.name_to_uuid = {}
        self.json_path = json_path

    def add_endpoints(self):
        @self.fastapi_app.put("/result/{job_id}")
        def receive_result(job_id: UUID, data: UploadFile):
            with self.result_lock.gen_rlock():
                if self.pending_results.get(job_id, None) is None:
                    LOG.info(f"Received unexpected result for job {job_id}")
                    raise HTTPException(404, "Result not expected")
            if not put_file(data.file, os.path.join(self.result_dir, f"{job_id}.zip")):
                LOG.error(f"Result for job {job_id} could not be stored")
                raise HTTPException(500, "File could not be stored")
            LOG.info(f"Received result of job {job_id}")
            with self.result_lock.gen_wlock():
                self.pending_results.pop(job_id, None)
                if self.all_queued and len(self.pending_results) == 0:
                    self.stop()

    def select_broker(self) -> Broker:
        with self.broker_listener.lock.gen_rlock():
            brokers = [
                b
                for b in self.broker_listener.brokers.values()
                if b.executor_count() > 0
            ]
            return random.choice(brokers) if len(brokers) else None

    def files_to_upload(self, job_info: JobInfo) -> dict[str, str]:
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

    def run_job(
        self, job_name: str, job_info: JobInfo, wait_for: Optional[set[UUID]] = None
    ) -> Optional[UUID]:
        job_id = uuid4()
        LOG.debug(f"Starting job {job_id}")
        job = ClientJob(job_id)
        to_upload = self.files_to_upload(job_info)
        LOG.debug(f"Uploading files for job {job_id}")
        for path, name in to_upload.items():
            LOG.debug(f"Uploading {path} for job {job_id}")
            if not job.upload_job_file(name, path, self.broker):
                job.delete(self.broker)
                LOG.error(f"Failed to upload {path} for job {job_name} {job_id}")
                return None
        job.transform_job_info_broker(job_info)
        LOG.debug(f"Submitting job {job_id}")
        if self.broker.submit_job(job_info, job_id, wait_for) == job_id:
            if (
                job_info.result_addr.host in self.addresses
                or job_info.result_addr.host == "this"
            ):
                with self.result_lock.gen_wlock():
                    self.pending_results[job_id] = job_name
            return job_id
        LOG.error(f"Failed to submit job {job_name}")
        return None

    def load_exec_plan(self, json_path: str) -> Union[ExecutionPlan, NodeRole]:
        try:
            with open(json_path, "r") as file:
                return ExecutionPlan.model_validate_json(file.read())
        except OSError as e:
            LOG.error(f"Problem opening Execution Plan: {e}")
            self.stop()
            return NodeRole.EXIT
        except ValidationError as e:
            LOG.error(f"Invalid Execution Plan: {e}")
            self.stop()
            return NodeRole.EXIT

    def run(self) -> NodeRole:
        self.add_service_listener(
            Node.zeroconf_service_type("broker"), self.broker_listener
        )

        self.start()
        time.sleep(10)
        self.broker = self.select_broker()
        while self.broker is None:
            time.sleep(3)
            self.broker = self.select_broker()
        if self.broker is None:
            LOG.error("No broker found")
            self.stop()
            return NodeRole.EXIT
        LOG.info(f"Selected Broker {self.broker}")

        plan = self.load_exec_plan(self.json_path)
        if type(plan) is NodeRole:
            return plan
        for path, name in plan.named_data.items():
            try_store_named_data(name, path, self.broker)
        for cmd in plan.exec:
            try:
                if not self.started_jobs.issuperset(cmd.wait):
                    LOG.error("Attempted to wait for Job that was not started")
                if cmd.queue:
                    try:
                        wait_for = {self.name_to_uuid[name] for name in cmd.wait}
                    except KeyError:
                        LOG.error("Attempted to wait for Job that was not started")
                        continue  # should never happen
                else:
                    wait_for = None
                    while True:
                        with self.result_lock.gen_rlock():
                            if cmd.wait.isdisjoint(self.pending_results.values()):
                                break
                        time.sleep(10)
                job_id = self.run_job(cmd.cmd, plan.cmds[cmd.cmd], wait_for)
                if job_id:
                    self.name_to_uuid[cmd.cmd] = job_id
                    self.started_jobs.add(cmd.cmd)
                    LOG.info(f"Job {cmd.cmd} was started as {job_id}")
                else:
                    LOG.error(f"Job {cmd.cmd} failed to start")
            except WasmRestException as e:
                LOG.error(f"Invalid Command {cmd.cmd}: {e.msg}")
        with self.result_lock.gen_rlock():
            if len(self.pending_results):
                self.all_queued = True
            else:
                self.stop()
        return NodeRole.EXIT


if __name__ == "__main__":
    Client("../../resources/voice.json", ["127.0.0.1"], 8004, "../../results").run()
