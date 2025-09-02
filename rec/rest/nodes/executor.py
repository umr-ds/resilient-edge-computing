import argparse
import os
import random
import sched
import threading
import time
from io import BytesIO
from typing import Any, Optional
from uuid import UUID

import psutil
from fastapi import HTTPException
from readerwriterlock.rwlock import RWLockWrite

from rec.rest.job import ExecutorJob
from rec.rest.model import Address, Capabilities, JobInfo, NodeRole
from rec.rest.nodes.node import Node
from rec.rest.nodes.zeroconf_listeners.brokers import BrokerListener
from rec.rest.nodetypes.broker import Broker
from rec.rest.nodetypes.executor import Executor as ExecutorObject
from rec.util.exceptions import WasmRestException
from rec.util.log import LOG


class Executor(Node):
    heartbeat_scheduler: sched.scheduler
    self_object: ExecutorObject

    broker_listener: BrokerListener
    broker: Optional[Broker]

    jobs_lock: RWLockWrite
    jobs: dict[UUID, ExecutorJob]
    root_dir: str = ""

    exit_code = NodeRole.EXIT

    def __init__(
        self,
        host: list[str],
        port: int,
        rootdir: str,
        uvicorn_args: dict[str, Any] = None,
    ):
        super().__init__(host, port, "executor", uvicorn_args)
        self.root_dir = rootdir
        self.heartbeat_scheduler = sched.scheduler()
        self.broker_listener = BrokerListener()
        self.jobs_lock = RWLockWrite()
        self.jobs = {}
        self.self_object = ExecutorObject(
            id=self.id, address=Address(host="", port=self.port)
        )
        self.broker = None

    def register_with_broker(self) -> None:
        waited = 0
        while True:
            self.broker = self.select_broker()
            if self.broker is None:
                if waited == 10:
                    self.exit_code = NodeRole.BROKER
                    self.stop()
                    return
                else:
                    time.sleep(10)
                    waited += 1
            else:
                for _ in range(0, 10):
                    self.update_capabilities()
                    if self.broker.register_executor(self.addresses, self.self_object):
                        self.heartbeat_scheduler.enter(60, 1, self.heartbeat)
                        threading.Thread(
                            target=self.heartbeat_scheduler.run,
                            daemon=True,
                            name="heartbeat",
                        ).start()
                        return
                    else:
                        self.broker_listener.remove_broker(self.broker.id)
                    time.sleep(2)

    def select_broker(self) -> Optional[Broker]:
        with self.broker_listener.lock.gen_rlock():
            for b in self.broker_listener.brokers.values():
                if b.executor_count() == 0:
                    return b
            return (
                random.choice(list(self.broker_listener.brokers.values()))
                if len(self.broker_listener.brokers)
                else None
            )

    def add_endpoints(self):
        @self.fastapi_app.put("/submit/{job_id}")
        def submit_job(job_id: UUID, job_info: JobInfo) -> None:
            LOG.debug(f"Submitting job {job_id}")
            try:
                job = ExecutorJob(
                    self.root_dir, job_id, job_info, self.store_job_in_datastore
                )
            except WasmRestException as e:
                LOG.error(e.msg)
                raise HTTPException(503, e.msg)
            threading.Thread(target=self.start_job, args=[job]).start()

        @self.fastapi_app.get("/capabilities")
        def server_capabilities() -> Capabilities:
            LOG.debug("Sending Capabilities")
            return self.update_capabilities()

        @self.fastapi_app.get("/job/list")
        def job_list() -> list[UUID]:
            LOG.debug("Sending list of all jobs")
            with self.jobs_lock.gen_rlock():
                return [key for key in self.jobs.keys()]

        @self.fastapi_app.delete("/job/{job_id}")
        def job_delete(job_id: UUID) -> None:
            LOG.debug(f"Deleting job {job_id}")
            with self.jobs_lock.gen_wlock():
                job = self.jobs.get(job_id, None)
                if job:
                    job.delete()
                    self.jobs.pop(job_id, None)
                else:
                    LOG.error(f"Failed to delete job {job_id}")
                    raise HTTPException(404, "Job not found")

    def start_job(self, job: ExecutorJob) -> None:
        LOG.debug(f"Resolving data globs for job {job.id}")
        job.resolve_glob_data(self.broker)
        LOG.debug(f"Downloading data for job {job.id}")
        for _ in range(10):
            if job.try_download_files(self.broker):
                break
            time.sleep(10)
        with self.jobs_lock.gen_wlock():
            self.jobs[job.id] = job
        try:
            LOG.debug(f"Starting process for job {job.id}")
            job.run()
        except WasmRestException as e:
            LOG.error(e.msg)
            return
        LOG.debug(f"Finished job {job.id}")

    def do_store_in_datastore(self, job: ExecutorJob) -> None:
        LOG.debug(f"Storing named results for job {job.id}")
        for _ in range(10):
            if job.store_named(self.broker):
                break
        if job.job_info.result_addr.host != "":
            LOG.debug(
                f"Sending result for job {job.id} to client at {job.job_info.result_addr}"
            )
            for _ in range(10):
                with open(job.result_path, "br") as result_file:
                    if self.broker.send_result(job.id, result_file):
                        return
                    time.sleep(10)
            LOG.debug(
                f"Failed to send result for job {job.id} to client at {job.job_info.result_addr}"
            )
        for _ in range(10):
            LOG.debug(f"Storing result for job {job.id}")
            with open(job.result_path, "br") as result_file:
                if self.broker.store_data(result_file, f"{job.id}/result"):
                    return
                time.sleep(10)
        LOG.error(f"Failed to store result for job {job.id}")
        for _ in range(10):
            if self.broker.store_data(
                BytesIO(b"Could not find datastore to store result: too big"),
                f"{job.id}/result",
            ):
                return
            time.sleep(10)
        LOG.error(f"Failed to store error for job {job.id}")

    def store_job_in_datastore(self, job: ExecutorJob) -> None:
        self.do_store_in_datastore(job)
        self.broker.job_done(job.id)
        if job.job_info.delete:
            self.broker.delete_job(job.id)

    def update_capabilities(self) -> Capabilities:
        os.makedirs(self.root_dir, exist_ok=True)
        battery = psutil.sensors_battery()
        self.self_object.cur_caps = Capabilities(
            memory=psutil.virtual_memory().available,
            disk=psutil.disk_usage(self.root_dir).free,
            cpu_load=(psutil.getloadavg()[1] / psutil.cpu_count() * 100),
            # (1, 5, 15) minutes
            cpu_cores=psutil.cpu_count(),
            cpu_freq=psutil.cpu_freq().max,  # not reliable
            has_battery=(battery is not None and not battery.power_plugged),
            power=battery.percent if battery else 100,
        )
        self.self_object.last_update = time.time()
        return self.self_object.cur_caps

    def heartbeat(self) -> None:
        if self.should_stop:
            return
        LOG.debug(f"Trying to send heartbeat to broker {self.broker.id}")
        if not self.broker.heartbeat_executor(self.id, self.update_capabilities()):
            self.register_with_broker()
            return
        LOG.debug(f"Sent heartbeat to broker {self.broker.id}")
        self.heartbeat_scheduler.enter(60, 1, self.heartbeat)

    def run(self) -> NodeRole:
        os.makedirs(self.root_dir, exist_ok=True)
        self.update_capabilities()
        self.add_service_listener(
            Node.zeroconf_service_type("broker"), self.broker_listener
        )
        threading.Thread(
            target=self.register_with_broker, daemon=True, name="register"
        ).start()
        self.do_run()
        return self.exit_code


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", default=8001, type=int)
    args = parser.parse_args()
    Executor(["12.32.4.4", "127.0.0.1"], args.port, "../../executor.d").run()
