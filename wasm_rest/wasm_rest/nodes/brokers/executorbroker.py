import random
import time

from typing import Optional, Callable
from uuid import UUID

import readerwriterlock.rwlock
from fastapi import FastAPI, HTTPException

from wasm_rest.model import JobInfo, Capabilities
from wasm_rest.nodes.node import Node
from wasm_rest.nodetypes.executor import Executor
from wasm_rest.util.log import LOG


class ExecutorBroker:
    executors: dict[UUID, Executor] = {}
    executor_lock = readerwriterlock.rwlock.RWLockWrite()
    __on_job_started: Callable[[UUID, JobInfo], None]

    def __init__(self, on_job_started: Callable[[UUID, JobInfo], None]):
        self.__on_job_started = on_job_started

    def add_endpoints(self, fastapi_app: FastAPI) -> None:
        @fastapi_app.put("/executors/register")
        def register_executor(executor: Executor) -> None:
            LOG.debug(f"Registering executor {executor.id}")
            name = executor.ping()
            if name is not None and Node.id_from_name(name) == executor.id:
                with self.executor_lock.gen_wlock():
                    LOG.info(f"Executor {executor.id} registered")
                    self.executors[executor.id] = executor
            else:
                LOG.error(f"Could not ping executor {executor.id} to verify it's online")
                raise HTTPException(400, "Could not ping to verify")

        @fastapi_app.put("/executors/heartbeat/{exec_id}")
        def heartbeat_executor(exec_id: UUID, capabilities: Capabilities) -> None:
            LOG.debug(f"heartbeat from executor {exec_id}")
            with self.executor_lock.gen_wlock():
                executor = self.executors.get(exec_id)
                if executor is None:
                    LOG.error(f"Executor {exec_id} not registered")
                    raise HTTPException(404, "No such executor")
                executor.cur_caps = capabilities
                executor.last_update = time.time()

        @fastapi_app.get("/executors/count")
        def executor_count() -> int:
            LOG.debug("Sending number of executors")
            self.prune_executor_list()
            return len(self.executors)

        @fastapi_app.put("/job/submit/{job_id}")
        def submit_job(job_info: JobInfo, job_id: UUID) -> UUID:
            LOG.debug(f"Submitting job {job_id}")
            executor = self.capable_executor(job_info.capabilities)
            if executor is None:
                LOG.error(f"Found not executor capable to run job {job_id}")
                raise HTTPException(503, "No capable Executor")
            if executor.submit_job(job_id, job_info):
                self.__on_job_started(job_id, job_info)
                return job_id
            else:
                LOG.error(f"Error when submitting job {job_id}")
                raise HTTPException(503, "Failed to submit Job")

    def capable_executor(self, capabilities: Capabilities) -> Optional[Executor]:
        self.prune_executor_list()
        with self.executor_lock.gen_rlock():
            capable_executors = [executor for executor in self.executors.values() if
                                 executor.cur_caps.is_capable(capabilities)]
        if len(capable_executors):
            return random.choice(capable_executors)
        return None

    def prune_executor_list(self) -> None:
        delete_list = []
        current_time = time.time()
        with self.executor_lock.gen_rlock():
            for _, executor in self.executors.items():
                if (current_time - executor.last_update) > 120:
                    delete_list.append(executor)
        with self.executor_lock.gen_wlock():
            for executor in delete_list:
                self.executors.pop(executor.id, None)

    def delete_job_from_executor(self, job_id: UUID) -> bool:
        with self.executor_lock.gen_rlock():
            for executor in self.executors.values():
                if executor.job_delete(job_id):
                    return True
        return False
