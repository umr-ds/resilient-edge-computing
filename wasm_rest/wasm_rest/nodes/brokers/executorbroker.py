import random
import threading
import time
from http.client import HTTPException
from typing import Optional, Callable
from uuid import UUID

from fastapi import FastAPI

from wasm_rest.model import JobInfo, Capabilities
from wasm_rest.nodetypes.executor import Executor


class ExecutorBroker:
    executors: dict[UUID, Executor] = {}
    executor_lock = threading.Lock()
    __on_job_started: Callable[[UUID, JobInfo], None]

    def __init__(self, on_job_started: Callable[[UUID, JobInfo], None]):
        self.__on_job_started = on_job_started

    def add_endpoints(self, fastapi_app: FastAPI) -> None:
        @fastapi_app.put("/executors/register")
        def register_executor(executor: Executor) -> None:
            if executor.update_capabilities() is not None:
                with self.executor_lock:
                    self.executors[executor.id] = executor
            else:
                raise HTTPException(400, "Could not request Capabilities to verify")

        @fastapi_app.put("/executors/heartbeat/{exec_id}")
        def heartbeat_executor(exec_id: UUID, capabilities: Capabilities) -> None:
            with self.executor_lock:
                executor = self.executors.get(exec_id)
                if executor is None:
                    raise HTTPException(404, "No such executor")
                executor.cur_caps = capabilities
                executor.last_update = time.time()

        @fastapi_app.get("/executors/count")
        def executor_count() -> int:
            return len(self.executors)

        @fastapi_app.put("/job/submit/{job_id}")
        def submit_job(job_info: JobInfo, job_id: UUID) -> UUID:
            executor = self.capable_executor(job_info.capabilities)
            if executor is None:
                raise HTTPException(503, "No capable Executor")
            if executor.submit_job(job_id, job_info):
                self.__on_job_started(job_id, job_info)
                return job_id
            else:
                raise HTTPException(503, "Failed to submit Job")

    def capable_executor(self, capabilities: Capabilities) -> Optional[Executor]:
        self.prune_executor_list()
        with self.executor_lock:
            capable_executors = [executor for executor in self.executors.values() if
                                 executor.cur_caps.is_capable(capabilities)]
        if len(capable_executors):
            return random.choice(capable_executors)
        return None

    def prune_executor_list(self) -> None:
        delete_list = []
        current_time = time.time()
        with self.executor_lock:
            for _, executor in self.executors.items():
                if (current_time - executor.last_update) > 120:
                    delete_list.append(executor)
            for executor in delete_list:
                del self.executors[executor.id]

    def delete_job_from_executor(self, job_id: UUID) -> bool:
        with self.executor_lock:
            for executor in self.executors.values():
                if executor.job_delete(job_id):
                    return True
        return False
