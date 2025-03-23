import random
import threading
import time
from queue import Queue
from typing import Optional, Callable
from uuid import UUID

import readerwriterlock.rwlock
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

from urban_compute_platform.model import JobInfo, Capabilities, ExecutorSelectionMethod
from urban_compute_platform.nodes.node import Node
from urban_compute_platform.nodetypes.executor import Executor
from urban_compute_platform.util.log import LOG


class QueuedJob(BaseModel):
    job_id: UUID
    job_info: JobInfo
    wait_for: set[UUID]


class ExecutorBroker:
    executors: dict[UUID, Executor]
    executor_lock: readerwriterlock.rwlock.RWLockWrite
    queued_jobs: Queue[QueuedJob]
    completed_jobs: set[UUID]
    cj_lock: threading.Lock
    should_exit = False
    __on_job_started: Callable[[UUID, JobInfo], None]
    selection_method: ExecutorSelectionMethod = ExecutorSelectionMethod.RANDOM

    def __init__(self, on_job_started: Callable[[UUID, JobInfo], None], 
                 selection_method: ExecutorSelectionMethod = ExecutorSelectionMethod.RANDOM):
        self.executors = {}
        self.executor_lock = readerwriterlock.rwlock.RWLockWrite()
        self.queued_jobs = Queue()
        self.completed_jobs = set()
        self.cj_lock = threading.Lock()
        self.__on_job_started = on_job_started
        self.selection_method = selection_method

    def set_selection_method(self, method: ExecutorSelectionMethod) -> None:
        self.selection_method = method
        LOG.info(f"Executor selection method changed to {method.value}")

    def add_endpoints(self, fastapi_app: FastAPI) -> None:
        @fastapi_app.put("/executors/register")
        def register_executor(hosts: list[str], executor: Executor, request: Request) -> None:
            LOG.debug(f"Registering executor {executor.id}")
            if request.client.host in hosts:
                hosts.remove(request.client.host)
                hosts.insert(0, request.client.host)
            for host in hosts:
                executor.address.host = host
                name = executor.ping()
                if name is not None and Node.id_from_name(name) == executor.id:
                    with self.executor_lock.gen_wlock():
                        LOG.info(f"Executor {executor.id} registered")
                        self.executors[executor.id] = executor
                        return
                else:
                    LOG.debug(f"Could not ping executor {executor.id} on {host} to verify it's online")
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
        
        @fastapi_app.put("/config/selection_method/{method}")
        def set_selection_method(method: str) -> dict:
            """Ändert die Methode zur Auswahl von Executors."""
            try:
                new_method = ExecutorSelectionMethod(method)
                self.set_selection_method(new_method)
                return {"status": "success", "message": f"Selection method set to {method}"}
            except ValueError:
                raise HTTPException(400, f"Invalid selection method: {method}. Valid options: random, genetic")

        @fastapi_app.put("/job/submit/{job_id}")
        def submit_job(job_info: JobInfo, job_id: UUID, request: Request,
                       wait_for: Optional[set[UUID]] = None) -> UUID:
            LOG.debug(f"Submitting job {job_id}")
            if job_info.result_addr.host == "this":
                job_info.result_addr.host = request.client.host
            if wait_for is None:
                executor = self.capable_executor(job_info.capabilities)
                if executor is None:
                    LOG.error(f"Found no executor capable to run job {job_id}")
                    raise HTTPException(503, "No capable Executor")
                if executor.submit_job(job_id, job_info):
                    self.__on_job_started(job_id, job_info)
                    return job_id
                else:
                    LOG.error(f"Error when submitting job {job_id}")
                    raise HTTPException(503, "Failed to submit Job")
            else:
                self.queue_job(job_id, job_info, wait_for)
            return job_id

        @fastapi_app.put("/job/done/{job_id}")
        def job_done(job_id: UUID, success_score: float = 1.0):
            with self.cj_lock:
                self.completed_jobs.add(job_id)
            
            # Performance-Tracking aktualisieren
            # Finde den Executor, der diesen Job ausgeführt hat
            executor_id = None
            with self.executor_lock.gen_rlock():
                for executor in self.executors.values():
                    if job_id in executor.job_list():
                        executor_id = executor.id
                        break
            
            if executor_id:
                if self.selection_method == ExecutorSelectionMethod.GENETIC:
                    from urban_compute_platform.util.genetic_selector import GeneticExecutorSelector
                    GeneticExecutorSelector.update_performance(executor_id, success_score)
                elif self.selection_method == ExecutorSelectionMethod.BUTTERFLY:
                    from urban_compute_platform.util.butterfly_selector import ButterflyExecutorSelector
                    ButterflyExecutorSelector.update_performance(executor_id, success_score)
                elif self.selection_method == ExecutorSelectionMethod.CORAL:
                    from urban_compute_platform.util.coral_selector import CoralExecutorSelector
                    CoralExecutorSelector.update_performance(executor_id, success_score)

    def capable_executor(self, capabilities: Capabilities) -> Optional[Executor]:
        self.prune_executor_list()
        with self.executor_lock.gen_rlock():
            capable_executors = [executor for executor in self.executors.values() if
                                executor.cur_caps.is_capable(capabilities)]
        
        if not capable_executors:
            return None

        if self.selection_method == ExecutorSelectionMethod.GENETIC:
            # Genetischen Algorithmus verwenden
            from urban_compute_platform.util.genetic_selector import GeneticExecutorSelector
            LOG.debug("Using genetic algorithm for executor selection")
            return GeneticExecutorSelector.select_executor(capable_executors, capabilities)
        elif self.selection_method == ExecutorSelectionMethod.BUTTERFLY:
            # Butterfly-Algorithmus verwenden
            from urban_compute_platform.util.butterfly_selector import ButterflyExecutorSelector
            LOG.debug("Using butterfly algorithm for executor selection")
            return ButterflyExecutorSelector.select_executor(capable_executors, capabilities)
        elif self.selection_method == ExecutorSelectionMethod.CORAL:
            # Coral Reefs-Algorithmus verwenden
            from urban_compute_platform.util.coral_selector import CoralExecutorSelector
            LOG.debug("Using coral reefs algorithm for executor selection")
            return CoralExecutorSelector.select_executor(capable_executors, capabilities)
        else:
            # Zufällige Auswahl verwenden (Standardverhalten)
            LOG.debug("Using random selection for executor")
            return random.choice(capable_executors)

    def prune_executor_list(self) -> None:
        delete_list = []
        current_time = time.time()
        with self.executor_lock.gen_rlock():
            for _, executor in self.executors.items():
                if (current_time - executor.last_update) > 130:
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

    def queue_job(self, job_id: UUID, job_info: JobInfo, wait_for: Optional[set[UUID]] = None):
        if wait_for is None:
            wait_for = set()
        self.queued_jobs.put(QueuedJob(job_id=job_id, job_info=job_info, wait_for=wait_for), block=True)

    def start(self):
        threading.Thread(target=self.job_scheduler, daemon=True, name="broker scheduler").start()

    def stop(self):
        self.should_exit = True

    def job_scheduler(self):
        while True:
            current_job = self.queued_jobs.get(block=True)
            while True:
                if self.should_exit:
                    return
                with self.cj_lock:
                    tmp = current_job.wait_for.issubset(self.completed_jobs)
                if tmp:
                    LOG.debug(f"Trying to submitt job {current_job.job_id}")
                    executor = self.capable_executor(current_job.job_info.capabilities)
                    if executor is not None and executor.submit_job(current_job.job_id, current_job.job_info):
                        self.__on_job_started(current_job.job_id, current_job.job_info)
                        break
                    LOG.debug(f"Failed to submitt job {current_job.job_id}: Wait 10")
                    time.sleep(10)
                else:
                    LOG.debug(f"Job {current_job.job_id} waiting for {current_job.wait_for}: Wait 1")
                    time.sleep(1)
