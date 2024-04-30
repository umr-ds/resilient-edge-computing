import random
import threading
import time
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, UploadFile
from fastapi.responses import StreamingResponse

from wasm_rest.model import NodeRole, Capabilities, JobInfo, Address
from wasm_rest.nodes.brokers.cache import Cache
from wasm_rest.nodes.listeners.datastores import DatastoreListener
from wasm_rest.nodes.node import Node
from wasm_rest.nodetypes.client import Client
from wasm_rest.nodetypes.datastore import Datastore
from wasm_rest.nodetypes.executor import Executor

fastapi_app = FastAPI()
node_object: Node

datastore_listener = DatastoreListener()

executors: dict[str, Executor] = {}
executor_lock = threading.Lock()
pending_results: dict[str, Address] = {}
job_datastore_cache = Cache()


@fastapi_app.put("/executors/register")
def register_executor(executor: Executor) -> None:
    if executor.update_capabilities() is not None:
        with executor_lock:
            executors[executor.id] = executor
    else:
        raise HTTPException(400, "Could not request Capabilities")


@fastapi_app.put("/executors/heartbeat/{exec_id}")
def heartbeat_executor(exec_id: str, capabilities: Capabilities) -> None:
    with executor_lock:
        executor = executors.get(exec_id)
        if executor is None:
            raise HTTPException(404, "No such executor")
        executor.cur_caps = capabilities
        executor.last_update = time.time()


@fastapi_app.get("/executors/count")
def executor_count() -> int:
    return len(executors)


@fastapi_app.get("/datastore/{name:path}")
def job_data_location(name: str, job_id: str = '', invalidate: bool = False) -> Datastore:
    if invalidate:
        job_datastore_cache.invalidate(name, job_id)
    datastore = job_datastore_cache.get(name, job_id)
    if datastore is not None:
        return datastore
    with datastore_listener.lock:
        for datastore in datastore_listener.datastores.values():
            page_number = 0
            while True:
                page_number += 1
                if job_id != '':
                    page = datastore.paginate_data_list(job_id=job_id, page_number=page_number)
                    job_datastore_cache.set(page, datastore, job_id=job_id)
                else:
                    page = datastore.paginate_data_list(name)
                    job_datastore_cache.set(page, datastore, name)
                if name in page.items:
                    return datastore
                else:
                    if len(page.items) == 0:
                        break
    raise HTTPException(404, "Named data does not exist")


@fastapi_app.get("/datastore")
def datastore_for_storage(required_storage: int) -> Datastore:
    with datastore_listener.lock:
        capable_datastores = [datastore for datastore in datastore_listener.datastores.values()
                              if datastore.free_space() > required_storage]
        if len(capable_datastores) != 0:
            datastore = random.choice(capable_datastores)
            return datastore
        else:
            raise HTTPException(503, "No datastore able to hold file")


@fastapi_app.put("/data/{name:path}")
def store_data(name: str, data: UploadFile) -> None:
    datastore = datastore_for_storage(data.size)
    if datastore:
        datastore.store_data(data.file, name)
    else:
        raise HTTPException(503, "No capable datastore")


@fastapi_app.get("/data/{name:path}")
def get_data(name: str) -> StreamingResponse:
    datastore = job_data_location(name)
    if datastore:
        data = datastore.get_data_iterator(name)
        if data:
            return StreamingResponse(data, media_type="application/octet-stream")
        else:
            raise HTTPException(404, "Error in Datastore")
    else:
        raise HTTPException(404, "Data location not known")


@fastapi_app.put("/result/{job_id}")
def send_result(job_id: str, data: UploadFile) -> None:
    address = pending_results.get(job_id, None)
    if address:
        client = Client(address=address, id='')
        if client.send_result(job_id, data.file):
            del pending_results[job_id]
        else:
            raise HTTPException(503, "Result destination not known")


@fastapi_app.put("/job/submit/{job_id}")
def submit_job(job_info: JobInfo, job_id: str) -> str:
    executor = capable_executor(job_info.capabilities)
    if executor is None:
        raise HTTPException(503, "No capable Executor")
    if executor.submit_job(job_id, job_info):
        pending_results[job_id] = job_info.result_addr
        return job_id
    else:
        raise HTTPException(503, "Failed to submit Job")


@fastapi_app.delete("/job/{job_id}")
def delete_job(job_id: str) -> None:
    with datastore_listener.lock:
        for store in datastore_listener.datastores.values():
            store.delete_job_data(job_id)
    with executor_lock:
        for executor in executors.values():
            if executor.job_delete(job_id):
                break


def capable_executor(capabilities: Capabilities) -> Optional[Executor]:
    prune_executor_list()
    with executor_lock:
        capable_executors = [executor for executor in executors.values() if executor.cur_caps.is_capable(capabilities)]
    if len(capable_executors):
        return random.choice(capable_executors)
    return None


def prune_executor_list() -> None:
    delete_list = []
    current_time = time.time()
    with executor_lock:
        for _, executor in executors.items():
            if (current_time - executor.last_update) > 120:
                delete_list.append(executor)
        for executor in delete_list:
            del executors[executor.id]


def run(host: str, port: int, uvicorn_args: dict[str, Any] = None) -> NodeRole:
    global node_object
    node_object = Node(host, port, "broker", fastapi_app, uvicorn_args)
    node_object.zeroconf.add_service_listener(Node.zeroconf_service_type("datastore"), datastore_listener)
    node_object.run()
    return NodeRole.EXIT


if __name__ == '__main__':
    run("127.0.0.1", 8000)
