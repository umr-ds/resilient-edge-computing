import random
import threading
import uuid
from typing import Optional
from uuid import UUID

from fastapi import FastAPI, HTTPException, UploadFile
from fastapi.responses import StreamingResponse

from wasm_rest.model import Address, JobInfo
from wasm_rest.nodes.brokers.datastorecache import DatastoreCache
from wasm_rest.nodes.listeners.datastores import DatastoreListener
from wasm_rest.nodetypes.client import Client
from wasm_rest.nodetypes.datastore import Datastore


class DataBroker:
    datastore_listener = DatastoreListener()
    job_datastore_cache = DatastoreCache()
    pending_results: dict[UUID, Address] = {}
    results_lock = threading.Lock()

    def add_endpoints(self, fastapi_app: FastAPI) -> None:

        @fastapi_app.put("/data/{name:path}")
        def store_data(name: str, data: UploadFile) -> None:
            datastore = self.datastore_for_storage(data.size)
            if datastore:
                datastore.store_data(data.file, name)
            else:
                raise HTTPException(503, "No capable datastore")

        @fastapi_app.get("/data/{name:path}")
        def get_data(name: str, job_id: Optional[UUID] = None) -> StreamingResponse:
            for _ in range(10):
                datastore = self.job_data_location(name, job_id)
                if datastore:
                    data = datastore.get_data_iterator(name)
                    if data:
                        return StreamingResponse(data, media_type="application/octet-stream")
                    else:
                        self.job_datastore_cache.invalidate(name, job_id)  # outdated value from cache
                        continue
                else:
                    raise HTTPException(404, "Data location not known")

        @fastapi_app.put("/result/{job_id}")
        def send_result(job_id: UUID, data: UploadFile) -> None:
            with self.results_lock:
                address = self.pending_results.get(job_id, None)
                if address:
                    client = Client(address=address, id=uuid.uuid4())
                    if client.send_result(job_id, data.file):
                        del self.pending_results[job_id]
                    else:
                        raise HTTPException(503, "Result destination not known")

    # @fastapi_app.get("/datastore")
    def datastore_for_storage(self, required_storage: int) -> Datastore:
        with self.datastore_listener.lock:
            capable_datastores = [datastore for datastore in self.datastore_listener.datastores.values()
                                  if datastore.free_space() > required_storage]
            if len(capable_datastores) != 0:
                datastore = random.choice(capable_datastores)
                return datastore
            else:
                raise HTTPException(503, "No datastore able to hold file")

    # @fastapi_app.get("/datastore/{name:path}")
    def job_data_location(self, name: str, job_id: Optional[UUID] = None) -> Optional[Datastore]:
        datastore = self.job_datastore_cache.get(name, job_id)
        if datastore is not None:
            return datastore
        with self.datastore_listener.lock:
            datastores = [store for store in self.datastore_listener.datastores.values()]
        for datastore in datastores:
            page_number = 0
            while True:
                page_number += 1
                if job_id != '':
                    page = datastore.paginate_data_list(job_id=job_id, page_number=page_number)
                    self.job_datastore_cache.set(page, datastore, job_id=job_id)
                else:
                    page = datastore.paginate_data_list(name)
                    self.job_datastore_cache.set(page, datastore, name)
                if name in page.items:
                    return datastore
                else:
                    if len(page.items) == 0:
                        break
        return None

    def add_pending_job(self, job_id: UUID, job_info: JobInfo):
        with self.results_lock:
            self.pending_results[job_id] = job_info.result_addr

    def delete_job_data(self, job_id: UUID):
        with self.datastore_listener.lock:
            for store in self.datastore_listener.datastores.values():
                store.delete_job_data(job_id)
