import random
from typing import Any, Callable, Optional
from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from fastapi_pagination import Page
from readerwriterlock.rwlock import RWLockWrite

from rec.rest.model import Address, JobInfo
from rec.rest.nodes.brokers.datastorecache import DatastoreCache
from rec.rest.nodes.zeroconf_listeners.datastores import DatastoreListener
from rec.rest.nodetypes.client import Client
from rec.rest.nodetypes.datastore import Datastore
from rec.util.log import LOG


class DataBroker:
    datastore_listener: DatastoreListener
    job_datastore_cache: DatastoreCache
    pending_results: dict[UUID, Address]
    results_lock: RWLockWrite

    def __init__(self):
        self.datastore_listener = DatastoreListener()
        self.job_datastore_cache = DatastoreCache()
        self.pending_results = {}
        self.results_lock = RWLockWrite()

    def add_endpoints(self, fastapi_app: FastAPI) -> None:

        @fastapi_app.put("/data/{name:path}")
        def store_data(name: str, data: UploadFile) -> None:
            LOG.debug(f"Storing data {name}")
            datastore = self.datastore_for_storage(data.size)
            if datastore:
                datastore.store_data(data.file, name)
            else:
                LOG.error(f"Could not find datastore to hold {name}")
                raise HTTPException(503, "No capable datastore")

        @fastapi_app.get("/data/{name:path}")
        def get_data(name: str, job_id: Optional[UUID] = None) -> StreamingResponse:
            LOG.debug(f"Retrieving data {name}")
            for _ in range(10):
                datastore = self.job_data_location(name, job_id)
                if datastore:
                    data = datastore.get_data_iterator(name)
                    if data:
                        return StreamingResponse(
                            data, media_type="application/octet-stream"
                        )
                    else:
                        self.job_datastore_cache.invalidate(
                            name, job_id
                        )  # outdated value from cache
                        continue
                else:
                    LOG.error(f"Could not find location of {name}")
                    raise HTTPException(404, "Data location not known")

        @fastapi_app.get("/list/data/{name:path}")
        def get_data_glob(name: str) -> list[str]:
            LOG.debug(f"Listing files starting with {name}")
            result = []
            self.iter_data(
                name, None, lambda page, datastore: result.extend(page.items)
            )
            return result

        @fastapi_app.put("/result/{job_id}")
        def send_result(job_id: UUID, data: UploadFile) -> None:
            LOG.debug(f"Sending result of job {job_id} to client")
            with self.results_lock.gen_rlock():
                address = self.pending_results.get(job_id, None)
            if address:
                client = Client(address=address, id=uuid4())
                if client.send_result(job_id, data.file):
                    with self.results_lock.gen_wlock():
                        self.pending_results.pop(job_id, None)
                else:
                    LOG.error(f"Could not connect to {address} to store result")
                    raise HTTPException(503, "Result destination not known")
            else:
                LOG.error(f"Destination for result of job {job_id} unknown")

    def datastore_for_storage(self, required_storage: int) -> Datastore:
        with self.datastore_listener.lock.gen_rlock():
            capable_datastores = []
            remove_list = []
            for datastore in self.datastore_listener.datastores.values():
                free_space = datastore.free_space()
                if free_space == -1:
                    remove_list.append(datastore)
                if free_space > required_storage:
                    capable_datastores.append(datastore)
        for datastore in remove_list:
            self.datastore_listener.remove_datastore(datastore.id)
        if len(capable_datastores) != 0:
            datastore = random.choice(capable_datastores)
            return datastore
        else:
            LOG.error(
                f"Could not find datastore able to hold file of size {required_storage}"
            )
            raise HTTPException(503, "No datastore able to hold file")

    def job_data_location(
        self, name: str, job_id: Optional[UUID] = None
    ) -> Optional[Datastore]:
        datastore = self.job_datastore_cache.get(name, job_id)
        if datastore is not None:
            return datastore
        return self.iter_data(
            name, job_id, lambda page, store: store if name in page.items else None
        )

    def iter_data(
        self,
        name: str,
        job_id: Optional[UUID],
        for_page: Callable[[Page, Datastore], Any],
    ) -> Any:
        with self.datastore_listener.lock.gen_rlock():
            datastores = [
                store for store in self.datastore_listener.datastores.values()
            ]
        for datastore in datastores:
            page_number = 0
            while True:
                page_number += 1
                if job_id:
                    page = datastore.paginate_data_list(
                        job_id=job_id, page_number=page_number
                    )
                    if len(page.items) != 0:
                        self.job_datastore_cache.set(page, datastore, job_id=job_id)
                else:
                    page = datastore.paginate_data_list(name, page_number=page_number)
                    if len(page.items) != 0:
                        self.job_datastore_cache.set(page, datastore, name)
                res = for_page(page, datastore)
                if res:
                    return res
                else:
                    if len(page.items) == 0:
                        break
        return None

    def add_pending_job(self, job_id: UUID, job_info: JobInfo):
        with self.results_lock.gen_wlock():
            self.pending_results[job_id] = job_info.result_addr

    def delete_job_data(self, job_id: UUID):
        with self.datastore_listener.lock.gen_rlock():
            for store in self.datastore_listener.datastores.values():
                store.delete_job_data(job_id)
