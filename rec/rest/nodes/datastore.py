import os
import threading
from enum import Enum
from typing import IO, Any, Optional
from uuid import UUID

import psutil
from fastapi import HTTPException, UploadFile
from fastapi.responses import FileResponse
from fastapi_pagination import Page, add_pagination, paginate
from readerwriterlock.rwlock import RWLockWrite

from rec.rest.model import NodeRole
from rec.rest.nodes.node import Node
from rec.rest.nodes.zeroconf_listeners.datastores import DatastoreListener
from rec.util.fs import prevent_breakout
from rec.util.log import LOG


class FileStatus(Enum):
    CREATED = 0
    ERROR = -1
    INTERRUPTED = 1


class File:
    path: str
    int_lock: threading.Lock
    write_lock: RWLockWrite
    interrupt: int = 0
    deleted: bool = False

    def __init__(self, path: str):
        self.path = path
        self.int_lock = threading.Lock()
        self.write_lock = RWLockWrite()

    def store(self, file: IO[bytes]) -> FileStatus:
        with self.int_lock:
            self.interrupt += 1
        with self.write_lock.gen_wlock():
            self.deleted = False
            with self.int_lock:
                self.interrupt -= 1
                if self.interrupt != 0:
                    return FileStatus.INTERRUPTED
            try:
                os.makedirs(os.path.dirname(self.path), exist_ok=True)
                with open(self.path, "bw") as o_file:
                    data = file.read(65536)
                    while len(data) != 0 and self.interrupt == 0:
                        o_file.write(data)
                        data = file.read(65536)
                    if self.interrupt != 0:
                        return FileStatus.INTERRUPTED
            except OSError:
                try:
                    os.remove(self.path)
                except OSError:
                    pass
                return FileStatus.ERROR
            return FileStatus.CREATED

    def read_lock(self):
        return self.write_lock.gen_rlock()

    def delete(self) -> bool:
        with self.write_lock.gen_wlock():
            try:
                os.remove(self.path)
            except OSError:
                return False
            self.deleted = True
            return True


class Datastore(Node):
    root_dir: str
    datastore_listener: DatastoreListener
    data_files: dict[str, File]
    stored_data: dict[str, str]
    data_lock: RWLockWrite

    def __init__(
        self,
        host: list[str],
        port: int,
        rootdir: str,
        uvicorn_args: Optional[dict[str, Any]] = None,
    ):
        super().__init__(host, port, "datastore", uvicorn_args)
        self.root_dir = rootdir
        self.data_files = {}
        self.stored_data = {}
        self.datastore_listener = DatastoreListener()
        self.data_lock = RWLockWrite()

    def add_endpoints(self):
        @self.fastapi_app.put("/data/{name:path}")
        def store_data(name: str, data: UploadFile) -> bool:
            LOG.debug(f"Storing data {name}")
            if psutil.disk_usage(self.root_dir).free < data.size:
                LOG.error("Tried to create to big file")
                raise HTTPException(500, "Not Enough Space")
            file_path = os.path.join(self.root_dir, prevent_breakout(name))
            with self.data_lock.gen_wlock():
                file = self.data_files.get(name, None)
                if file is None:
                    file = File(file_path)
                    self.data_files[name] = file
            error = file.store(data.file)
            if error is FileStatus.CREATED:
                with self.data_lock.gen_wlock():
                    self.stored_data[name] = file_path
                    return True
            elif error is FileStatus.INTERRUPTED:
                LOG.debug(f"Interrupted upload of {name}")
                return False
            else:
                LOG.error(f"Failed to create {name}")
                raise HTTPException(500, "Resource could not be created")

        @self.fastapi_app.get("/data/{name:path}")
        def get_data(name: str) -> FileResponse:
            LOG.debug(f"Retrieving data {name}")
            with self.data_lock.gen_rlock():
                if name in self.stored_data.keys():
                    file = self.data_files.get(name, None)
                else:
                    file = None
            if file:
                with file.read_lock():
                    return FileResponse(path=file.path, filename=os.path.basename(name))
            LOG.error(f"Could not find {name}")
            raise HTTPException(404, "Resource Not Found")

        @self.fastapi_app.delete("/data/{name:path}")
        def delete_data(name: str) -> None:
            LOG.debug(f"Deleting data {name}")
            if not self._delete_data(name):
                LOG.error(f"Could not find {name}")
                raise HTTPException(404, "No data of that name")

        @self.fastapi_app.delete("/job_data/{job_id}")
        def delete_job_data(job_id: UUID) -> None:
            LOG.debug(f"Deleting job {job_id}")
            with self.data_lock.gen_wlock():
                for data in [
                    data
                    for data in self.stored_data
                    if data.startswith(str(job_id)) and not data.endswith("/result")
                ]:
                    self._delete_data(data)

        @self.fastapi_app.get("/list")
        def data_list() -> list[str]:
            LOG.debug("Listing all data")
            with self.data_lock.gen_rlock():
                return list(self.stored_data.keys())

        @self.fastapi_app.get("/list/{name:path}")
        def paginate_data(
            name: Optional[str] = "", job_id: Optional[UUID] = None
        ) -> Page[str]:
            LOG.debug("paginating data")
            job_id = str(job_id) if job_id else ""
            with self.data_lock.gen_rlock():
                return paginate(
                    [
                        data_name
                        for data_name in self.stored_data.keys()
                        if data_name.startswith(job_id) and data_name.startswith(name)
                    ]
                )

        @self.fastapi_app.get("/free")
        def free_space() -> int:
            LOG.debug("Sending free space")
            os.makedirs(self.root_dir, exist_ok=True)
            return psutil.disk_usage(self.root_dir).free

        add_pagination(self.fastapi_app)

    def _delete_data(self, name: str) -> bool:
        path = self.stored_data.pop(name, None)
        file = self.data_files.pop(name, None)
        if path:
            try:
                file.delete()
                os.removedirs(os.path.dirname(path))
                return True
            except OSError:
                pass
        return False

    def run(self) -> NodeRole:
        try:
            os.makedirs(self.root_dir, exist_ok=True)
        except OSError:
            return NodeRole.EXIT
        self.add_service_listener(
            Node.zeroconf_service_type("datastore"), self.datastore_listener
        )
        self.do_run()
        return NodeRole.EXIT


if __name__ == "__main__":
    Datastore(["127.0.0.1"], 8002, "../../datastore.d").run()
