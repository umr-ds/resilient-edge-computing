import os
import threading
from typing import Any, Optional
from uuid import UUID

import psutil
from fastapi import FastAPI, UploadFile, HTTPException
from fastapi.responses import FileResponse
from fastapi_pagination import Page, add_pagination, paginate

from wasm_rest.model import NodeRole
from wasm_rest.nodes.node import Node
from wasm_rest.util.util import prevent_breakout, put_file

fastapi_app = FastAPI()
node_object: Node

root_dir: str = ""
stored_data: dict[str, str] = {}
data_lock = threading.Lock()


@fastapi_app.put("/data/{name:path}")
def store_data(name: str, data: UploadFile) -> None:
    if psutil.disk_usage(root_dir).free < data.size:
        raise HTTPException(500, "Not Enough Space")
    file_path = os.path.join(root_dir, prevent_breakout(name))
    if put_file(data.file, file_path):
        with data_lock:
            stored_data[name] = file_path
    else:
        raise HTTPException(500, "Resource could not be created")


@fastapi_app.get("/data/{name:path}")
def get_data(name: str) -> FileResponse:
    with data_lock:
        if name in stored_data.keys():
            return FileResponse(path=stored_data[name], filename=os.path.basename(name))
    raise HTTPException(404, "Resource Not Found")


@fastapi_app.delete("/data/{name:path}")
def delete_data(name: str) -> None:
    if not _delete_data(name):
        raise HTTPException(404, "No data of that name")


@fastapi_app.delete("/job_data/{job_id}")
def delete_job_data(job_id: UUID) -> None:
    remove_list = []
    with data_lock:
        for data in stored_data:
            if data.startswith(str(job_id)) and not data.endswith("/result"):
                remove_list.append(data)
        for data in remove_list:
            _delete_data(data)


@fastapi_app.get("/list")
def data_list() -> list[str]:
    with data_lock:
        return list(stored_data.keys())


@fastapi_app.get("/list/{name:path}")
def paginate_data(name: Optional[str] = '', job_id: Optional[UUID] = None) -> Page[str]:
    job_id = str(job_id) if job_id else ''
    with data_lock:
        return paginate(
            [data_name for data_name in stored_data.keys() if data_name.startswith(job_id) and name in data_name])


@fastapi_app.get("/free")
def free_space() -> int:
    os.makedirs(root_dir, exist_ok=True)
    return psutil.disk_usage(root_dir).free


add_pagination(fastapi_app)


def _delete_data(name: str) -> bool:
    path = stored_data.pop(name, None)
    if path:
        try:
            os.remove(path)
            os.removedirs(os.path.dirname(path))
            return True
        except OSError:
            pass
    return False


def run(host: str, port: int, rootdir: str, uvicorn_args: dict[str, Any] = None) -> NodeRole:
    global node_object, root_dir
    node_object = Node(host, port, "datastore", fastapi_app, uvicorn_args)
    root_dir = rootdir
    try:
        os.makedirs(root_dir, exist_ok=True)
    except OSError:
        return NodeRole.EXIT

    node_object.run()
    return NodeRole.EXIT


if __name__ == '__main__':
    run("127.0.0.1", 8002, "../../datastore.d")
