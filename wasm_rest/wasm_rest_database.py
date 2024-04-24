import json
import os
import socket
import argparse

import psutil

from wasm_rest.server.database import Database
from wasm_rest.util import put_file, gen_unique_id, prevent_break
from wasm_rest.model import Address, Node

from fastapi import FastAPI, UploadFile, HTTPException
from fastapi.responses import FileResponse
from zeroconf import Zeroconf, ServiceInfo
from uvicorn import Server as UVServer, Config as UVConfig

app: FastAPI = FastAPI()
uv_server: UVServer = None
zeroconf: Zeroconf = Zeroconf()

root_dir: str = ""
stored_data: dict[str, str] = {}

self_object: Database


@app.put("/data/{name:path}")
def store_data(name: str, data: UploadFile):
    file_path = os.path.join(root_dir, prevent_break(name))
    if put_file(data.file, file_path):
        stored_data[name] = file_path
        on_content_update()
    else:
        raise HTTPException(500, "Resource could not be created")


@app.get("/data/{name:path}")
def get_data(name: str) -> FileResponse:
    if name in stored_data.keys():
        return FileResponse(path=stored_data[name], filename=name)
    raise HTTPException(404, "Resource Not Found")


@app.delete("/data/{name:path}")
def delete_data(name: str):
    del stored_data[name]
    os.remove(os.path.join(root_dir, prevent_break(name)))
    on_content_update()


@app.get("/list")
def get_data_list():
    return list(stored_data.keys())


@app.get("/free")
def free_space():
    return psutil.disk_usage(root_dir).free


def gen_service_info() -> ServiceInfo:
    return ServiceInfo(
        "_database_wasm-rest._tcp.local.",
        f"_database{self_object.node.id}._wasm-rest._tcp.local.",
        addresses=[socket.inet_aton(self_object.node.address.host)],
        port=int(self_object.node.address.port),
        server=f"_database{self_object.node.id}._wasm-rest._tcp.local.",
        properties={"node_id": self_object.node.id, "data": json.dumps(list(stored_data.keys())), "free_storage": free_space()}
    )


def valid_resource_name(name: str) -> bool:
    return not name.startswith("/result/")


def on_content_update():
    zeroconf.update_service(gen_service_info())


def wasm_rest_parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cert", help="server cert file")
    parser.add_argument("--key", help="server key file")
    parser.add_argument("--rootdir", default="wasm_rest_database", help="root for the wasm rest file tree")
    parser.add_argument("--addr", default="127.0.0.1:8002", help="address of this server as host:port")
    return parser.parse_args()


def main():
    global uv_server, root_dir, self_object
    args = wasm_rest_parse_args()
    os.makedirs(args.rootdir, exist_ok=True)
    root_dir = args.rootdir
    host, port = args.addr.split(":")
    self_object = Database(node=Node(id=gen_unique_id(), address=Address(host=host, port=port)),
                           free_storage=free_space(), data_names=set())
    info = gen_service_info()
    uv_server = UVServer(UVConfig(app, host="127.0.0.1", port=8002))
    zeroconf.register_service(info)
    uv_server.run()
    zeroconf.unregister_service(gen_service_info())


if __name__ == '__main__':
    main()
