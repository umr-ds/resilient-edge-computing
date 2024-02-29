import json
import os
import socket
import argparse

import psutil

from wasm_rest.util import put_file, gen_node_id, prevent_break
from wasm_rest.model import Server

from fastapi import FastAPI, UploadFile, HTTPException
from fastapi.responses import FileResponse
from zeroconf import Zeroconf, ServiceInfo
from uvicorn import Server as UVServer, Config as UVConfig

app: FastAPI = FastAPI()
uv_server: UVServer = None
zeroconf: Zeroconf = Zeroconf()

node_id = gen_node_id()
address: Server = None

root_dir: str = ""
stored_data: dict[str, str] = {}


@app.put("/data/{name}")
def store_data(name: str, data: UploadFile):
    file_path = os.path.join(root_dir, prevent_break(name))
    if put_file(data.file, file_path):
        stored_data[name] = file_path
        on_content_update()
    else:
        raise HTTPException(500, "Resource could not be created")


@app.get("/data/{name}")
def get_data(name: str) -> FileResponse:
    if name in stored_data.keys():
        return FileResponse(path=stored_data[name], filename=name)
    raise HTTPException(404, "Resource Not Found")


@app.delete("/data/{name}")
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
        f"_database{node_id}._wasm-rest._tcp.local.",
        addresses=[socket.inet_aton(address.host)],
        port=int(address.port),
        server=f"_database{node_id}._wasm-rest._tcp.local.",
        properties={"node_id": node_id, "data": json.dumps(list(stored_data.keys())), "free_storage": free_space()}
    )


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
    global uv_server, root_dir, address
    args = wasm_rest_parse_args()
    os.makedirs(args.rootdir, exist_ok=True)
    root_dir = args.rootdir
    host, port = args.addr.split(":")
    address = Server(host=host, port=port)
    info = gen_service_info()
    uv_server = UVServer(UVConfig(app, host="127.0.0.1", port=8002))
    zeroconf.register_service(info)
    uv_server.run()
    zeroconf.unregister_service(gen_service_info())


if __name__ == '__main__':
    main()
