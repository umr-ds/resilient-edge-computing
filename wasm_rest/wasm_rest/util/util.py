import base64
import os
import random
import time
from io import BytesIO
from typing import IO
from zipfile import ZipFile

from wasm_rest.nodetypes.broker import Broker


def put_file(file: IO[bytes], path: str) -> bool:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "bw") as o_file:
            data = file.read(65536)
            while len(data) != 0:
                o_file.write(data)
                data = file.read(65536)
    except OSError:
        try:
            os.remove(path)
        except OSError:
            pass
        return False
    return True


def prevent_breakout(path: str) -> str:
    if path == "/":
        return ""
    check_path = path[:-1] if path.endswith("/") else path
    if os.path.normpath(check_path) != check_path or os.path.expanduser(check_path) != check_path or \
            os.path.expandvars(check_path) != check_path:  # TODO prevent breaking out
        raise ValueError(f"Path {path} was incorrectly formatted")
    if path.startswith("/"):
        path = path[1:]
    return path


def zip_folder(zip_file: ZipFile, host: str, to_zip: str) -> None:
    for root, _, files in os.walk(host):
        for file in files:
            relpath = os.path.relpath(os.path.join(root, file), host)
            zip_file.write(os.path.join(root, file),
                           os.path.join(to_zip, relpath))


def generate_unique_id() -> str:
    return base64.urlsafe_b64encode(random.randbytes(32)).decode()[:-1]


def try_store_named_data(name: str, path: str, broker: Broker) -> bool:
    result_size = os.stat(path).st_size
    datastore = broker.datastore_for_storage(result_size)
    if datastore is None:
        return False
    with open(path, "br") as result_file:
        return datastore.store_data(result_file, name)
    '''for _ in range(10):
        datastore = broker.datastore_for_storage(100)
        if datastore is None:
            time.sleep(10)
            continue
        datastore.store_data(BytesIO(b"Could not find datastore to store result: too big"), name)'''
    # TODO reasonable error


def try_download_file(name: str, path: str, broker: Broker, job_id: str = '', invalidate: bool = False) -> bool:
    if os.path.exists(path):
        return True
    datastore = broker.data_location(name, job_id, invalidate)
    if datastore is None:
        return False
    with open(path, "bw") as data_file:
        if not datastore.get_data(data_file, name):
            try:
                os.remove(path)
            except OSError:
                pass  # cleanup if failed
            return False
        return True
