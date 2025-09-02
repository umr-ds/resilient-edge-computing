import os
from typing import IO, Optional
from uuid import UUID
from zipfile import ZipFile

from rec.rest.nodetypes.broker import Broker


def zip_folder(zip_file: ZipFile, host: str, to_zip: str) -> None:
    for root, _, files in os.walk(host):
        for file in files:
            relpath = os.path.relpath(os.path.join(root, file), host)
            zip_file.write(os.path.join(root, file), os.path.join(to_zip, relpath))


def prevent_breakout(path: str) -> str:
    path = path.replace("\\", "/")
    if path == "/":
        return ""
    check_path = path[:-1] if path.endswith("/") else path
    if (
        os.path.normpath(check_path).replace("\\", "/") != check_path
        or os.path.expanduser(check_path) != check_path
        or os.path.expandvars(check_path) != check_path
    ):  # TODO prevent breaking out
        raise ValueError(f"Path {path} was incorrectly formatted")
    if path.startswith("/"):
        path = path[1:]
    return path


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


def try_store_named_data(name: str, path: str, broker: Broker) -> bool:
    try:
        with open(path, "br") as result_file:
            return broker.store_data(result_file, name)
    except OSError:
        return False


def try_download_file(
    name: str, path: str, broker: Broker, job_id: Optional[UUID] = None
) -> bool:
    if os.path.exists(path):
        return True
    with open(path, "bw") as data_file:
        if not broker.get_data(data_file, name, job_id):
            try:
                os.remove(path)
            except OSError:
                pass  # cleanup if failed
            return False
        return True
