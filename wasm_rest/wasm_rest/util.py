import os
import time

from zipfile import ZipFile

import requests

from .model import Server
from fastapi import HTTPException
from typing import IO


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


def prevent_break(path: str) -> str:
    if path == "/":
        return ""
    check_path = path[:-1] if path.endswith("/") else path
    if os.path.normpath(check_path) != check_path or os.path.expanduser(check_path) != check_path or \
            os.path.expandvars(check_path) != check_path:  # TODO prevent breaking out
        raise HTTPException(400, f"Path {path} was incorrectly formatted")
    if path.startswith("/"):
        path = path[1:]
    return path


def zip_folder(zip_file: ZipFile, host: str, to_zip: str):
    for root, _, files in os.walk(host):
        for file in files:
            relpath = os.path.relpath(os.path.join(root, file), host)
            zip_file.write(os.path.join(root, file),
                           os.path.join(to_zip, relpath))


def wait_online(server: Server, endpoint: str, max_tries: int, wait_next_try: float) -> bool:
    tries = 0
    while True:
        try:
            requests.put(f"http://{server.host}:{server.port}/{endpoint}")
            return True
        except requests.exceptions.RequestException:
            if tries >= max_tries:
                return False
            tries += 1
        time.sleep(wait_next_try)
