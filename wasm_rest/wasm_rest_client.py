import json
import os
import argparse
import re
import logging
import sys
import time
import threading

from typing import cast
from zipfile import ZipFile, BadZipFile

from wasm_rest.client.server import Server
from wasm_rest.client.job import Job
from wasm_rest.exceptions import ServerException, WasmRestException, ClientException
from wasm_rest.model import Capabilities, Command, Address
from wasm_rest.util import wait_online
from wasm_rest.server.broker import Broker
from pydantic import ValidationError
from zeroconf import Zeroconf, ServiceBrowser, ServiceStateChange

import uvicorn

from contextlib import asynccontextmanager

from fastapi import FastAPI, UploadFile, HTTPException
from wasm_rest.util import put_file


results: list[str] = []
broker: Broker = None
brokers: list[Broker] = []
reply_server: Address = Address(host="localhost", port=8003)
pending_results: dict[str, Command] = {}
log = logging.getLogger("wasm_rest_client")
done: threading.Semaphore = threading.Semaphore(0)
uv_server: uvicorn.Server = None


def capable_server(caps: Capabilities) -> Server:
    executor = broker.get_executor(caps)
    if executor is None:
        raise ServerException("Failed to communicate with broker")
    return Server(executor)


def upload_data(host_path: str, server_path: str, job: Job):
    try:
        if not re.match('result\$\d+:.+', host_path) is None:
            res_num = int(re.search('\d+', host_path).group())
            res_path = re.search(':.+', host_path).group()[1:]
            if len(results) > res_num and not (results[res_num] is None):
                with ZipFile(results[res_num]) as zip_file:
                    with zip_file.open(res_path) as file:
                        return job.upload(file, server_path)
            else:
                if results[res_num] is None:
                    raise ClientException(f"Required previous execution failed for {host_path}")
                else:
                    raise ClientException(f"Index to previous executions out of bounds for {host_path}")
        else:
            with open(host_path, "br") as file:
                return job.upload(file, server_path)
    except OSError as e:
        raise ClientException(f"Error {os.strerror(e.errno)} with {host_path}")
    except ValueError:
        raise ClientException(f"Problem with result {host_path}")
    except BadZipFile:
        raise ClientException(f"Could not find zip for {host_path}")


def check_data_exists(data: str):
    if not re.match('result\$\d+:.+', data) is None:
        res_num = int(re.search('\d+', data).group())
        if len(results) <= res_num or results[res_num] is None:
            raise ClientException(f"Execution failed {data} references non existent result")
    else:
        if not os.path.isfile(data):
            raise ClientException(f"File {data} does not exist")


def run_cmd(command: Command):
    if len(command.stdin) > 1:
        raise ClientException("Only one stdin file is allowed")
    for data in command.stdin.keys():
        check_data_exists(data)
    for data in command.data.keys():
        check_data_exists(data)

    executor = capable_server(command.capabilities)
    if executor is None:
        raise ClientException("Could not find executor")
    log.info(f"Selected server {executor}")
    try:
        try:
            job = executor.submit(command.wasm_bin)
        except OSError as e:
            raise ClientException(f"Error {os.strerror(e.errno)} with {command.wasm_bin}")
        log.info("Uploaded binary")

        stdin_server = ""
        for host_path, server_path in command.stdin.items():
            if host_path != "local":
                upload_data(host_path, server_path, job)
            stdin_server = server_path

        for host_path, server_path in command.data.items():
            upload_data(host_path, server_path, job)
        log.info("Uploaded data")

        if command.result_addr.host == "":
            job.run(stdin_server, command.args, command.env, command.ndn_data, command.results, command.capabilities, command.result_addr)
            while True:
                time.sleep(1)
                log.info(f"polling {command.wasm_bin}")
                result_id = job.result(command.result_path)
                if result_id is not None:
                    return result_id
        else:
            pending_results[job.job_id] = command
            ensure_online()
            job.run(stdin_server, command.args, command.env, command.ndn_data, command.results, command.capabilities, command.result_addr)

    except ServerException as e:
        raise ClientException("Selected server went offline or encountered an error" if e.msg == "" else e.msg)


def found_broker(
        zeroconf: Zeroconf, service_type: str, name: str, state_change: ServiceStateChange
) -> None:
    if state_change is ServiceStateChange.Added:
        info = zeroconf.get_service_info(service_type, name)
        if info:
            addresses = info.parsed_scoped_addresses()
            brokers.append(Broker(host=addresses[0], port=cast(int, info.port)))


def select_broker():
    global broker
    zeroconf = Zeroconf()
    services = ["_broker._tcp.local."]
    browser = ServiceBrowser(zeroconf, services, handlers=[found_broker])
    time.sleep(10)
    browser.cancel()
    if not len(brokers):
        raise WasmRestException("Could not find broker")
    broker = brokers[0]


def main():
    global reply_server, done
    parser = argparse.ArgumentParser()
    parser.add_argument("json")
    parser.add_argument("--reply", default="localhost:8003",
                        help="address of the reply server as host:port (if required)")
    parser.add_argument("-v", action="store_true")
    args = parser.parse_args()
    host, port = args.reply.split(":")
    reply_server = Address(host=host, port=port)
    select_broker()
    if args.v:
        log.setLevel(logging.INFO)
        log.addHandler(logging.StreamHandler(sys.stdout))

    if os.path.isfile(args.json):
        cmds = []
        with open(args.json, "r") as file:
            cmds = json.load(file)
        for cmd in cmds:
            result = None
            try:
                result = run_cmd(Command.model_validate(cmd))
            except ValidationError:
                print("Invalid Command")
            except ClientException as e:
                print(e.msg)
            results.append(result)


@asynccontextmanager
async def lifespan(application: FastAPI):
    yield


app = FastAPI(lifespan=lifespan)


def ensure_online():
    global uv_server
    if uv_server is None or not wait_online(reply_server, "result", 2, 1):
        uv_server = uvicorn.Server(uvicorn.Config(app, host=reply_server.host, port=reply_server.port))
        threading.Thread(target=uv_server.run).start()
        wait_online(reply_server, "result", 16, 1)


@app.put("/result/{job_id}")
def put_result(job_id: str, data: UploadFile):
    try:
        cmd = pending_results.pop(job_id)
        if put_file(data.file, os.path.join(cmd.result_path, data.filename)):
            log.info("result arrived")
    except KeyError:
        raise HTTPException(404, "Result not expected")


if __name__ == "__main__":
    main()
