import threading
from typing import Any, Union
from uuid import UUID

from fastapi import FastAPI

from wasm_rest.model import NodeRole
from wasm_rest.nodes.brokers.databroker import DataBroker
from wasm_rest.nodes.brokers.executorbroker import ExecutorBroker
from wasm_rest.nodes.node import Node
from wasm_rest.util.log import LOG


fastapi_app = FastAPI()
node_object: Node

data_broker = DataBroker()
executor_broker = ExecutorBroker(data_broker.add_pending_job)


@fastapi_app.delete("/job/{job_id}")
def delete_job(job_id: UUID) -> None:
    LOG.debug(f"deleting job {job_id}")
    data_broker.delete_job_data(job_id)
    executor_broker.delete_job_from_executor(job_id)


def run(host: Union[str, list[str]], port: int, uvicorn_args: dict[str, Any] = None) -> NodeRole:
    global node_object
    data_broker.add_endpoints(fastapi_app)
    executor_broker.add_endpoints(fastapi_app)
    threading.Thread(target=executor_broker.job_scheduler, daemon=True, name="broker scheduler").start()
    node_object = Node(host, port, "broker", fastapi_app, uvicorn_args)
    node_object.add_service_listener(Node.zeroconf_service_type("datastore"), data_broker.datastore_listener)
    node_object.run()
    return NodeRole.EXIT


if __name__ == '__main__':
    run(["127.0.0.1"], 8000)
