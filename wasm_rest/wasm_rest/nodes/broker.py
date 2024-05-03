from typing import Any
from uuid import UUID

from fastapi import FastAPI

from wasm_rest.model import NodeRole
from wasm_rest.nodes.brokers.databroker import DataBroker
from wasm_rest.nodes.brokers.executorbroker import ExecutorBroker
from wasm_rest.nodes.node import Node

fastapi_app = FastAPI()
node_object: Node

data_broker = DataBroker()
executor_broker = ExecutorBroker(data_broker.add_pending_job)


@fastapi_app.delete("/job/{job_id}")
def delete_job(job_id: UUID) -> None:
    data_broker.delete_job_data(job_id)
    executor_broker.delete_job_from_executor(job_id)


def run(host: str, port: int, uvicorn_args: dict[str, Any] = None) -> NodeRole:
    global node_object
    data_broker.add_endpoints(fastapi_app)
    executor_broker.add_endpoints(fastapi_app)
    node_object = Node(host, port, "broker", fastapi_app, uvicorn_args)
    node_object.zeroconf.add_service_listener(Node.zeroconf_service_type("datastore"), data_broker.datastore_listener)
    node_object.run()
    return NodeRole.EXIT


if __name__ == '__main__':
    run("127.0.0.1", 8000)
