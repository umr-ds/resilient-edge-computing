import threading
from typing import Any, Optional
from uuid import UUID

from rec.rest.model import NodeRole
from rec.rest.nodes.brokers.databroker import DataBroker
from rec.rest.nodes.brokers.executorbroker import ExecutorBroker
from rec.rest.nodes.node import Node
from rec.util.log import LOG


class Broker(Node):
    data_broker: DataBroker
    executor_broker: ExecutorBroker

    def __init__(
        self, host: list[str], port: int, uvicorn_args: Optional[dict[str, Any]] = None
    ):
        super().__init__(host, port, "broker", uvicorn_args)
        self.data_broker = DataBroker()
        self.executor_broker = ExecutorBroker(self.data_broker.add_pending_job)
        self.data_broker.add_endpoints(self.fastapi_app)
        self.executor_broker.add_endpoints(self.fastapi_app)

    def add_endpoints(self):
        @self.fastapi_app.delete("/job/{job_id}")
        def delete_job(job_id: UUID) -> None:
            LOG.debug(f"deleting job {job_id}")
            self.data_broker.delete_job_data(job_id)
            self.executor_broker.delete_job_from_executor(job_id)

    def run(self) -> NodeRole:
        threading.Thread(
            target=self.executor_broker.job_scheduler,
            daemon=True,
            name="broker scheduler",
        ).start()
        self.add_service_listener(
            Node.zeroconf_service_type("datastore"), self.data_broker.datastore_listener
        )
        self.do_run()
        return NodeRole.EXIT

    def stop(self):
        super().stop()
        self.executor_broker.stop()


if __name__ == "__main__":
    Broker(["127.0.0.1"], 8000).run()
