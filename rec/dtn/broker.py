import asyncio
from queue import Queue

from rec.dtn.messages import *
from rec.dtn.node import Node
from rec.util.log import LOG


class Broker(Node):
    completed_jobs: set
    queued_jobs: Queue

    def __init__(self, node_id: str, dtn_agent_socket: str):
        super().__init__(node_id=node_id, dtn_agent_socket=dtn_agent_socket)
        self.completed_jobs = set()
        self.queued_jobs = Queue()

    @override
    async def run(self) -> None:
        message = Register(Type=MessageType.REGISTER, EndpointID=self.node_id)

        try:
            reply = await self._send_message(message=message)
        except FileNotFoundError as err:
            LOG.critical("Error connecting to dtnd: %s", err, exc_info=True)
            return

        if not reply.Success:
            LOG.critical("Error registering with dtnd: %s", reply.Error)
            return

        async with asyncio.TaskGroup() as tg:
            bundle_handler = tg.create_task(self._handle_bundle_messages())
            job_scheduler = tg.create_task(self._schedule_jobs())

    async def _handle_bundle_messages(self) -> None:
        LOG.info("Starting bundle handler")
        while True:
            LOG.debug("Bundle handler going to sleep")
            await asyncio.sleep(10)

            LOG.debug("Running bundle handler")
            try:
                LOG.debug("Retrieving bundles")
                bundles = await self._get_new_bundles()
                if bundles:
                    LOG.debug(f"Bundles: {bundles}")
                else:
                    LOG.debug("No new bundles")
            except Exception as err:
                LOG.exception("Error fetching bundles: %s", err)

    async def _get_new_bundles(self) -> list[BundleData]:
        LOG.debug("Retrieving new bundles")
        bundles: list[BundleData] = []

        message = Fetch(
            Type=MessageType.FETCH, EndpointID=self.node_id, NodeType=NodeType.BROKER
        )
        reply = await self._send_message(message=message)

        assert isinstance(reply, FetchReply)

        if reply.Success:
            bundles = reply.Bundles
        else:
            LOG.error("dtnd replied with error: %s", reply.Error)

        return bundles

    async def _schedule_jobs(self) -> None:
        LOG.info("Starting job scheduler")
        while True:
            LOG.debug("Job scheduler going to sleep")
            await asyncio.sleep(10)

            LOG.debug("Running Job scheduler")
