import asyncio

from queue import Queue
from typing import Callable, Coroutine

import msgpack

from rec.dtn.messages import *
from rec.dtn.node import Node
from rec.util.log import LOG


class Broker(Node):
    state_mutex: asyncio.Lock
    completed_jobs: set
    queued_jobs: Queue
    bundle_handlers: dict[BundleType, Callable[[BundleData], Coroutine]]

    def __init__(self, node_id: str, dtn_agent_socket: str):
        super().__init__(node_id=node_id, dtn_agent_socket=dtn_agent_socket)
        self.state_mutex = asyncio.Lock()
        self.completed_jobs = set()
        self.queued_jobs = Queue()

        self.bundle_handlers = {
            BundleType.JOBS_QUERY: self._handle_jobs_query,
        }

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
                bundles = await self._get_new_bundles(NodeType.BROKER)
                if bundles:
                    LOG.debug(f"Bundles: {bundles}")
                    for bundle in bundles:
                        await self._handle_bundle(bundle=bundle)
                else:
                    LOG.debug("No new bundles")
            except Exception as err:
                LOG.exception("Error fetching bundles: %s", err)

    async def _handle_bundle(self, bundle: BundleData) -> None:
        if bundle.Type not in self.bundle_handlers:
            LOG.error("Don't know how to handle bundle")
            return

        await self.bundle_handlers[bundle.Type](bundle)

    async def _handle_jobs_query(self, bundle: BundleData) -> None:
        LOG.debug("Handling jobs query")
        async with self.state_mutex:
            jobs = {
                "completed": list(self.completed_jobs),
                "queued": list(self.queued_jobs.queue),
            }
            jobs_bytes = msgpack.packb(jobs)
            bundle_response = BundleData(
                Type=BundleType.JOBS_REPLY,
                Source=self.node_id,
                Destination=bundle.Source,
                Metadata=bundle.Metadata,
                Payload=jobs_bytes,
            )

        message = BundleCreate(Type=MessageType.CREATE, Bundle=bundle_response)
        try:
            dtnd_reply = await self._send_message(message=message)
            if not dtnd_reply.Success:
                LOG.error("dtnd replied with error: %s", dtnd_reply.Error)
        except Exception as err:
            LOG.exception("Error creating bundle: %s", err)

    async def _schedule_jobs(self) -> None:
        LOG.info("Starting job scheduler")
        while True:
            LOG.debug("Job scheduler going to sleep")
            await asyncio.sleep(10)

            async with self.state_mutex:
                LOG.debug("Running Job scheduler")
