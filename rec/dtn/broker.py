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

    def __init__(self, node_id: str | EID, dtn_agent_socket: str):
        super().__init__(node_id=node_id, dtn_agent_socket=dtn_agent_socket)
        self.state_mutex = asyncio.Lock()
        self.completed_jobs = set()
        self.queued_jobs = Queue()

        self.bundle_handlers = {
            BundleType.JOB_QUERY: self._handle_job_query,
        }

    @override
    async def run(self) -> None:
        await self._register()

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
        if bundle.type not in self.bundle_handlers:
            LOG.error(f"Don't know how to handle bundle of type: {bundle.type}")
            return

        await self.bundle_handlers[bundle.type](bundle)

    async def _handle_job_query(self, bundle: BundleData) -> None:
        LOG.debug("Handling jobs query")
        async with self.state_mutex:
            jobs = {
                "completed": list(self.completed_jobs),
                "queued": list(self.queued_jobs.queue),
            }
            jobs_bytes = msgpack.packb(jobs)
            bundle_response = BundleData(
                type=BundleType.JOB_LIST,
                source=self.node_id,
                destination=bundle.source,
                submitter=bundle.submitter,
                payload=jobs_bytes,
            )
            LOG.debug(f"Response bundle: {bundle_response}")

        message = BundleCreate(type=MessageType.CREATE, bundle=bundle_response)
        try:
            dtnd_reply = await self._send_message(message=message)
            if not dtnd_reply.success:
                LOG.error("dtnd replied with error: %s", dtnd_reply.error)
        except Exception as err:
            LOG.exception("Error creating bundle: %s", err)

    async def _schedule_jobs(self) -> None:
        LOG.info("Starting job scheduler")
        while True:
            LOG.debug("Job scheduler going to sleep")
            await asyncio.sleep(10)

            async with self.state_mutex:
                LOG.debug("Running Job scheduler")
