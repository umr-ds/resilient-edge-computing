import asyncio
from queue import Queue

import msgpack

from rec.dtn.messages import *
from rec.dtn.node import Node
from rec.util.log import LOG


class Broker(Node):
    completed_jobs: set
    queued_jobs: Queue

    discovered_nodes: dict[NodeType, set[EID]]

    def __init__(self, node_id: str | EID, dtn_agent_socket: str) -> None:
        super().__init__(
            node_id=node_id,
            dtn_agent_socket=dtn_agent_socket,
            node_type=NodeType.BROKER,
            _broker=node_id,
        )

        self.completed_jobs = set()
        self.queued_jobs = Queue()

        self.discovered_nodes = {
            NodeType.BROKER: set(),
            NodeType.CLIENT: set(),
            NodeType.EXECUTOR: set(),
            NodeType.DATASTORE: set(),
        }

    @override
    async def run(self) -> None:
        await self._register()

        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._announce_yourself())
            tg.create_task(self._handle_bundle_messages())
            tg.create_task(self._schedule_jobs())

    async def _announce_yourself(self) -> None:
        LOG.info("Starting announcer")

        while True:
            LOG.debug("Announcer going to sleep")
            await asyncio.sleep(10)

            announcement = BundleData(
                type=BundleType.BROKER_ANNOUNCE,
                node_type=NodeType.BROKER,
                source=self.node_id,
                destination=BROADCAST_ADDRESS,
            )

            try:
                LOG.debug("Sending announcement")
                dtnd_reply = await self._send_bundle(bundle=announcement)
                if not dtnd_reply.success:
                    LOG.error(f"Error sending bundle: {dtnd_reply.error}")
            except Exception as err:
                LOG.exception("Error sending bundle: %s", err)

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
                    for bundle in bundles:
                        await self._handle_bundle(bundle=bundle)
                else:
                    LOG.debug("No new bundles")
            except Exception as err:
                LOG.exception("Error fetching bundles: %s", err)

    async def _handle_bundle(self, bundle: BundleData) -> None:
        reply: BundleData | None
        if bundle.type == BundleType.JOB_QUERY:
            reply = await self._handle_job_query(bundle=bundle)
        elif BundleType.BROKER_ANNOUNCE <= bundle.type <= BundleType.BROKER_ACK:
            reply = await self._handle_discovery(bundle=bundle)
        else:
            LOG.warning(f"Won't handle bundle of type: {bundle.type}")
            return

        if reply is None:
            return

        try:
            dtnd_reply = await self._send_bundle(bundle=reply)
            if not dtnd_reply.success:
                LOG.error("dtnd replied with error: %s", dtnd_reply.error)
        except Exception as err:
            LOG.exception("Error creating bundle: %s", err)

    async def _handle_job_query(self, bundle: BundleData) -> BundleData:
        LOG.debug("Handling jobs query")
        async with self._state_mutex.reader_lock:
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

            return bundle_response

    @override
    async def _handle_discovery(self, bundle: BundleData) -> BundleData | None:
        LOG.debug("Handling discovery")

        async with self._state_mutex.writer_lock:
            match bundle.type:
                case BundleType.BROKER_ANNOUNCE:
                    if bundle.source != self.node_id:
                        LOG.debug(
                            f"Received announcement from other broker: {bundle.source}"
                        )
                    return None
                case BundleType.BROKER_REQUEST:
                    LOG.debug("Broker request")
                    async with self._state_mutex.writer_lock:
                        self.discovered_nodes[bundle.node_type].add(bundle.source)
                        LOG.info(
                            f"Discovered node {bundle.source} of type {bundle.node_type}"
                        )
                        return BundleData(
                            type=BundleType.BROKER_ACK,
                            source=self.node_id,
                            destination=bundle.source,
                        )
                case _:
                    LOG.warning(f"Won't handle bundle of type {bundle.type}")
                    return None

    async def _schedule_jobs(self) -> None:
        LOG.info("Starting job scheduler")
        while True:
            LOG.debug("Job scheduler going to sleep")
            await asyncio.sleep(10)

            async with self._state_mutex.writer_lock:
                LOG.debug("Running Job scheduler")
