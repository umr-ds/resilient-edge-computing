import asyncio
import random
from queue import Queue
from typing import override

import msgpack

from rec.dtn.eid import BROADCAST_ADDRESS, EID
from rec.dtn.job import Job, JobInfo, JobResult, dictify_job_infos
from rec.dtn.messages import BundleData, BundleType, NodeType
from rec.dtn.node import Node
from rec.util.log import LOG


class Broker(Node):
    completed_jobs: set[JobInfo]
    queued_jobs: Queue[Job]

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
            tg.create_task(self._handle_bundles())
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

            await self._send_and_check(bundles=[announcement])

    async def _handle_bundles(self) -> None:
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
                    replies: list[BundleData] = []
                    for bundle in bundles:
                        reply = await self._handle_bundle(bundle=bundle)
                        if reply is not None:
                            replies.append(reply)
                    if replies:
                        await self._send_and_check(bundles=replies)
                else:
                    LOG.debug("No new bundles")
            except Exception as err:
                LOG.exception("Error fetching bundles: %s", err)

    async def _handle_bundle(self, bundle: BundleData) -> BundleData | None:
        reply: BundleData | None = None
        if bundle.type == BundleType.JOB_SUBMIT:
            await self._handle_job_submit(bundle=bundle)
        elif bundle.type == BundleType.JOB_RESULT:
            await self._handle_job_result(bundle=bundle)
        elif bundle.type == BundleType.JOB_QUERY:
            reply = await self._handle_job_query(bundle=bundle)
        elif BundleType.BROKER_ANNOUNCE <= bundle.type <= BundleType.BROKER_ACK:
            reply = await self._handle_discovery(bundle=bundle)
        else:
            LOG.warning(f"Won't handle bundle of type: {bundle.type}")

        return reply

    async def _handle_job_submit(self, bundle: BundleData) -> None:
        """
        Handle a job submission by forwarding it to an available executor.

        Args:
            bundle (BundleData): The job submission bundle.
        """
        LOG.debug("Handling job submission")

        job = Job.deserialize(bundle.payload)

        async with self._state_mutex.writer_lock:
            self.queued_jobs.put(job)

        LOG.info(f"Queued job: {job.metadata.job_id}")

        # TODO: ACK?

    async def _handle_job_result(self, bundle: BundleData) -> None:
        """
        Handle a job result by storing it in the completed jobs set.

        Args:
            bundle (BundleData): The job result bundle.
        """
        LOG.debug("Handling job result")

        job_result = JobResult.deserialize(bundle.payload)

        async with self._state_mutex.writer_lock:
            self.completed_jobs.add(job_result.metadata)

        LOG.info(f"Stored completed job: {job_result.metadata.job_id}")

    async def _handle_job_query(self, bundle: BundleData) -> BundleData:
        LOG.debug("Handling jobs query")
        async with self._state_mutex.reader_lock:
            queued_job_infos = [job.metadata for job in self.queued_jobs.queue]
            jobs = {
                "completed": dictify_job_infos(self.completed_jobs),
                "queued": dictify_job_infos(queued_job_infos),
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
                        self.discovered_nodes[NodeType.BROKER].add(bundle.source)
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
                            node_type=self.node_type,
                        )
                case _:
                    LOG.warning(f"Won't handle bundle of type {bundle.type}")
                    return None

    async def _schedule_jobs(self) -> None:
        LOG.info("Starting job scheduler")
        while True:
            LOG.debug("Job scheduler going to sleep")
            await asyncio.sleep(10)

            async with self._state_mutex.reader_lock:
                executors = self.discovered_nodes.get(NodeType.EXECUTOR, set())

            if not executors:
                LOG.info("No executors available to schedule jobs")
                continue

            async with self._state_mutex.writer_lock:
                if self.queued_jobs.empty():
                    LOG.debug("No jobs to schedule")
                    continue
                job = self.queued_jobs.get()

            # Randomly select an executor for rudimentary load balancing
            # TODO: Implement proper load balancing
            # TODO: Implement capabilities checking; this depends on executor capability announcements -> Heartbeats?
            executor = random.choice(list(executors))

            LOG.info(f"Scheduling job {job.metadata.job_id} on executor {executor}")

            job_submission = BundleData(
                type=BundleType.JOB_SUBMIT,
                source=self.node_id,
                destination=executor,
                payload=job.serialize(),
            )

            await self._send_and_check(bundles=[job_submission])

            LOG.info(f"Job {job.metadata.job_id} scheduled on executor {executor}")
