import asyncio
import random
from pathlib import Path
from typing import override

from ormsgpack import packb

from rec.dtn.eid import BROADCAST_ADDRESS, EID
from rec.dtn.job import Job, JobInfo, JobResult, dictify_job_infos
from rec.dtn.messages import BundleData, BundleType, NodeType
from rec.dtn.node import Node
from rec.util.log import LOG


class Broker(Node):
    _completed_jobs: set[JobInfo]
    _queued_jobs: list[Job]

    _queued_ndata: list[BundleData]

    _queue_cv: asyncio.Condition

    _discovered_nodes: dict[NodeType, set[EID]]

    def __init__(self, node_id: EID, dtn_agent_socket: Path) -> None:
        super().__init__(
            _node_id=node_id,
            _dtn_agent_socket=dtn_agent_socket,
            _node_type=NodeType.BROKER,
            _broker=node_id,
        )

        self._completed_jobs = set()
        self._queued_jobs = []

        self._queued_ndata = []

        self._queue_cv = asyncio.Condition()

        self._discovered_nodes = {
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
            tg.create_task(self._queue_worker())

    async def _announce_yourself(self) -> None:
        LOG.info("Starting announcer")

        while True:
            LOG.debug("Announcer going to sleep")
            await asyncio.sleep(10)

            announcement = BundleData(
                type=BundleType.BROKER_ANNOUNCE,
                node_type=NodeType.BROKER,
                source=self._node_id,
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
        elif bundle.type == BundleType.NDATA_PUT:
            await self._handle_ndata_put(bundle=bundle)
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

        async with self._state_mutex.reader_lock:
            executors = self._discovered_nodes.get(NodeType.EXECUTOR, set())

        if not executors:
            LOG.debug("No executors available to schedule job on. Queuing.")
            async with self._state_mutex.writer_lock:
                self._queued_jobs.append(job)
                return

        executor = random.choice(list(executors))
        await self._schedule_job(job, executor)

    async def _handle_job_result(self, bundle: BundleData) -> None:
        """
        Handle a job result by storing it in the completed jobs set.

        Args:
            bundle (BundleData): The job result bundle.
        """
        LOG.debug("Handling job result")

        job_result = JobResult.deserialize(bundle.payload)

        async with self._state_mutex.writer_lock:
            self._completed_jobs.add(job_result.metadata)

        LOG.info(f"Stored completed job: {job_result.metadata.job_id}")

    async def _handle_job_query(self, bundle: BundleData) -> BundleData:
        LOG.debug("Handling jobs query")
        async with self._state_mutex.reader_lock:
            queued_job_infos = [job.metadata for job in self._queued_jobs]
            jobs = {
                "completed": dictify_job_infos(self._completed_jobs),
                "queued": dictify_job_infos(queued_job_infos),
            }

        jobs_bytes = packb(jobs)
        bundle_response = BundleData(
            type=BundleType.JOB_LIST,
            source=self._node_id,
            destination=bundle.source,
            submitter=bundle.submitter,
            payload=jobs_bytes,
        )
        LOG.debug(f"Response bundle: {bundle_response}")

        return bundle_response

    async def _handle_ndata_put(self, bundle: BundleData) -> None:
        LOG.debug("Handling named data PUT")

        async with self._state_mutex.reader_lock:
            datastores = self._discovered_nodes.get(NodeType.DATASTORE, set())

        if not datastores:
            LOG.debug("No datastores available to forward named data to. Queuing.")
            async with self._state_mutex.writer_lock:
                self._queued_ndata.append(bundle)
                return

        datastore = random.choice(list(datastores))
        await self._forward_ndata(bundle, datastore)

    @override
    async def _handle_discovery(self, bundle: BundleData) -> BundleData | None:
        LOG.debug("Handling discovery")

        async with self._state_mutex.writer_lock:
            match bundle.type:
                case BundleType.BROKER_ANNOUNCE:
                    if bundle.source != self._node_id:
                        LOG.debug(
                            f"Received announcement from other broker: {bundle.source}"
                        )
                        self._discovered_nodes[NodeType.BROKER].add(bundle.source)
                    return None
                case BundleType.BROKER_REQUEST:
                    LOG.debug("Broker request")
                    self._discovered_nodes[bundle.node_type].add(bundle.source)
                    LOG.info(
                        f"Discovered node {bundle.source} of type {bundle.node_type}"
                    )

                    # Notify queue worker in case there are queued jobs or data puts that can now be processed
                    async with self._queue_cv:
                        self._queue_cv.notify_all()

                    return BundleData(
                        type=BundleType.BROKER_ACK,
                        source=self._node_id,
                        destination=bundle.source,
                        node_type=self._node_type,
                    )
                case _:
                    LOG.warning(f"Won't handle bundle of type {bundle.type}")
                    return None

    async def _queue_worker(self) -> None:
        """
        Worker that processes queued jobs and named data puts when nodes become available.
        """
        LOG.info("Starting queue worker")
        while True:
            # Wait for work and nodes to be available
            async with self._queue_cv:
                await self._queue_cv.wait()

            # Create state snapshots to block as little as possible
            jobs_to_process: list[Job] = []
            ndata_to_process: list[BundleData] = []

            available_executors: list[EID] = []
            available_datastores: list[EID] = []

            async with self._state_mutex.writer_lock:
                # Check for Executors + Jobs
                executors = self._discovered_nodes.get(NodeType.EXECUTOR, set())
                if executors and self._queued_jobs:
                    available_executors = list(executors)
                    jobs_to_process = self._queued_jobs
                    self._queued_jobs = []

                # Check for Datastores + Data
                datastores = self._discovered_nodes.get(NodeType.DATASTORE, set())
                if datastores and self._queued_ndata:
                    available_datastores = list(datastores)
                    ndata_to_process = self._queued_ndata
                    self._queued_ndata = []

            # Process Jobs
            if jobs_to_process and available_executors:
                LOG.info(f"Processing {len(jobs_to_process)} queued jobs")
                for job in jobs_to_process:
                    # TODO: Implement proper load balancing
                    # TODO: Implement capabilities checking; this depends on executor capability announcements -> Heartbeats?
                    executor = random.choice(available_executors)
                    await self._schedule_job(job, executor)

            # Process Named Data
            if ndata_to_process and available_datastores:
                LOG.info(f"Processing {len(ndata_to_process)} queued data puts")
                for bundle in ndata_to_process:
                    # TODO: Implement proper load balancing
                    datastore = random.choice(available_datastores)
                    await self._forward_ndata(bundle, datastore)

    async def _forward_ndata(self, bundle: BundleData, datastore: EID) -> None:
        """
        Forward named data to the specified datastore.

        Args:
            bundle (BundleData): The named data bundle to forward.
            datastore (EID): The datastore EID to forward the data to.
        """
        LOG.info(f"Forwarding named data {bundle.named_data} to datastore {datastore}")

        forward_bundle = BundleData(
            type=BundleType.NDATA_PUT,
            source=bundle.source,  # As if it came from the original sender
            destination=datastore,
            payload=bundle.payload,
            submitter=bundle.source,
            named_data=bundle.named_data,
        )

        await self._send_and_check(bundles=[forward_bundle])

    async def _schedule_job(self, job: Job, executor: EID) -> None:
        """
        Schedule a job on the specified executor.

        Args:
            job (Job): The job to schedule.
            executor (EID): The executor EID to schedule the job on.
        """
        LOG.info(f"Scheduling job {job.metadata.job_id} on executor {executor}")

        job_submission = BundleData(
            type=BundleType.JOB_SUBMIT,
            source=self._node_id,
            destination=executor,
            payload=job.serialize(),
        )

        await self._send_and_check(bundles=[job_submission])
