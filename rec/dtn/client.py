import asyncio
from argparse import Namespace
from pathlib import Path
from typing import override

import msgpack
from tomlkit import dump, load

from rec.dtn.eid import DATASTORE_MULTICAST_ADDRESS, EID
from rec.dtn.job import ExecutionPlan, Job, JobResult
from rec.dtn.messages import BundleCreate, BundleData, BundleType, MessageType, NodeType
from rec.dtn.node import Node
from rec.util.log import LOG


class Client(Node):
    _context_file: Path
    _context_data: dict
    _results_directory: Path

    _pending_responses: dict[BundleType, list[BundleData]]
    _response_cv: asyncio.Condition

    def __init__(
        self,
        node_id: EID,
        dtn_agent_socket: Path,
        context_file: Path,
        results_directory: Path,
    ) -> None:
        super().__init__(
            _node_id=node_id,
            _dtn_agent_socket=dtn_agent_socket,
            _node_type=NodeType.CLIENT,
        )

        self._context_file = context_file
        self._results_directory = results_directory
        self._results_directory.mkdir(parents=True, exist_ok=True)

        self._pending_responses = {}
        self._response_cv = asyncio.Condition()

        if self._context_file.is_file():
            with self._context_file.open("r") as f:
                self._context_data = load(f)
                assert (
                    "broker" in self._context_data
                ), "context file must contain broker address"
                assert self._context_data["broker"], "broker address must be a value"
                self._broker = EID(self._context_data["broker"])
        else:
            self._context_data = {}

    @override
    async def run(self) -> None:
        await self._register()

        if self._broker is not None:
            LOG.info("Already associated with broker")
            return
        else:
            LOG.info("Not associated with broker")
            await self._find_broker()

    async def _find_broker(self) -> None:
        LOG.info("Waiting for broker announcement")
        while self._broker is None:
            await asyncio.sleep(10)
            bundles = await self._get_new_bundles()
            for bundle in bundles:
                if BundleType.BROKER_ANNOUNCE <= bundle.type <= BundleType.BROKER_ACK:
                    reply = await self._handle_discovery(bundle=bundle)
                    if reply:
                        try:
                            LOG.debug("Sending reply")
                            dtnd_reply = await self._send_bundle(reply[0])
                            if not dtnd_reply.success:
                                LOG.error(f"Error sending bundle: {dtnd_reply.error}")
                        except Exception as err:
                            LOG.exception("Error sending bundle: %s", err)

        LOG.info("Saving broker info")
        self._context_data["broker"] = self._broker
        with self._context_file.open("w") as f:
            dump(self._context_data, f)

    async def _process_incoming_bundles(self) -> None:
        """
        Process all currently available bundles once.
        """
        LOG.debug("Processing incoming bundles")
        try:
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
        """
        Handle an incoming bundle based on its type.

        Args:
            bundle (BundleData): The incoming bundle to handle.
        """
        LOG.debug(f"Handling bundle: {bundle}")

        if bundle.type == BundleType.JOB_RESULT:
            await self._handle_job_result(bundle=bundle)
        elif bundle.type == BundleType.JOB_LIST:
            await self._cache_response(bundle=bundle)
        elif bundle.type == BundleType.NDATA_GET:
            await self._cache_response(bundle=bundle)
        elif bundle.type == BundleType.NDATA_PUT:
            await self._cache_response(bundle=bundle)
        elif BundleType.BROKER_ANNOUNCE <= bundle.type <= BundleType.BROKER_ACK:
            # Discovery handled in _find_broker
            pass
        else:
            LOG.warning(f"Won't handle bundle of type: {bundle.type}")

    async def _handle_job_result(self, bundle: BundleData) -> None:
        """
        Handle a job result bundle by saving the results to the results directory.

        Args:
            bundle (BundleData): The job result bundle.
        """
        LOG.info("Received job result")

        job_result = JobResult.deserialize(bundle.payload)
        result_path = (
            self._results_directory / f"{job_result.metadata.job_id}_result.zip"
        )

        with result_path.open("wb") as f:
            f.write(job_result.results_data)

        LOG.info(f"Saved result to {result_path}")

    async def _cache_response(self, bundle: BundleData) -> None:
        """
        Cache a response bundle for synchronous operations to retrieve.

        Args:
            bundle (BundleData): The response bundle to cache.
        """
        async with self._response_cv:
            if bundle.type not in self._pending_responses:
                self._pending_responses[bundle.type] = []
            self._pending_responses[bundle.type].append(bundle)
            self._response_cv.notify_all()

    async def wait_reply(self, wait_for: BundleType) -> BundleData:
        """
        Wait for a reply bundle of specific type, checking for new bundles periodically.

        Args:
            wait_for (BundleType): The type of bundle to wait for.
        """
        LOG.info(f"Waiting for reply of type {wait_for}")

        while True:
            await asyncio.sleep(10)

            # Check if we already have the response cached
            async with self._response_cv:
                if (
                    wait_for in self._pending_responses
                    and self._pending_responses[wait_for]
                ):
                    return self._pending_responses[wait_for].pop(0)

            # Process any new bundles
            await self._process_incoming_bundles()

            # Check again if response arrived
            async with self._response_cv:
                if (
                    wait_for in self._pending_responses
                    and self._pending_responses[wait_for]
                ):
                    return self._pending_responses[wait_for].pop(0)

    async def job_query(self, submitter: str) -> None:
        """
        Query the broker for jobs submitted by a specific submitter.

        Args:
            submitter (str): The EndpointID of the job submitter to query for.
        """
        LOG.info("Performing job query")

        query_bundle = BundleData(
            type=BundleType.JOB_QUERY,
            source=self._node_id,
            destination=self._broker,
            submitter=EID(submitter),
        )
        message = BundleCreate(type=MessageType.CREATE, bundle=query_bundle)
        await self._send_message(message=message)
        broker_response = await self.wait_reply(BundleType.JOB_LIST)
        if not broker_response.success:
            LOG.error(
                "Broker responded with error %s", broker_response.error, exc_info=False
            )
        else:
            jobs = msgpack.unpackb(broker_response.payload)
            print(jobs)

    async def data_get(self, name: str) -> None:
        """
        Retrieve data from a datastore.

        Args:
            name (str): The name of the data to retrieve.
        """
        LOG.info(f"Performing data GET: Name: {name}")

        query_bundle = BundleData(
            type=BundleType.NDATA_GET,
            source=self._node_id,
            destination=DATASTORE_MULTICAST_ADDRESS,
            named_data=name,
        )
        message = BundleCreate(type=MessageType.CREATE, bundle=query_bundle)
        reply = await self._send_message(message=message)
        print(reply)
        store_rply = await self.wait_reply(BundleType.NDATA_GET)
        if not store_rply.success:
            LOG.error(
                "DataStore responded with error %s", store_rply.error, exc_info=False
            )
        else:
            print(store_rply.payload)

    async def data_put(self, name: str, data_file: Path) -> bool:
        """
        Upload data to a datastore.

        Args:
            name (str): The name to store the data under.
            data_file (Path): Path to the file to upload.

        Returns:
            bool: True if successful, False otherwise.
        """
        LOG.info(f"Performing data PUT: Name: {name}")

        with data_file.open("rb") as f:
            data = f.read()

        query_bundle = BundleData(
            type=BundleType.NDATA_PUT,
            source=self._node_id,
            destination=self._broker,
            payload=data,
            named_data=name,
        )
        message = BundleCreate(type=MessageType.CREATE, bundle=query_bundle)
        reply = await self._send_message(message=message)

        if not reply.success:
            LOG.error(f"Failed to send data PUT for {name}: {reply.error}")
            return False

        store_rply = await self.wait_reply(BundleType.NDATA_PUT)
        if not store_rply.success:
            if store_rply.error.endswith("already taken"):
                LOG.warning(f"DataStore already has {name}, skipping PUT")
                LOG.info(f"Successfully published {name}")
                return True
            else:
                LOG.error(
                    f"DataStore rejected {name}: {store_rply.error}", exc_info=False
                )
                return False
        else:
            LOG.info(f"Successfully published {name}")
            return True

    async def execute_plan(self, plan_path: Path) -> None:
        """
        Execute an execution plan: publish named data and submit jobs.

        Args:
            plan_path (Path): The path to the execution plan file.

        Raises:
            ValueError: If broker is not known or if paths are missing.
        """
        LOG.info("Starting execution plan")

        plan = await ExecutionPlan.from_toml(plan_path)

        missing_paths = plan.validate_all_paths()
        if missing_paths:
            raise ValueError(f"Missing data files: {missing_paths}")

        if plan.named_data:
            await self._publish_all_named_data(named_data=plan.named_data)

        for idx, lazy_job in enumerate(plan.jobs):
            LOG.info(f"Submitting job {idx + 1}/{len(plan.jobs)}")
            job = await lazy_job.as_job()
            await self._submit_job(job)

        LOG.info("Execution plan submitted")

    async def _publish_all_named_data(self, named_data: dict[str, Path]) -> None:
        """
        Publish multiple named data items to datastore.

        Args:
            named_data (dict[str, Path]): Mapping of named identifiers to file paths.
        """
        LOG.info(f"Publishing {len(named_data)} named data items")

        for name, path in named_data.items():
            await self.data_put(name=name, data_file=path)

    async def _submit_job(self, job: Job) -> None:
        """
        Submit a job to the broker.

        Args:
            job: Job instance to submit.
        """
        LOG.debug(f"Submitting job: {job.metadata.wasm_module}")

        # Dictify the job and set the submitter
        job_dict = job.dictify()
        job_dict["metadata"]["submitter"] = self._node_id

        job_payload = msgpack.packb(job_dict)

        bundle = BundleData(
            type=BundleType.JOB_SUBMIT,
            source=self._node_id,
            destination=self._broker,
            payload=job_payload,
        )
        message = BundleCreate(type=MessageType.CREATE, bundle=bundle)

        reply = await self._send_message(message=message)

        job_id = job.metadata.job_id
        if reply.success:
            LOG.info(f"Job submitted successfully: {job_id}")
        else:
            LOG.error(f"Failed to submit job {job_id}: {reply.error}")

    async def check_for_bundles(self) -> None:
        """
        Check for any incoming bundles from the daemon, such as job results.
        """
        LOG.info("Checking for bundles from the daemon")

        await self._process_incoming_bundles()

    async def listen_for_bundles(self) -> None:
        """
        Continuously listen for incoming bundles from the daemon, such as job results.
        This runs indefinitely until interrupted.
        """
        LOG.info("Starting to listen for bundles from the daemon")

        while True:
            await self._process_incoming_bundles()
            await asyncio.sleep(10)


def main(args: Namespace) -> None:
    client = Client(
        node_id=args.id,
        dtn_agent_socket=args.socket,
        context_file=args.context_file,
        results_directory=args.results_directory,
    )
    asyncio.run(client.run())

    match args.command:
        case "query":
            asyncio.run(client.job_query(submitter=args.submitter))
        case "data":
            match args.data_command:
                case "get":
                    asyncio.run(client.data_get(name=args.data_name))
                case "put":
                    asyncio.run(
                        client.data_put(
                            name=args.data_name,
                            data_file=args.data_file,
                        )
                    )
        case "exec":
            try:
                asyncio.run(client.execute_plan(plan_path=args.plan_file))
            except Exception as e:
                LOG.error(f"Failed to execute plan: {e}", exc_info=True)
        case "check":
            asyncio.run(client.check_for_bundles())
        case "listen":
            try:
                asyncio.run(client.listen_for_bundles())
            except KeyboardInterrupt:
                LOG.info("Stopping listener")
        case _:
            LOG.critical(f"Unknown command: {args.command}")
