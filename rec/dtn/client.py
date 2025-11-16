import asyncio
import os
from argparse import Namespace
from pathlib import Path
from typing import override

import msgpack
from tomlkit import dump, load

from rec.dtn.eid import EID
from rec.dtn.job import ExecutionPlan, Job
from rec.dtn.messages import BundleCreate, BundleData, BundleType, MessageType, NodeType
from rec.dtn.node import Node
from rec.util.log import LOG


class Client(Node):
    context_file: str
    context_data: dict

    def __init__(self, node_id: str | EID, dtn_agent_socket: str, context_file: str):
        super().__init__(
            node_id=node_id,
            dtn_agent_socket=dtn_agent_socket,
            node_type=NodeType.CLIENT,
        )

        self.context_file = context_file

        if os.path.isfile(context_file):
            with open(context_file, "r") as f:
                self.context_data = load(f)
                assert (
                    "broker" in self.context_data
                ), "context file must contain broker address"
                assert self.context_data["broker"], "broker address must be a value"
                self._broker = EID(self.context_data["broker"])
        else:
            self.context_data = {}

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
        self.context_data["broker"] = self._broker
        with open(self.context_file, "w") as f:
            dump(self.context_data, f)

    async def wait_reply(self, wait_for: BundleType) -> BundleData:
        LOG.info("Waiting for reply")
        while True:
            await asyncio.sleep(10)
            bundles = await self._get_new_bundles()
            for bundle in bundles:
                if bundle.type == wait_for:
                    return bundle

    async def job_query(self, submitter: str) -> None:
        LOG.info("Performing job query")

        query_bundle = BundleData(
            type=BundleType.JOB_QUERY,
            source=self.node_id,
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

    async def data_get(self, datastore: EID, name: str) -> None:
        """
        Retrieve data from a datastore.

        Args:
            datastore (EID): The datastore endpoint ID.
            name (str): The name of the data to retrieve.
        """
        LOG.info(f"Performing data GET: Name: {name}")

        query_bundle = BundleData(
            type=BundleType.NDATA_GET,
            source=self.node_id,
            destination=datastore,
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

    async def data_put(self, datastore: EID, name: str, data_file: Path) -> bool:
        """
        Upload data to a datastore.

        Args:
            datastore (EID): The datastore endpoint ID.
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
            source=self.node_id,
            destination=datastore,
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

    async def execute_plan(self, plan_path: Path, datastore: EID) -> None:
        """
        Execute an execution plan: publish named data and submit jobs.

        Args:
            plan (ExecutionPlan): The execution plan to execute.
            datastore (EID): Datastore EID for publishing named data.

        Raises:
            ValueError: If broker is not known or if paths are missing.
        """
        LOG.info("Starting execution plan")

        plan = await ExecutionPlan.from_toml(plan_path)

        missing_paths = plan.validate_all_paths()
        if missing_paths:
            raise ValueError(f"Missing data files: {missing_paths}")

        if plan.named_data:
            await self._publish_all_named_data(plan.named_data, datastore)

        for idx, job_on_disk in enumerate(plan.jobs):
            LOG.info(f"Submitting job {idx + 1}/{len(plan.jobs)}")
            job = await job_on_disk.as_job()
            await self._submit_job(job)

        LOG.info("Execution plan submitted")

    async def _publish_all_named_data(
        self, named_data: dict[str, Path], datastore: EID
    ) -> None:
        """
        Publish multiple named data items to datastore.

        Args:
            named_data (dict[str, Path]): Mapping of named identifiers to file paths.
            datastore (EID): The datastore endpoint ID.
        """
        LOG.info(f"Publishing {len(named_data)} named data items")

        for name, path in named_data.items():
            await self.data_put(datastore=datastore, name=name, data_file=path)

    async def _submit_job(self, job: Job) -> None:
        """
        Submit a job to the broker.

        Args:
            job: Job instance to submit.
        """
        LOG.debug(f"Submitting job: {job.metadata.wasm_module}")

        # Dictify the job and set the submitter
        job_dict = job.dictify()
        job_dict["metadata"]["submitter"] = self.node_id

        job_payload = msgpack.packb(job_dict)

        bundle = BundleData(
            type=BundleType.JOB_SUBMIT,
            source=self.node_id,
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

    async def _check_for_job_results(self) -> None:
        """
        Check for job results and process them.
        """
        LOG.info("Checking for job results")
        bundles = await self._get_new_bundles()

        for bundle in bundles:
            if bundle.type == BundleType.JOB_RESULT:
                LOG.info("Received job result")

                # TODO: Save result to disk?
                result_data = bundle.payload
                LOG.info(f"Result size: {len(result_data)} bytes")


def main(args: Namespace) -> None:
    client = Client(
        node_id=args.id, dtn_agent_socket=args.socket, context_file=args.context_file
    )
    asyncio.run(client.run())

    match args.command:
        case "query":
            asyncio.run(client.job_query(submitter=args.submitter))
        case "data":
            match args.data_command:
                case "get":
                    asyncio.run(
                        client.data_get(
                            datastore=args.datastore_id, name=args.data_name
                        )
                    )
                case "put":
                    asyncio.run(
                        client.data_put(
                            datastore=args.datastore_id,
                            name=args.data_name,
                            data_file=args.data_file,
                        )
                    )
        case "exec":
            try:
                asyncio.run(
                    client.execute_plan(
                        plan_path=args.plan_file, datastore=args.datastore_id
                    )
                )
            except Exception as e:
                LOG.error(f"Failed to execute plan: {e}", exc_info=True)
        case "check":
            asyncio.run(client._check_for_job_results())
        case _:
            LOG.critical(f"Unknown command: {args.command}")
