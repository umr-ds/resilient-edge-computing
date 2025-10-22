#! /usr/bin/env python3

import asyncio
import os
from argparse import Namespace

from tomlkit import dump, load

from rec.dtn.messages import *
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
            payload=b"",
            submitter=EID(submitter),
        )
        message = BundleCreate(type=MessageType.CREATE, bundle=query_bundle)
        reply = await self._send_message(message=message)
        print(reply)
        broker_response = await self.wait_reply(BundleType.JOB_LIST)
        if not broker_response.success:
            LOG.error(
                "Broker responded with error %s", broker_response.error, exc_info=False
            )
        else:
            jobs = unpackb(broker_response.payload)
            print(jobs)

    async def data_get(self, datastore: EID, name: str) -> None:
        LOG.info(f"Performing data GET: Name: {name}")

        query_bundle = BundleData(
            type=BundleType.NDATA_GET,
            source=self.node_id,
            destination=datastore,
            payload=b"",
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

    async def data_put(self, datastore: EID, name: str, data_file: str) -> None:
        LOG.info(f"Performing data PUT: Name: {name}")

        with open(data_file, "rb") as f:
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
        print(reply)
        store_rply = await self.wait_reply(BundleType.NDATA_PUT)
        if not store_rply.success:
            LOG.error(
                "DataStore responded with error %s", store_rply.error, exc_info=False
            )
        else:
            print("Success")


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
        case _:
            LOG.critical(f"Unknown command: {args.command}")
