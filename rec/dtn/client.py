#! /usr/bin/env python3

import asyncio
from argparse import Namespace

from rec.dtn.messages import *
from rec.dtn.node import Node
from rec.util.log import LOG

TMP_BROKER = EID("dtn://broker_1/")


@dataclass
class Client(Node):
    @override
    async def run(self) -> None:
        pass

    async def wait_reply(self, wait_for: BundleType) -> BundleData:
        LOG.info("Waiting for reply")
        while True:
            await asyncio.sleep(10)
            bundles = await self._get_new_bundles(NodeType.CLIENT)
            for bundle in bundles:
                if bundle.type == wait_for:
                    return bundle

    async def job_query(self, submitter: str) -> None:
        LOG.info("Performing job query")
        await self._register()

        query_bundle = BundleData(
            type=BundleType.JOB_QUERY,
            source=self.node_id,
            destination=TMP_BROKER,
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
        await self._register()

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
            jobs = unpackb(store_rply.payload)
            print(jobs)

    async def data_put(self, datastore: EID, name: str, data_file: str) -> None:
        LOG.info(f"Performing data PUT: Name: {name}")
        await self._register()

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
            jobs = unpackb(store_rply.payload)
            print(jobs)


def main(args: Namespace) -> None:
    client = Client(node_id=args.id, dtn_agent_socket=args.socket)

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
