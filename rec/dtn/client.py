#! /usr/bin/env python3

import asyncio
import time

import msgpack

from rec.dtn.messages import *
from rec.dtn.model.eid import EID
from rec.dtn.node import Node
from rec.util.log import LOG


DTN_ID = EID.dtn("client_1")
DTN_SOCKET = "/tmp/rec_test_1.sock"


@dataclass
class Client(Node):
    @override
    async def run(self) -> None:
        message = Register(type=MessageType.REGISTER, endpoint_id=self.node_id)

        try:
            reply = await self._send_message(message=message)
        except FileNotFoundError as err:
            LOG.critical("Error connecting to dtnd: %s", err, exc_info=True)
            return

        if not reply.success:
            LOG.info("Error registering with dtnd: %s", reply.error)

        test_bundle = BundleData(
            type=BundleType.JOBS_QUERY,
            source=self.node_id,
            destination=EID.dtn("broker_1"),
            payload=b"test",
            submitter=EID.dtn("client_1"),
        )
        message = BundleCreate(type=MessageType.CREATE, bundle=test_bundle)
        reply = await self._send_message(message=message)
        print(reply)

        time.sleep(30)

        bundles = await self._get_new_bundles(NodeType.CLIENT)
        jobs = msgpack.unpackb(bundles[0].payload)
        print(jobs)


def main() -> None:
    client = Client(node_id=DTN_ID, dtn_agent_socket=DTN_SOCKET)
    asyncio.run(client.run())


if __name__ == "__main__":
    main()
