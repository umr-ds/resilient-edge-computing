#! /usr/bin/env python3

import argparse
import asyncio
from dataclasses import dataclass
import time
from typing import override

import msgpack

from rec.dtn.messages import (
    MessageType,
    NodeType,
    Register,
    BundleCreate,
    BundleData,
    BundleType,
)
from rec.dtn.node import Node
from rec.util.log import LOG


DEFAULT_DTN_ID = "dtn://client_1/"
DEFAULT_DTN_SOCKET = "/tmp/rec_test_1.sock"


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
            destination="dtn://broker_1/",
            payload=b"test",
            submitter=self.node_id,
        )
        message = BundleCreate(type=MessageType.CREATE, bundle=test_bundle)
        reply = await self._send_message(message=message)
        print(reply)

        time.sleep(30)

        bundles = await self._get_new_bundles(NodeType.CLIENT)
        jobs = msgpack.unpackb(bundles[0].payload)
        print(jobs)


def main() -> None:
    parser = argparse.ArgumentParser(description="REC Client Node")
    parser.add_argument(
        "--dtn-id",
        default=DEFAULT_DTN_ID,
        help=f"DTN endpoint ID (default: {DEFAULT_DTN_ID})",
    )
    parser.add_argument(
        "--dtn-socket",
        default=DEFAULT_DTN_SOCKET,
        help=f"Path to DTN daemon socket (default: {DEFAULT_DTN_SOCKET})",
    )

    args = parser.parse_args()

    client = Client(node_id=args.dtn_id, dtn_agent_socket=args.dtn_socket)
    asyncio.run(client.run())


if __name__ == "__main__":
    main()
