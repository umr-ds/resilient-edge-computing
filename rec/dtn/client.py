#! /usr/bin/env python3

import asyncio

from rec.dtn.messages import *
from rec.dtn.node import Node
from rec.util.log import LOG


DTN_ID = "dtn://client_1/"
DTN_SOCKET = "/tmp/rec_test_1.sock"


@dataclass
class Client(Node):

    @override
    async def run(self) -> None:
        message = Register(Type=MessageType.REGISTER, EndpointID=self.node_id)

        try:
            reply = await self._send_message(message=message)
        except FileNotFoundError as err:
            LOG.critical("Error connecting to dtnd: %s", err, exc_info=True)
            return

        if not reply.Success:
            LOG.info("Error registering with dtnd: %s", reply.Error)

        test_bundle = BundleData(
            Type=BundleType.JOBS_QUERY,
            Source=self.node_id,
            Destination="dtn://broker_1/",
            Payload=b"test",
            Metadata={"Submitter": "dtn://client_1/"},
        )
        message = BundleCreate(Type=MessageType.CREATE, Bundle=test_bundle)
        reply = await self._send_message(message=message)
        print(reply)


def main() -> None:
    client = Client(node_id=DTN_ID, dtn_agent_socket=DTN_SOCKET)
    asyncio.run(client.run())


if __name__ == "__main__":
    main()
