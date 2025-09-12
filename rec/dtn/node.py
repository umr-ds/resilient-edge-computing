import asyncio

from abc import ABC, abstractmethod
from socket import socket, AF_UNIX, SOCK_STREAM

from rec.dtn.messages import *
from rec.util.log import LOG


@dataclass
class Node(ABC):
    node_id: str
    dtn_agent_socket: str

    @abstractmethod
    async def run(self) -> None:
        pass

    async def _send_message(self, message: Message) -> Reply:
        loop = asyncio.get_running_loop()
        LOG.info(f"Connecting to dtnd on {self.dtn_agent_socket}")
        with socket(AF_UNIX, SOCK_STREAM) as s:
            s.connect(self.dtn_agent_socket)
            LOG.debug("Connected to dtnd")

            ## serialize and send message
            message_bytes = serialize(message=message)
            message_length = len(message_bytes)
            LOG.debug(f"Message length: {message_length}")
            message_length_bytes = message_length.to_bytes(
                length=8, byteorder="big", signed=False
            )

            await loop.sock_sendall(s, message_length_bytes)
            LOG.debug("Sent message length")
            await loop.sock_sendall(s, message_bytes)
            LOG.debug("Sent message")

            # receive and deserialize reply
            data = await loop.sock_recv(s, 8)
            reply_length = int.from_bytes(bytes=data, byteorder="big", signed=False)
            LOG.debug(f"Reply length: {reply_length}")

            data = await loop.sock_recv(s, reply_length)
            reply = deserialize(data=data)
            LOG.debug(f"Received reply: {reply}")

            assert isinstance(reply, Reply)
            return reply

    async def _get_new_bundles(self, node_type: NodeType) -> list[BundleData]:
        LOG.debug("Retrieving new bundles")
        bundles: list[BundleData] = []

        message = Fetch(
            type=MessageType.FETCH, endpoint_id=self.node_id, node_type=node_type
        )
        reply = await self._send_message(message=message)

        assert isinstance(reply, FetchReply)

        if reply.success:
            bundles = reply.bundles
        else:
            LOG.error("dtnd replied with error: %s", reply.error)

        return bundles
