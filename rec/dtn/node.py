import asyncio
import sys
from abc import ABC, abstractmethod
from dataclasses import field
from socket import AF_UNIX, SOCK_STREAM, socket

from aiorwlock import RWLock

from rec.dtn.messages import *
from rec.util.log import LOG


@dataclass
class Node(ABC):
    node_id: EID
    dtn_agent_socket: str
    node_type: NodeType

    _state_mutex: RWLock = field(default_factory=RWLock)

    _broker_pending: EID | None = None
    _broker: EID | None = None

    def __post_init__(self) -> None:
        if isinstance(self.node_id, str):
            self.node_id = EID(self.node_id)

    @abstractmethod
    async def run(self) -> None:
        pass

    async def _send_messages(self, messages: list[Message]) -> list[Reply]:
        replies: list[Reply] = []

        for message in messages:
            replies.append(await self._send_message(message=message))

        return replies

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

    async def _register(self) -> None:
        LOG.info("Performing registration")
        message = Register(type=MessageType.REGISTER, endpoint_id=self.node_id)
        LOG.debug(f"Sending registration message: {message}")

        try:
            reply = await self._send_message(message=message)
        except FileNotFoundError as err:
            LOG.critical("Error connecting to dtnd: %s", err, exc_info=True)
            sys.exit(1)

        if not reply.success:
            LOG.debug("Error registering with dtnd: %s", reply.error)
            return

    async def _send_bundle(self, bundle: BundleData) -> Reply:
        message = BundleCreate(type=MessageType.CREATE, bundle=bundle)
        return await self._send_message(message=message)

    async def _send_bundles(self, bundles: list[BundleData]) -> list[Reply]:
        replies: list[Reply] = []

        for bundle in bundles:
            replies.append(await self._send_bundle(bundle=bundle))

        return replies

    async def _get_new_bundles(self) -> list[BundleData]:
        LOG.debug("Retrieving new bundles")
        bundles: list[BundleData] = []

        message = Fetch(
            type=MessageType.FETCH, endpoint_id=self.node_id, node_type=self.node_type
        )
        LOG.debug(f"Sending fetch: {message}")
        reply = await self._send_message(message=message)

        assert isinstance(reply, FetchReply)

        if reply.success:
            bundles = reply.bundles
        else:
            LOG.error("dtnd replied with error: %s", reply.error)

        return bundles

    async def _handle_discovery(self, bundle: BundleData) -> list[BundleData]:
        """
        To be called when a broker-discovery bundle is received
        """
        responses: list[BundleData] = []

        match bundle.type:
            case BundleType.BROKER_ANNOUNCE:
                LOG.debug("Broker announcement")
                async with self._state_mutex.writer_lock:
                    if self._broker_pending is None and self._broker is None:
                        self._broker_pending = bundle.source
                        LOG.info(
                            f"Pending association with broker {self._broker_pending}"
                        )
                        response = BundleData(
                            type=BundleType.BROKER_REQUEST,
                            source=self.node_id,
                            destination=bundle.source,
                            node_type=self.node_type,
                        )
                        responses.append(response)

            case BundleType.BROKER_ACK:
                LOG.debug("Broker ACK")
                async with self._state_mutex.writer_lock:
                    if self._broker_pending == bundle.source:
                        self._broker = bundle.source
                        self._broker_pending = None
                        LOG.debug(f"Now associated with broker {bundle.source}")
                    else:
                        LOG.debug(f"Received ACK from unknown broker: {bundle.source}")

            case _:
                LOG.warning(f"Can't handle bundle of type {bundle.type}")

        return responses
