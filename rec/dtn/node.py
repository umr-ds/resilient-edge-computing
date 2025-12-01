import asyncio
import signal
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from socket import AF_UNIX, SOCK_STREAM, socket

from aiorwlock import RWLock

from rec.dtn.eid import EID
from rec.dtn.messages import (
    BundleCreate,
    BundleData,
    BundleType,
    Fetch,
    FetchReply,
    Message,
    MessageType,
    NodeType,
    Register,
    Reply,
    deserialize,
    serialize,
)
from rec.util.log import LOG

BUNDLE_POLL_INTERVAL_SECONDS = 10


@dataclass
class Node(ABC):
    _node_id: EID
    _dtn_agent_socket: Path
    _node_type: NodeType

    _state_mutex: RWLock = field(default_factory=RWLock)
    _stop_event: asyncio.Event = field(default_factory=asyncio.Event)

    _receive_task: asyncio.Task | None = None

    _broker_pending: EID | None = None
    _broker: EID | None = None

    @property
    def _running(self) -> bool:
        """Check if the node is still running."""
        return not self._stop_event.is_set()

    async def stop(self) -> None:
        """Stop the node and wait for background tasks to finish."""
        self._stop_event.set()

        if self._receive_task:
            await self._receive_task

    async def _interruptible_sleep(self, seconds: float) -> None:
        """
        Sleep for the specified number of seconds, but wake up early if the node is stopping.

        Args:
            seconds (float): The number of seconds to sleep.
        """
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            # We slept the full duration
            pass

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        # Call stop() on SIGINT (Ctrl+C)
        loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(self.stop()))

        # Start background receive loop for handling incoming bundles
        self._receive_task = asyncio.create_task(self._receive_loop())

        await self._register()

    async def _send_messages(self, messages: list[Message]) -> list[Reply]:
        replies: list[Reply] = []

        for message in messages:
            replies.append(await self._send_message(message=message))

        return replies

    async def _send_message(self, message: Message) -> Reply:
        loop = asyncio.get_running_loop()
        LOG.info(f"Connecting to dtnd on {self._dtn_agent_socket}")
        with socket(AF_UNIX, SOCK_STREAM) as s:
            s.connect(str(self._dtn_agent_socket))
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

            assert reply_length > 0

            data = await loop.sock_recv(s, reply_length)
            reply = deserialize(serialized=data)
            LOG.debug(f"Received reply: {reply}")

            assert isinstance(reply, Reply)
            return reply

    async def _register(self) -> None:
        LOG.info("Performing registration")
        message = Register(type=MessageType.REGISTER, endpoint_id=self._node_id)
        LOG.debug(f"Sending registration message: {message}")

        try:
            reply = await self._send_message(message=message)
        except FileNotFoundError as err:
            LOG.critical("Error connecting to dtnd: %s", err, exc_info=True)
            sys.exit(1)

        if not reply.success:
            LOG.debug("Error registering with dtnd: %s", reply.error)
            return

        LOG.info("Successfully registered with dtnd")

    async def _send_bundle(self, bundle: BundleData) -> Reply:
        message = BundleCreate(type=MessageType.CREATE, bundle=bundle)
        return await self._send_message(message=message)

    async def _send_bundles(self, bundles: list[BundleData]) -> list[Reply]:
        replies: list[Reply] = []

        for bundle in bundles:
            replies.append(await self._send_bundle(bundle=bundle))

        return replies

    async def _send_and_check(self, bundles: list[BundleData]) -> None:
        try:
            LOG.debug("Sending bundles")
            dtnd_responses = await self._send_bundles(bundles=bundles)
            for dtnd_response in dtnd_responses:
                if not dtnd_response.success:
                    LOG.exception("dtnd sent error: %s", dtnd_response.error)
        except Exception as err:
            LOG.exception("error communicating with dtnd: %s", err, exc_info=True)

    async def _get_new_bundles(self) -> list[BundleData]:
        LOG.debug("Retrieving new bundles")
        bundles: list[BundleData] = []

        message = Fetch(
            type=MessageType.FETCH, endpoint_id=self._node_id, node_type=self._node_type
        )
        LOG.debug(f"Sending fetch: {message}")
        reply = await self._send_message(message=message)

        assert isinstance(reply, FetchReply)

        if reply.success:
            bundles = reply.bundles
        else:
            LOG.error("dtnd replied with error: %s", reply.error)

        return bundles

    @abstractmethod
    async def _handle_bundle(self, bundle: BundleData) -> list[BundleData]:
        """
        Handle an incoming bundle.

        Args:
            bundle (BundleData): The incoming bundle to handle.

        Returns:
            list[BundleData]: A list of response bundles to send back.
        """
        pass

    async def _handle_bundles(self, bundles: list[BundleData]) -> list[BundleData]:
        """
        Handle multiple incoming bundles.

        Args:
            bundles (list[BundleData]): The incoming bundles to handle.

        Returns:
            list[BundleData]: A list of response bundles to send back.
        """
        replies: list[BundleData] = []
        for bundle in bundles:
            replies.extend(await self._handle_bundle(bundle=bundle))
        return replies

    async def _receive_once(self) -> None:
        """
        Receive and handle bundles once.
        """
        try:
            LOG.debug("Retrieving bundles")
            bundles = await self._get_new_bundles()
            if bundles:
                LOG.debug(f"Got {len(bundles)} new bundles")
                replies = await self._handle_bundles(bundles=bundles)

                if replies:
                    await self._send_and_check(bundles=replies)
            else:
                LOG.debug("No new bundles")
        except Exception as err:
            LOG.exception("Error fetching bundles: %s", err)

    async def _receive_loop(self) -> None:
        """
        Loop to continuously receive and handle bundles.
        """
        LOG.info("Starting receive loop")

        while self._running:
            await self._receive_once()

            LOG.debug("Sleeping before next poll")
            await self._interruptible_sleep(BUNDLE_POLL_INTERVAL_SECONDS)

        LOG.info("Receive loop stopped")

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
                            source=self._node_id,
                            destination=bundle.source,
                            node_type=self._node_type,
                        )
                        responses.append(response)

            case BundleType.BROKER_ACK:
                LOG.debug("Broker ACK")
                async with self._state_mutex.writer_lock:
                    if self._broker_pending == bundle.source:
                        self._broker = bundle.source
                        self._broker_pending = None
                        LOG.info(f"Now associated with broker {bundle.source}")
                    else:
                        LOG.debug(f"Received ACK from unknown broker: {bundle.source}")

            case _:
                LOG.warning(f"Can't handle bundle of type {bundle.type}")

        return responses
