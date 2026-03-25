import asyncio
import contextlib
import signal
import sys
from abc import ABC, abstractmethod
from asyncio import Lock
from dataclasses import dataclass, field
from pathlib import Path
from socket import AF_UNIX, SOCK_STREAM, socket
from types import TracebackType
from typing import Self
from uuid import UUID

from rec.eid import EID, get_multicast_address
from rec.errors import (
    BundlePushStartFailedError,
    DtndConnectionClosedError,
    DtndSocketNotFoundError,
    DtndSocketOperationError,
    InvalidReplyLengthError,
    MessageError,
    NodeConnectionError,
    NotConnectedToDtndError,
    RecError,
)
from rec.log import LOG
from rec.messages import (
    BundleCreate,
    BundleData,
    BundlePush,
    BundlePushStart,
    BundlePushStop,
    BundleType,
    Message,
    MessageType,
    NodeType,
    Register,
    Reply,
    deserialize,
    serialize,
)


@dataclass
class Node(ABC):
    _node_id: EID
    _dtn_agent_socket: Path
    _node_type: NodeType

    _state_mutex: Lock = field(default_factory=Lock)
    _stop_event: asyncio.Event = field(default_factory=asyncio.Event)

    _broker_pending: EID | None = None
    _broker: EID | None = None

    _socket: socket | None = None
    _socket_lock: Lock = field(default_factory=Lock)

    _bundle_queue: asyncio.Queue[BundlePush] = field(default_factory=asyncio.Queue)
    _bundle_processing_task: asyncio.Task | None = None

    _message_receive_task: asyncio.Task | None = None
    _pending_requests: dict[UUID, asyncio.Future[Reply]] = field(default_factory=dict)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        type_: type[BaseException] | None,
        value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._socket is not None:
            with contextlib.suppress(OSError):
                self._socket.close()
            self._socket = None
            LOG.debug("Disconnected from dtnd")

    @property
    def _running(self) -> bool:
        """Check if the node is still running."""
        return not self._stop_event.is_set()

    async def stop(self) -> None:
        """Stop the node and wait for background tasks to finish."""
        self._stop_event.set()

        async with self._socket_lock:
            await self._close_connection()

        if self._bundle_processing_task:
            self._bundle_processing_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._bundle_processing_task
            self._bundle_processing_task = None

    async def _interruptible_sleep(self, seconds: float) -> None:
        """
        Sleep for the specified number of seconds, but wake up early if the node is stopping.

        Args:
            seconds (float): The number of seconds to sleep.
        """
        with contextlib.suppress(TimeoutError):  # Slept the full duration
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        # Call stop() on SIGINT (Ctrl+C)
        loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(self.stop()))

        # Start bundle processing task
        self._bundle_processing_task = asyncio.create_task(
            self._bundle_processing_loop()
        )

        await self._register()

    async def _ensure_connected(self) -> None:
        """
        Ensures an active connection to dtnd, creates one if needed.

        Note: Caller must hold _socket_lock before calling this method.
        """
        if self._socket is None:
            LOG.info(f"Connecting to dtnd on {self._dtn_agent_socket}")
            new_socket = socket(AF_UNIX, SOCK_STREAM)
            new_socket.setblocking(False)  # noqa: FBT003 # positional argument only
            loop = asyncio.get_running_loop()
            try:
                await loop.sock_connect(new_socket, str(self._dtn_agent_socket))
            except FileNotFoundError as err:
                new_socket.close()
                raise DtndSocketNotFoundError(self._dtn_agent_socket) from err
            except OSError as err:
                new_socket.close()
                raise DtndSocketOperationError.for_connect() from err

            self._socket = new_socket
            LOG.debug("Connected to dtnd")

            # Start background message receive loop
            self._message_receive_task = asyncio.create_task(
                self._message_receive_loop(self._socket)
            )

            await self._start_bundle_push()

    async def _close_connection(self) -> None:
        """
        Closes the connection to dtnd if open.

        Note: Caller must hold _socket_lock before calling this method.
        """
        await self._stop_bundle_push()

        # Cancel message receive task
        if self._message_receive_task is not None:
            if self._message_receive_task is not asyncio.current_task():
                self._message_receive_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._message_receive_task
            self._message_receive_task = None

        # Fail all pending requests
        for reply_future in self._pending_requests.values():
            if not reply_future.done():
                reply_future.set_exception(ConnectionResetError("Connection closed"))
        self._pending_requests.clear()

        # Close socket
        if self._socket is not None:
            with contextlib.suppress(OSError):
                self._socket.close()
            self._socket = None
            LOG.debug("Disconnected from dtnd")

    async def _start_bundle_push(self) -> None:
        """
        Start push-based bundle delivery.
        To avoid recursion and deadlocks, this method handles the handshake itself.

        Note: Caller must hold _socket_lock before calling this method.
        """
        message = BundlePushStart(type=MessageType.BUNDLE_PUSH_START)

        # Create a future for this request
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Reply] = loop.create_future()
        self._pending_requests[message.message_id] = future

        await self._send_raw(message)

        reply = await future

        if not reply.success:
            raise BundlePushStartFailedError(reply.error)
        LOG.info("Push mode enabled")

    async def _stop_bundle_push(self) -> None:
        """
        Stop push-based bundle delivery.
        Since the connection is closing, we just ignore failure and don't require a reply.

        Note: Caller must hold _socket_lock before calling this method.
        """
        message = BundlePushStop(type=MessageType.BUNDLE_PUSH_STOP)
        with contextlib.suppress(NodeConnectionError):
            await self._send_raw(message)
        LOG.info("Push mode disabled")

    async def _recv_exact(self, socket: socket, nbytes: int) -> bytes:
        loop = asyncio.get_running_loop()
        data = b""
        while len(data) < nbytes:
            try:
                packet = await loop.sock_recv(socket, nbytes - len(data))
            except OSError as err:
                raise DtndSocketOperationError.for_receive() from err
            if not packet:
                raise DtndConnectionClosedError
            data += packet
        return data

    @staticmethod
    def _parse_reply_length(length_bytes: bytes) -> int:
        reply_length = int.from_bytes(bytes=length_bytes, byteorder="big", signed=False)
        if reply_length <= 0:
            raise InvalidReplyLengthError(reply_length)
        return reply_length

    async def _message_receive_loop(self, sock: socket) -> None:
        """
        Background loop to receive any messages from dtnd.
        """
        LOG.debug("Starting message receive loop")

        try:
            while self._running:
                data = await self._recv_exact(sock, 8)
                reply_length = self._parse_reply_length(data)
                LOG.debug(f"Reply length: {reply_length}")

                data = await self._recv_exact(sock, reply_length)
                message = deserialize(serialized=data)
                await self._handle_incoming_message(message=message)

        except asyncio.CancelledError:
            LOG.debug("Message receive loop cancelled")
        except (MessageError, NodeConnectionError) as err:
            LOG.exception("Error in message receive loop: %s", err)
            async with self._socket_lock:
                await self._close_connection()
        finally:
            LOG.debug("Message receive loop stopped")

    async def _handle_incoming_message(self, message: Message) -> None:
        """
        Handle an incoming message from dtnd.

        Args:
            message (Message): The incoming message to handle.
        """
        match message:
            case Reply() as reply:
                LOG.debug(f"Received reply: {reply}")

                future = self._pending_requests.pop(reply.message_id, None)
                if future is None:
                    LOG.warning(
                        f"Received reply for unknown request: {reply.message_id}"
                    )
                    return

                if not future.done():
                    future.set_result(reply)

            case BundlePush() as bundle_push:
                # ACK BundlePush immediately
                ack = Reply(
                    type=MessageType.REPLY,
                    message_id=bundle_push.message_id,
                    success=True,
                    error="",
                )
                async with self._socket_lock:
                    await self._send_raw(ack)

                # Queue bundles for processing
                self._bundle_queue.put_nowait(bundle_push)

    async def _bundle_processing_loop(self) -> None:
        LOG.debug("Starting bundle processing loop")
        try:
            while self._running:
                bundle_push = await self._bundle_queue.get()
                await self._process_bundle_push(bundle_push)
                self._bundle_queue.task_done()
        except asyncio.CancelledError:
            LOG.debug("Bundle processing loop cancelled")
        except (OSError, RecError, TypeError, ValueError) as err:
            LOG.exception("Error in bundle processing loop: %s", err)
        finally:
            LOG.debug("Bundle processing loop stopped")

    async def _process_bundle_push(self, bundle_push: BundlePush) -> None:
        bundles = bundle_push.bundles
        if not bundles:
            LOG.warning("Received empty bundle push")
            return

        LOG.debug(f"Received bundle push with {len(bundles)} bundles")

        try:
            replies = await self._handle_bundles(bundles=bundles)
            if replies:
                await self._send_bundles_and_check(bundles=replies)
        except (OSError, RecError, TypeError, ValueError) as err:
            LOG.exception("Error processing bundle push: %s", err)

    async def _send_raw(self, message: Message) -> None:
        """
        Sends a message over the socket.

        Note: Caller must hold `_socket_lock` before calling this method.
        Assumes that `_ensure_connected` has already been called.

        Args:
            message (Message): The message to send.

        Raises:
            NotConnectedToDtndError: If not connected to dtnd.
            ConnectionResetError: If the connection is closed by dtnd.
        """
        if self._socket is None:
            raise NotConnectedToDtndError
        loop = asyncio.get_running_loop()

        # Serialize and send message
        message_bytes = serialize(message=message)
        message_length = len(message_bytes)
        LOG.debug(f"Message length: {message_length}")
        message_length_bytes = message_length.to_bytes(
            length=8, byteorder="big", signed=False
        )

        try:
            await loop.sock_sendall(self._socket, message_length_bytes)
            LOG.debug("Sent message length")
            await loop.sock_sendall(self._socket, message_bytes)
            LOG.debug("Sent message")
        except OSError as err:
            raise DtndSocketOperationError.for_send() from err

    async def _send_message(self, message: Message) -> Reply:
        """
        Sends a message over the socket and receives the reply.
        Retries once on connection failure.

        Args:
            message (Message): The message to send.

        Returns:
            Reply: The reply received for the message.
        """
        # Create a future for this request
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Reply] = loop.create_future()
        self._pending_requests[message.message_id] = future

        async with self._socket_lock:
            try:
                await self._ensure_connected()
                await self._send_raw(message)
            except NodeConnectionError as err:
                # Connection failed, close and retry once
                LOG.warning(f"Connection error, reconnecting: {err}")
                await self._close_connection()

                # Retry with fresh connection
                await self._ensure_connected()
                await self._send_raw(message)

        return await future

    async def _register_eid(self, eid: EID) -> None:
        LOG.info(f"Registering endpoint ID {eid}")

        message = Register(type=MessageType.REGISTER, endpoint_id=eid)
        LOG.debug(f"Sending registration message: {message}")

        try:
            reply = await self._send_message(message=message)
        except FileNotFoundError as err:
            LOG.critical("Error connecting to dtnd: %s", err, exc_info=True)
            sys.exit(1)

        if not reply.success:
            LOG.debug(f"Error registering {eid} with dtnd: {reply.error}")
            return

        LOG.info(f"Successfully registered endpoint ID {eid} with dtnd")

    async def _register(self) -> None:
        LOG.info("Performing registration")

        # Register unicast endpoint
        await self._register_eid(self._node_id)

        # Register multicast endpoint
        multicast_eid = get_multicast_address(self._node_type)
        if multicast_eid is not None:
            await self._register_eid(multicast_eid)

    async def _send_bundle(self, bundle: BundleData) -> Reply:
        message = BundleCreate(type=MessageType.BUNDLE_CREATE, bundle=bundle)
        return await self._send_message(message=message)

    async def _send_bundle_and_check(self, bundle: BundleData) -> None:
        try:
            LOG.debug("Sending bundle")
            dtnd_response = await self._send_bundle(bundle=bundle)
            if not dtnd_response.success:
                LOG.exception("dtnd sent error: %s", dtnd_response.error)
        except NodeConnectionError as err:
            LOG.exception("error communicating with dtnd: %s", err, exc_info=True)

    async def _send_bundles(self, bundles: list[BundleData]) -> list[Reply]:
        replies: list[Reply] = []

        for bundle in bundles:
            replies.append(await self._send_bundle(bundle=bundle))

        return replies

    async def _send_bundles_and_check(self, bundles: list[BundleData]) -> None:
        try:
            LOG.debug("Sending bundles")
            dtnd_responses = await self._send_bundles(bundles=bundles)
            for dtnd_response in dtnd_responses:
                if not dtnd_response.success:
                    LOG.exception("dtnd sent error: %s", dtnd_response.error)
        except NodeConnectionError as err:
            LOG.exception("error communicating with dtnd: %s", err, exc_info=True)

    @abstractmethod
    async def _handle_bundle(self, bundle: BundleData) -> list[BundleData]:
        """
        Handle an incoming bundle.

        Args:
            bundle (BundleData): The incoming bundle to handle.

        Returns:
            list[BundleData]: A list of response bundles to send back.
        """

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

    async def _handle_discovery(self, bundle: BundleData) -> list[BundleData]:
        """
        To be called when a broker-discovery bundle is received
        """
        responses: list[BundleData] = []

        match bundle.type:
            case BundleType.BROKER_ANNOUNCE:
                LOG.debug("Broker announcement")
                async with self._state_mutex:
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
                async with self._state_mutex:
                    if self._broker_pending == bundle.source:
                        self._broker = bundle.source
                        self._broker_pending = None
                        LOG.info(f"Now associated with broker {bundle.source}")
                    else:
                        LOG.debug(f"Received ACK from unknown broker: {bundle.source}")

            case _:
                LOG.warning(f"Can't handle bundle of type {bundle.type}")

        return responses
