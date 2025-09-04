import asyncio
from queue import Queue
from socket import socket, AF_UNIX, SOCK_STREAM

from rec.dtn.messages import *
from rec.dtn.node import Node
from rec.util.log import LOG


class Broker(Node):
    node_id: str
    dtn_agent_socket: str
    completed_jobs: set
    queued_jobs: Queue

    def __init__(self, node_id: str, dtn_agent_socket: str):
        self.node_id = node_id
        self.dtn_agent_socket = dtn_agent_socket
        self.completed_jobs = set()
        self.queued_jobs = Queue()

    @override
    async def run(self) -> None:
        message = Register(Type=MsgType.REGISTER, EID=self.node_id)
        _ = await self._send_message(message=message)

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

    def _handle_bundle_message(self, message: BundleMessage) -> Message:
        if isinstance(message, JobsQuery):
            return self._handle_jobs_query(message)

        return BundleReply(
            Type=MsgType.REPLY,
            Sender=self.dtn_id,
            Recipient=message.Sender,
            Status=MsgStatus.FAILURE,
            Text="Unsupported Message",
        )

    def _handle_jobs_query(self, message: JobsQuery) -> Message:
        queued = [job.job_id for job in list(self.queued_jobs.queue)]
        completed = list(self.completed_jobs)
        return JobsReply(
            Type=MsgType.JOBS_REPLY,
            Sender=self.dtn_id,
            Recipient=message.Sender,
            Queued=queued,
            Completed=completed,
        )
