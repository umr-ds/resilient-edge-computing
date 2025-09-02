from queue import Queue
from typing import override
from socket import socket, AF_UNIX, SOCK_STREAM

from rec.dtn.messages import (
    serialize,
    deserialize,
    Message,
    Reply,
    MsgType,
    MsgStatus,
    JobsQuery,
    JobsReply,
)
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
    def run(self) -> None:
        LOG.info(f"Staring unix listener on {self.dtn_agent_socket}")
        with socket(AF_UNIX, SOCK_STREAM) as s:
            s.bind(self.dtn_agent_socket)
            s.listen(1)

            conn, _ = s.accept()
            with conn:
                LOG.debug("Accepted connection")

                # receive and deserialize message
                data = conn.recv(4)
                message_length = int.from_bytes(
                    bytes=data, byteorder="big", signed=False
                )
                LOG.debug(f"Message length: {message_length}")

                data = conn.recv(message_length)
                message = deserialize(data=data)

                LOG.debug(f"Received message: {message}")

                reply = self._handle_message(message=message)
                LOG.debug(f"Sending reply: {reply}")

                ## serialize and send reply
                reply_bytes = serialize(message=reply)
                reply_length = len(reply_bytes)
                LOG.debug(f"Reply length: {reply_length}")
                reply_length_bytes = reply_length.to_bytes(
                    length=4, byteorder="big", signed=False
                )

                s.sendall(reply_length_bytes)
                LOG.debug("Sent reply length")
                s.sendall(reply_bytes)
                LOG.debug("Sent reply")

    def _handle_message(self, message: Message) -> Message:
        if isinstance(message, JobsQuery):
            return self._handle_jobs_query(message)

        return Reply(
            Type=MsgType.GENERAL_REPLY,
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
