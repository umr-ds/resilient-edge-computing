import asyncio
from queue import Queue

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

        try:
            reply = await self._send_message(message=message)
        except FileNotFoundError as err:
            LOG.critical("Error connecting to dtnd: %s", err, exc_info=True)
            return

        if not reply.Success:
            LOG.critical("Error registering with dtnd: %s", reply.Error)
            return

        async with asyncio.TaskGroup() as tg:
            bundle_handler = tg.create_task(self._handle_bundle_messages())
            job_scheduler = tg.create_task(self._schedule_jobs())

    async def _handle_bundle_messages(self) -> None:
        while True:
            await asyncio.sleep(11)

    async def _schedule_jobs(self) -> None:
        while True:
            await asyncio.sleep(1)

    def _handle_jobs_query(self, message: JobsQuery) -> BundleMessage:
        queued = [job.job_id for job in list(self.queued_jobs.queue)]
        completed = list(self.completed_jobs)
        return JobsReply(
            Type=BundleType.JOBS_REPLY,
            Sender=self.dtn_id,
            Recipient=message.Sender,
            Queued=queued,
            Completed=completed,
        )
