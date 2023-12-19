import queue
import subprocess

from ndn.appv2 import NDNApp, pass_all
from ndn.encoding import Component
from ndn.transport.stream_face import TcpFace
from ndn.types import NetworkError, InterestNack, InterestTimeout, InterestCanceled, ValidationFailure


class NDN:
    ndn: NDNApp = None
    ndn_requests = queue.Queue()

    def connect(self, host: str):
        if self.ndn is not None:
            self.shutdown()
        self.ndn = NDNApp(TcpFace(host, 6363))

    def save_from_ndn(self, name: str, data_path: str) -> bool:
        error = queue.Queue(1)
        self.ndn_requests.put((error, name, data_path), block=True)
        return error.get(block=True)

    def shutdown(self):
        self.ndn_requests.put((None, None, None), block=True)

    async def loop(self):
        while True:
            error, name, path = self.ndn_requests.get(block=True)
            if not isinstance(error, queue.Queue):
                self.ndn.shutdown()
                return
            error: queue.Queue = error
            try:
                error.put(await self.segment_fetcher(name, path), block=True)
            except OSError:
                error.put(False, block=True)
                continue

    async def segment_fetcher(self, name: str, path: str):  # TODO smart implementation with asyncio tasks and congestion control
        try:
            name, content, meta = await self.ndn.express(name, pass_all, can_be_prefix=True)
            if Component.get_type(name[-1]) != Component.TYPE_SEGMENT:
                with open(path, "bw") as file:
                    file.write(content)
                return True

            with open(path, "bw") as file:
                for i in range(0, Component.to_number(meta["meta_info"].final_block_id) + 1):
                    name[-1] = Component.from_segment(i)
                    _, content, _ = await self.ndn.express(name, pass_all)
                    file.write(bytes(content))
        except (NetworkError, ValueError, InterestTimeout, InterestNack, InterestCanceled, ValidationFailure):
            return False
        return True

    async def segment_alt(self, name: str, path: str) -> bool:
        with open(path, "bw") as file:
            try:
                res_name, content, _ = await self.ndn.express(name, pass_all)  # TODO real validator
                file.write(content)
            except (NetworkError, ValueError, InterestTimeout, InterestNack, InterestCanceled, ValidationFailure):
                if subprocess.run(["ndncatchunks", name], stdout=file, stderr=None).returncode:  # only work with nfd on same machine
                    return False
            return True


ndn_app: NDN = NDN()
