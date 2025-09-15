import asyncio

from pathlib import Path

from asynctinydb import TinyDB

from rec.dtn.messages import *
from rec.dtn.node import Node
from rec.util.log import LOG


class Datastore(Node):
    rootdir: Path
    db: TinyDB
    state_mutex: asyncio.Lock

    def __init__(self, *args, rootdir: str | Path, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        if isinstance(rootdir, str):
            self.rootdir = Path(rootdir)
        else:
            self.rootdir = rootdir
        self.db = TinyDB(f"{rootdir}/database.db")
        self.state_mutex = asyncio.Lock()

    @override
    async def run(self) -> None:
        LOG.info("Starting datastore")
        await self._handle_bundles()

    async def _handle_bundles(self) -> None:
        LOG.info("Starting bundle handler")
        while True:
            LOG.debug("Bundle handler going to sleep")
            await asyncio.sleep(10)

            LOG.debug("Running bundle handler")
            try:
                LOG.debug("Retrieving bundles")
                bundles = await self._get_new_bundles(NodeType.BROKER)
                if bundles:
                    LOG.debug(f"Bundles: {bundles}")
                    for bundle in bundles:
                        await self._handle_bundle(bundle=bundle)
                else:
                    LOG.debug("No new bundles")
            except Exception as err:
                LOG.exception("Error fetching bundles: %s", err)

    async def _store_data(self, name: str, data: bytes) -> None:
        pass

    async def _load_data(self, name: str) -> bytes:
        return b""
