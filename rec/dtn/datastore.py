import asyncio

from pathlib import Path
from hashlib import sha1

from aiofiles import open
from asynctinydb import TinyDB, Query

from rec.dtn.messages import *
from rec.dtn.node import Node
from rec.util.log import LOG


@dataclass
class NameTakenError(Exception):
    name: str

    @override
    def __str__(self) -> str:
        return f"name {self.name} already taken"


@dataclass
class NoSuchNameError(Exception):
    name: str

    @override
    def __str__(self) -> str:
        return f"no such name: {self.name}"


class Datastore(Node):
    root_directory: Path
    blob_directory: Path
    db: TinyDB
    state_mutex: asyncio.Lock

    def __init__(self, *args, root_directory: str | Path, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        if isinstance(root_directory, str):
            self.root_directory = Path(root_directory)
        else:
            self.root_directory = root_directory
        self.root_directory.mkdir(parents=True, exist_ok=True)

        self.blob_directory = self.root_directory / "blobs"
        self.blob_directory.mkdir(exist_ok=True)

        self.db = TinyDB(f"{root_directory}/database.db")
        self.state_mutex = asyncio.Lock()

    @override
    async def run(self) -> None:
        LOG.info("Starting datastore")
        await self._register()
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
        """
        Stores data on disk

        Actual data is stored in 'blobs' directory. SHA1 of data will be used as filename.
        Mapping of name -> filename will be stored in tinydb.

        Args:
            name (str):   "human-readable" name for data. May be split in hierarchical parts with '/' separator.
                          Names must be unique.
            data (bytes): Actual data to be stored, in serialized binary form.

        Raises:
            NameTakenError: If there already exists stored data with the exact same name.
        """
        db_data = Query()
        test = await self.db.search(db_data.name == name)
        if test:
            raise NameTakenError(name=name)

        filename = sha1(data, usedforsecurity=False).hexdigest()
        await self.db.insert({"name": name, "filename": filename})

        filepath = self.blob_directory / filename

        # dedup - if the file already exists, we don't need to create it
        if filepath.exists():
            return

        async with open(self.blob_directory / filename, "wb") as f:
            await f.write(data)

    async def _load_data(self, name: str) -> bytes:
        """
        Loads data from disk

        Looks up name in database and tries to load binary data from blobs directory.

        Args:
            name (str): "human-readable" name of data. May be split in hierarchical parts with '/' separator.

        Returns:
            bytes: Binary data which was stored on disk.

        Raises:
            NoSuchNameError:   If database holds no mapping for name
            FileNotFoundError: If for some reason, there is a mapping in database but the associated filename does not exist.
        """
        db_data = Query()
        entry = await self.db.search(db_data.name == name)
        if not entry:
            raise NoSuchNameError(name=name)

        return b""
