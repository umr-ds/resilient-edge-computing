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
        This provides dedup capabilities, as multiple names can refer to the same hash.
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

    async def _load_data(self, name: str) -> list[tuple[str, bytes]]:
        """
        Loads data from disk

        Looks up name in database and tries to load binary data from blobs directory.

        Args:
            name (str): "human-readable" name of data. May be split in hierarchical parts with '/' separator.
                        If `name` is a common prefix for multiple datas, then we return all of them.

        Returns:
            list(tuple(str, bytes)): List of (name, data) of all data with names that started with `name`

        Raises:
            FileNotFoundError: If for some reason, there is a mapping in database but the associated filename does not exist.
        """
        db_data = Query()
        # TODO: this has linear complexity with the number of stored data. Might be more efficient to build a tree-structure.
        #       but I'm not sure if the performance warrants the increased complexity...
        prefix_finder = lambda s: s.startswith(name)
        entries: list[dict] = await self.db.search(db_data.name.test(prefix_finder))

        all_data: list[tuple[str, bytes]] = []

        for entry in entries:
            filepath = self.blob_directory / entry["filename"]
            async with open(filepath, "rb") as f:
                data = await f.read()
                all_data.append((entry["name"], data))

        return all_data
