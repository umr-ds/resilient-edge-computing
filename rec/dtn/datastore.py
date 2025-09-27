import asyncio
from hashlib import sha1
from pathlib import Path

from aiofiles import open
from asynctinydb import Query, TinyDB

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

    def __init__(
        self, node_id: str | EID, dtn_agent_socket: str, root_directory: str | Path
    ) -> None:
        super().__init__(node_id=node_id, dtn_agent_socket=dtn_agent_socket)
        if isinstance(root_directory, str):
            self.root_directory = Path(root_directory)
        else:
            self.root_directory = root_directory
        self.root_directory.mkdir(parents=True, exist_ok=True)

        self.blob_directory = self.root_directory / "blobs"
        self.blob_directory.mkdir(exist_ok=True)

        self.db = TinyDB(f"{root_directory}/database.db")

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
                bundles = await self._get_new_bundles(NodeType.DATASTORE)
                if bundles:
                    LOG.debug(f"Bundles: {bundles}")
                    for bundle in bundles:
                        await self._handle_bundle(bundle=bundle)
                else:
                    LOG.debug("No new bundles")
            except Exception as err:
                LOG.exception("Error fetching bundles: %s", err)

    async def _handle_bundle(self, bundle: BundleData) -> None:
        messages: list[Message] = []

        match bundle.type:
            case BundleType.NDATA_PUT:
                LOG.debug("Data action is PUT")
                if bundle.named_data is None:
                    LOG.error("Name was none, this should never happen")
                    return
                if isinstance(bundle.named_data, str):
                    bundle.named_data = [bundle.named_data]
                for name in bundle.named_data:
                    response = BundleData(
                        type=BundleType.NDATA_PUT,
                        source=self.node_id,
                        destination=bundle.source,
                        payload=b"",
                        named_data=name,
                    )
                    message = BundleCreate(type=MessageType.CREATE, bundle=response)
                    try:
                        await self.store_data(name=name, data=bundle.payload)
                    except NameTakenError as err:
                        response.success = False
                        response.error = str(err)
                    messages.append(message)
            case BundleType.NDATA_GET:
                LOG.debug("Data action is GET")
                if bundle.named_data is None:
                    LOG.error("Name was none, this should never happen")
                    return
                if isinstance(bundle.named_data, str):
                    bundle.named_data = [bundle.named_data]
                for name in bundle.named_data:
                    loaded = await self.load_data(name=name)
                    LOG.debug(f"Loaded data: {loaded}")
                    for l_name, l_data in loaded:
                        response = BundleData(
                            type=BundleType.NDATA_GET,
                            source=self.node_id,
                            destination=bundle.source,
                            payload=l_data,
                            named_data=l_name,
                        )
                        message = BundleCreate(type=MessageType.CREATE, bundle=response)
                        messages.append(message)
            case _:
                LOG.error(f"Received bundle of type {bundle.type}, ignoring")

        if messages:
            try:
                dtnd_responses = await self._send_messages(messages=messages)
                for dtnd_response in dtnd_responses:
                    if not dtnd_response.success:
                        LOG.exception("dtnd sent error: %s", dtnd_response.error)
            except Exception as err:
                LOG.exception("error communicating with dtnd: %s", err, exc_info=True)

    async def store_data(self, name: str, data: bytes) -> None:
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
        async with self._state_mutex.writer_lock:
            db_data = Query()
            test = await self.db.search(db_data.name == name)
            if test:
                raise NameTakenError(name=name)

            filename = sha1(data, usedforsecurity=False).hexdigest()

            filepath = self.blob_directory / filename

            # dedup - if the file already exists, we don't need to create it
            if filepath.exists():
                await self.db.insert({"name": name, "filename": filename})
                return

            async with open(self.blob_directory / filename, "wb") as f:
                await f.write(data)
            await self.db.insert({"name": name, "filename": filename})

    async def load_data(self, name: str) -> list[tuple[str, bytes]]:
        """
        Loads data from disk

        Looks up name in database and tries to load binary data from blobs directory.

        Args:
            name (str): "human-readable" name of data. May be split in hierarchical parts with '/' separator.
                        If `name` is a common prefix for multiple datas, then we return all of them.

        Returns:
            list(tuple(str, bytes)): List of (name, data) of all data with names that started with `name`
        """
        async with self._state_mutex.reader_lock:
            db_data = Query()
            # TODO: this has linear complexity with the number of stored data. Might be more efficient to build a tree-structure.
            #       but I'm not sure if the performance warrants the increased complexity...
            prefix_finder = lambda s: s.startswith(name)
            entries: list[dict] = await self.db.search(db_data.name.test(prefix_finder))

            all_data: list[tuple[str, bytes]] = []
            disappeared: list[str] = []

            for entry in entries:
                try:
                    filepath = self.blob_directory / entry["filename"]
                    async with open(filepath, "rb") as f:
                        data = await f.read()
                        all_data.append((entry["name"], data))
                except FileNotFoundError as err:
                    LOG.exception("Error loading blob: %s", err, exc_info=True)
                    disappeared.append(entry["filename"])

        if disappeared:
            await self._cleanup(disappeared)

        return all_data

    async def _cleanup(self, names: list[str]) -> None:
        async with self._state_mutex.writer_lock:
            for name in names:
                db_data = Query()
                await self.db.remove(db_data.name == name)
