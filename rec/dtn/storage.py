import shutil
from dataclasses import dataclass
from hashlib import sha1
from pathlib import Path
from typing import override

from aiofiles import open
from aiorwlock import RWLock
from asynctinydb import Query, TinyDB

from rec.util.log import LOG


@dataclass
class NameTakenError(Exception):
    """Raised when trying to store data with a name that is already taken."""

    name: str

    @override
    def __str__(self) -> str:
        return f"name {self.name} already taken"


@dataclass
class NoSuchNameError(Exception):
    """Raised when trying to access data with a name that does not exist."""

    name: str

    @override
    def __str__(self) -> str:
        return f"no such name: {self.name}"


class Storage:
    """
    Async-safe named data storage system with deduplication capabilities.

    This class provides a persistent storage layer that maps human-readable names to binary data.
    It uses SHA1 hashing to deduplicate identical data and a TinyDB database to maintain name-to-hash mappings.
    """

    _db: TinyDB
    _blob_directory: Path
    _state_mutex: RWLock

    def __init__(self, db_path: Path, blob_directory: Path) -> None:
        """
        Initialize the storage system.

        Args:
            db_path: Path to the TinyDB database file.
            blob_directory: Directory where actual data blobs will be stored.
        """
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._db = TinyDB(db_path)
        self._blob_directory = blob_directory
        self._blob_directory.mkdir(parents=True, exist_ok=True)

        self._state_mutex = RWLock()

    async def store_data(self, name: str, data: bytes) -> None:
        """
        Stores data on disk.

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
            test = await self._db.search(db_data.name == name)
            if test:
                raise NameTakenError(name=name)

            filename = sha1(data, usedforsecurity=False).hexdigest()

            filepath = self._blob_directory / filename

            # dedup - if the file already exists, we don't need to create it
            if filepath.exists():
                await self._db.insert({"name": name, "filename": filename})
                return

            async with open(self._blob_directory / filename, "wb") as f:
                await f.write(data)
            await self._db.insert({"name": name, "filename": filename})

    async def load_data(self, name: str) -> list[tuple[str, bytes]]:
        """
        Loads data from disk.

        Looks up name in database and tries to load binary data from blobs directory.

        Args:
            name (str): "human-readable" name of data. May be split in hierarchical parts with '/' separator.
                        If `name` is a common prefix for multiple datas, then we return all of them.

        Returns:
            list(tuple(str, bytes)): List of (name, data) of all data with names that started with `name`.
        """
        async with self._state_mutex.reader_lock:
            db_data = Query()

            # TODO: this has linear complexity with the number of stored data. Might be more efficient to build a tree-structure.
            #       but I'm not sure if the performance warrants the increased complexity...
            def prefix_finder(s: str) -> bool:
                return s.startswith(name)

            entries: list[dict] = await self._db.search(
                db_data.name.test(prefix_finder)
            )

            all_data: list[tuple[str, bytes]] = []
            disappeared: list[str] = []

            for entry in entries:
                try:
                    filepath = self._blob_directory / entry["filename"]
                    async with open(filepath, "rb") as f:
                        data = await f.read()
                        all_data.append((entry["name"], data))
                except FileNotFoundError as err:
                    LOG.exception("Error loading blob: %s", err, exc_info=True)
                    disappeared.append(entry["filename"])

        if disappeared:
            await self._cleanup(disappeared)

        return all_data

    async def find_missing(self, names: set[str]) -> set[str]:
        """
        Find which of the names in the given set are missing from storage.

        Args:
            names: Set of names to check for.

        Returns:
            set[str]: Set of names that are missing from storage.
        """
        if not names:
            return set()

        async with self._state_mutex.reader_lock:
            missing = set()
            db_data = Query()
            for name in names:
                entries = await self._db.search(db_data.name == name)
                if not entries:
                    missing.add(name)

        return missing

    async def copy_to_file(self, name: str, destination: Path) -> None:
        """
        Copy stored data directly to a file.

        Args:
            name: The name of the stored data.
            destination: Path where to copy the data.

        Raises:
            NoSuchNameError: If the named data doesn't exist.
        """
        async with self._state_mutex.reader_lock:
            db_data = Query()
            entries = await self._db.search(db_data.name == name)

            if not entries:
                raise NoSuchNameError(name=name)

            filename = entries[0]["filename"]
            source_path = self._blob_directory / filename

            if source_path.exists():
                missing_filename = None
                shutil.copyfile(source_path, destination)
            else:
                missing_filename = filename

        if missing_filename:
            await self._cleanup([missing_filename])
            raise NoSuchNameError(name=name)

    async def _cleanup(self, names: list[str]) -> None:
        """
        Remove orphaned database entries for missing blob files.

        Args:
            names: List of data names whose blob files are missing.
        """
        async with self._state_mutex.writer_lock:
            for name in names:
                db_data = Query()
                await self._db.remove(db_data.name == name)
