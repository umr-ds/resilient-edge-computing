import asyncio
from pathlib import Path

from rec.dtn.messages import *
from rec.dtn.node import Node
from rec.dtn.storage import NameTakenError, Storage
from rec.util.log import LOG


class Datastore(Node):
    _root_directory: Path
    _storage: Storage

    def __init__(
        self, node_id: str | EID, dtn_agent_socket: str, root_directory: str | Path
    ) -> None:
        super().__init__(
            node_id=node_id,
            dtn_agent_socket=dtn_agent_socket,
            node_type=NodeType.DATASTORE,
        )
        if isinstance(root_directory, str):
            self._root_directory = Path(root_directory)
        else:
            self._root_directory = root_directory
        self._root_directory.mkdir(parents=True, exist_ok=True)

        db_path = self._root_directory / "database.db"
        blob_directory = self._root_directory / "blobs"
        self._storage = Storage(db_path, blob_directory)

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
                bundles = await self._get_new_bundles()
                if bundles:
                    LOG.debug(f"Bundles: {bundles}")
                    for bundle in bundles:
                        await self._handle_bundle(bundle=bundle)
                else:
                    LOG.debug("No new bundles")
            except Exception as err:
                LOG.exception("Error fetching bundles: %s", err)

    async def _handle_bundle(self, bundle: BundleData) -> None:
        LOG.debug(f"Handling bundle: {bundle}")
        to_send: list[BundleData] = []

        if BundleType.BROKER_ANNOUNCE <= bundle.type <= BundleType.BROKER_ACK:
            to_send = await self._handle_discovery(bundle=bundle)
        if BundleType.NDATA_PUT <= bundle.type <= BundleType.NDATA_DEL:
            to_send = await self._handle_data(bundle=bundle)

        LOG.debug(f"Response bundles: {to_send}")

        if to_send:
            try:
                LOG.debug("Sending bundles")
                dtnd_responses = await self._send_bundles(bundles=to_send)
                for dtnd_response in dtnd_responses:
                    if not dtnd_response.success:
                        LOG.exception("dtnd sent error: %s", dtnd_response.error)
            except Exception as err:
                LOG.exception("error communicating with dtnd: %s", err, exc_info=True)

    async def _handle_data(self, bundle: BundleData) -> list[BundleData]:
        LOG.debug("Named data bundle")
        bundles: list[BundleData] = []
        match bundle.type:
            case BundleType.NDATA_PUT:
                LOG.debug("Data action is PUT")
                if bundle.named_data is None:
                    LOG.error("Name was none, this should never happen")
                    return bundles
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
                    try:
                        await self._storage.store_data(name=name, data=bundle.payload)
                    except NameTakenError as err:
                        response.success = False
                        response.error = str(err)
                    bundles.append(response)
            case BundleType.NDATA_GET:
                LOG.debug("Data action is GET")
                if bundle.named_data is None:
                    LOG.error("Name was none, this should never happen")
                    return bundles
                if isinstance(bundle.named_data, str):
                    bundle.named_data = [bundle.named_data]
                for name in bundle.named_data:
                    loaded = await self._storage.load_data(name=name)
                    LOG.debug(f"Loaded data: {loaded}")
                    for l_name, l_data in loaded:
                        response = BundleData(
                            type=BundleType.NDATA_GET,
                            source=self.node_id,
                            destination=bundle.source,
                            payload=l_data,
                            named_data=l_name,
                        )
                        bundles.append(response)
            case _:
                LOG.error(f"Received bundle of type {bundle.type}, ignoring")

        return bundles
