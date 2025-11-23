import asyncio
from pathlib import Path
from typing import override

from rec.dtn.eid import EID
from rec.dtn.messages import BundleData, BundleType, NodeType
from rec.dtn.node import Node
from rec.dtn.storage import NameTakenError, Storage
from rec.util.log import LOG


class Datastore(Node):
    _storage: Storage

    def __init__(
        self, node_id: EID, dtn_agent_socket: Path, root_directory: Path
    ) -> None:
        super().__init__(
            _node_id=node_id,
            _dtn_agent_socket=dtn_agent_socket,
            _node_type=NodeType.DATASTORE,
        )

        root_directory.mkdir(parents=True, exist_ok=True)

        db_path = root_directory / "database.db"
        blob_directory = root_directory / "blobs"
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
                    replies: list[BundleData] = []
                    for bundle in bundles:
                        bundle_replies = await self._handle_bundle(bundle=bundle)
                        replies.extend(bundle_replies)

                    if replies:
                        await self._send_and_check(bundles=replies)
                else:
                    LOG.debug("No new bundles")
            except Exception as err:
                LOG.exception("Error fetching bundles: %s", err)

    async def _handle_bundle(self, bundle: BundleData) -> list[BundleData]:
        LOG.debug(f"Handling bundle: {bundle}")
        to_send: list[BundleData] = []

        if BundleType.BROKER_ANNOUNCE <= bundle.type <= BundleType.BROKER_ACK:
            to_send = await self._handle_discovery(bundle=bundle)
        if BundleType.NDATA_PUT <= bundle.type <= BundleType.NDATA_DEL:
            to_send = await self._handle_data(bundle=bundle)

        LOG.debug(f"Response bundles: {to_send}")
        return to_send

    async def _handle_data(self, bundle: BundleData) -> list[BundleData]:
        LOG.debug("Named data bundle")
        bundles: list[BundleData] = []
        if isinstance(bundle.named_data, str):
            named_data = [bundle.named_data]
        else:
            named_data = bundle.named_data

        if named_data is None:
            LOG.error("Name was none, this should never happen")
            return bundles

        match bundle.type:
            case BundleType.NDATA_PUT:
                LOG.debug("Data action is PUT")

                success = True
                error = ""
                for name in named_data:
                    try:
                        await self._storage.store_data(name=name, data=bundle.payload)
                    except NameTakenError as err:
                        success = False
                        error = str(err)

                    response = BundleData(
                        type=BundleType.NDATA_PUT,
                        source=self._node_id,
                        destination=bundle.source,
                        named_data=name,
                        success=success,
                        error=error,
                    )
                    bundles.append(response)
            case BundleType.NDATA_GET:
                LOG.debug("Data action is GET")

                for name in named_data:
                    loaded = await self._storage.load_data(name=name)
                    LOG.debug(f"Loaded data: {loaded}")
                    for l_name, l_data in loaded:
                        response = BundleData(
                            type=BundleType.NDATA_GET,
                            source=self._node_id,
                            destination=bundle.source,
                            payload=l_data,
                            named_data=l_name,
                        )
                        bundles.append(response)
            case _:
                LOG.error(f"Received bundle of type {bundle.type}, ignoring")

        return bundles
