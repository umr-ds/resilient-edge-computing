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
        await super().run()

        if self._bundle_receive_task:
            await self._bundle_receive_task

    @override
    async def _handle_bundle(self, bundle: BundleData) -> list[BundleData]:
        replies: list[BundleData] = []

        if BundleType.NDATA_PUT <= bundle.type <= BundleType.NDATA_DEL:
            replies = await self._handle_data(bundle=bundle)
        elif BundleType.BROKER_ANNOUNCE <= bundle.type <= BundleType.BROKER_ACK:
            replies = await self._handle_discovery(bundle=bundle)
        else:
            LOG.warning(f"Won't handle bundle of type: {bundle.type}")

        return replies

    async def _handle_data(self, bundle: BundleData) -> list[BundleData]:
        LOG.debug("Named data bundle")

        if not bundle.named_data:
            LOG.error(
                "Received NDATA bundle with no name set. "
                "This indicates a malformed bundle from the sender. "
                "Ignoring."
            )
            return []

        bundles: list[BundleData] = []

        match bundle.type:
            case BundleType.NDATA_PUT:
                LOG.debug("Data action is PUT")

                success = True
                error = ""
                try:
                    await self._storage.store_data(
                        name=bundle.named_data, data=bundle.payload
                    )
                except NameTakenError as err:
                    success = False
                    error = str(err)

                response = BundleData(
                    type=BundleType.NDATA_PUT,
                    source=self._node_id,
                    destination=bundle.source,
                    named_data=bundle.named_data,
                    success=success,
                    error=error,
                )
                bundles.append(response)
            case BundleType.NDATA_GET:
                LOG.debug("Data action is GET")

                loaded = await self._storage.load_data(name=bundle.named_data)
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
