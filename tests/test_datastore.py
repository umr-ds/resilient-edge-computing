from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from hypothesis import assume, given
from hypothesis import strategies as st

from rec.datastore import Datastore
from rec.eid import BROADCAST_ADDRESS, EID
from rec.messages import BundleData, BundleType
from rec.node import NodeType
from tests.utils.helpers import dtn_eid, hierarchical_data


@pytest.mark.asyncio
@given(
    node_id=dtn_eid(not_none=True),  # ty: ignore[missing-argument]
    broker_id=dtn_eid(not_none=True),  # ty: ignore[missing-argument]
)
async def test_broker_discovery(node_id: EID, broker_id: EID) -> None:
    with TemporaryDirectory(delete=True) as root_dir:
        root_path = Path(root_dir)
        dstore = Datastore(
            node_id=node_id,
            dtn_agent_socket=Path("/tmp/dtn_agent.sock"),
            root_directory=root_path,
        )

        # broker announcement
        broker_bundle = BundleData(
            type=BundleType.BROKER_ANNOUNCE,
            source=broker_id,
            destination=BROADCAST_ADDRESS,
            node_type=NodeType.BROKER,
        )
        reply = await dstore._handle_bundle(bundle=broker_bundle)
        assert isinstance(reply, list)
        assert len(reply) == 1
        reply_bundle = reply[0]
        assert isinstance(reply_bundle, BundleData)
        assert reply_bundle.type == BundleType.BROKER_REQUEST
        assert reply_bundle.success
        assert reply_bundle.error == ""
        assert reply_bundle.node_type == NodeType.DATASTORE

        # broker ack
        broker_bundle = BundleData(
            type=BundleType.BROKER_ACK,
            source=broker_id,
            destination=node_id,
            node_type=NodeType.BROKER,
        )
        reply = await dstore._handle_bundle(bundle=broker_bundle)
        assert isinstance(reply, list)
        assert len(reply) == 0

        assert dstore._broker == broker_id


@pytest.mark.asyncio
@given(
    node_id=dtn_eid(not_none=True),  # ty: ignore[missing-argument]
    other_node_id=dtn_eid(not_none=True),  # ty: ignore[missing-argument]
    data_name=st.text(min_size=1),
    false_data_name=st.text(min_size=1),
    data=st.binary(),
)
async def test_store_load_single(
    node_id: EID, other_node_id: EID, data_name: str, false_data_name: str, data: bytes
) -> None:
    assume(not data_name.startswith(false_data_name))
    with TemporaryDirectory(delete=True) as root_dir:
        root_path = Path(root_dir)
        dstore = Datastore(
            node_id=node_id, dtn_agent_socket=Path(), root_directory=root_path
        )

        # store data in datastore
        store_msg = BundleData(
            type=BundleType.NDATA_PUT,
            source=other_node_id,
            destination=node_id,
            named_data=data_name,
            payload=data,
        )
        store_reply = await dstore._handle_bundle(bundle=store_msg)
        assert isinstance(store_reply, list)
        assert len(store_reply) == 1
        reply_bundle = store_reply[0]
        assert isinstance(reply_bundle, BundleData)
        assert reply_bundle.type == BundleType.NDATA_PUT
        assert reply_bundle.success
        assert reply_bundle.error == ""
        assert reply_bundle.named_data == data_name

        # Test correct lookup
        store_msg = BundleData(
            type=BundleType.NDATA_GET,
            source=other_node_id,
            destination=node_id,
            named_data=data_name,
        )
        store_reply = await dstore._handle_bundle(bundle=store_msg)
        assert isinstance(store_reply, list)
        assert len(store_reply) == 1
        reply_bundle = store_reply[0]
        assert isinstance(reply_bundle, BundleData)
        assert reply_bundle.type == BundleType.NDATA_GET
        assert reply_bundle.success
        assert reply_bundle.error == ""
        assert reply_bundle.named_data == data_name
        assert reply_bundle.payload == data

        # Test incorrect lookup
        store_msg = BundleData(
            type=BundleType.NDATA_GET,
            source=other_node_id,
            destination=node_id,
            named_data=false_data_name,
        )
        store_reply = await dstore._handle_bundle(bundle=store_msg)
        assert isinstance(store_reply, list)
        assert len(store_reply) == 0


@pytest.mark.asyncio
@given(
    node_id=dtn_eid(not_none=True),  # ty: ignore[missing-argument]
    other_node_id=dtn_eid(not_none=True),  # ty: ignore[missing-argument]
    data=hierarchical_data(),  # ty: ignore[missing-argument]
)
async def test_store_load_hierarchical(
    node_id: EID, other_node_id: EID, data: tuple[str, list[tuple[str, bytes]]]
) -> None:
    data_dict = dict(data[1])
    with TemporaryDirectory(delete=True) as root_dir:
        root_path = Path(root_dir)
        dstore = Datastore(
            node_id=node_id, dtn_agent_socket=Path(), root_directory=root_path
        )

        # store data in datastore
        for name, datum in data[1]:
            store_msg = BundleData(
                type=BundleType.NDATA_PUT,
                source=other_node_id,
                destination=node_id,
                named_data=name,
                payload=datum,
            )
            store_reply = await dstore._handle_bundle(bundle=store_msg)
            assert isinstance(store_reply, list)
            assert len(store_reply) == 1
            reply_bundle = store_reply[0]
            assert isinstance(reply_bundle, BundleData)
            assert reply_bundle.type == BundleType.NDATA_PUT
            assert reply_bundle.success
            assert reply_bundle.error == ""

        # prefix lookup
        store_msg = BundleData(
            type=BundleType.NDATA_GET,
            source=other_node_id,
            destination=node_id,
            named_data=data[0],
        )
        store_reply = await dstore._handle_bundle(bundle=store_msg)
        assert isinstance(store_reply, list)
        assert len(store_reply) == len(data[1])
        for reply_bundle in store_reply:
            assert isinstance(reply_bundle, BundleData)
            assert reply_bundle.type == BundleType.NDATA_GET
            assert reply_bundle.success
            assert reply_bundle.error == ""
            assert reply_bundle.named_data in data_dict
            assert data_dict[reply_bundle.named_data] == reply_bundle.payload
