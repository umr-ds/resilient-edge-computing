from pathlib import Path
from typing import override

import pytest
from hypothesis import given
from hypothesis import strategies as st

from rec.dtn.eid import BROADCAST_ADDRESS, EID
from rec.dtn.messages import BundleData, BundleType
from rec.dtn.node import Node, NodeType
from tests.dtn.utils.helpers import dtn_eid


class DummyNode(Node):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @override
    async def run(self) -> None:
        pass

    @override
    async def _handle_bundle(self, bundle: BundleData) -> list[BundleData]:
        return []


@pytest.mark.asyncio
@given(
    node_id=dtn_eid(not_none=True),
    broker_id=dtn_eid(not_none=True),
    node_type=st.integers(min_value=NodeType.BROKER, max_value=NodeType.CLIENT),
)
async def test_broker_discovery(node_id: EID, broker_id: EID, node_type: int) -> None:
    node = DummyNode(_node_id=node_id, _dtn_agent_socket=Path(), _node_type=node_type)

    # broker announcement
    broker_bundle = BundleData(
        type=BundleType.BROKER_ANNOUNCE,
        source=broker_id,
        destination=BROADCAST_ADDRESS,
        node_type=NodeType.BROKER,
    )

    reply = await node._handle_discovery(bundle=broker_bundle)
    assert isinstance(reply, list)
    assert len(reply) == 1
    reply_bundle = reply[0]
    assert isinstance(reply_bundle, BundleData)
    assert reply_bundle.type == BundleType.BROKER_REQUEST
    assert reply_bundle.source == node_id
    assert reply_bundle.destination == broker_id
    assert reply_bundle.node_type == node_type
    assert reply_bundle.success
    assert reply_bundle.error == ""

    # broker ack
    broker_bundle = BundleData(
        type=BundleType.BROKER_ACK,
        source=broker_id,
        destination=node_id,
        node_type=NodeType.BROKER,
    )
    reply = await node._handle_discovery(bundle=broker_bundle)
    assert isinstance(reply, list)
    assert len(reply) == 0

    assert node._broker == broker_id
