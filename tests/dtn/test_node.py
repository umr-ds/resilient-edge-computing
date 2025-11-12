from typing import override

import pytest
from hypothesis import given

from rec.dtn.eid import BROADCAST_ADDRESS
from rec.dtn.messages import BundleData, BundleType
from rec.dtn.node import Node, NodeType
from tests.dtn.utils.helpers import *


class DummyNode(Node):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @override
    async def run(self) -> None:
        pass


@pytest.mark.asyncio
@given(
    node_id=dtn_eid(),
    broker_id=dtn_eid(),
    node_type=st.integers(min_value=NodeType.BROKER, max_value=NodeType.CLIENT),
)
async def test_broker_discovery(node_id: EID, broker_id: EID, node_type: int) -> None:
    node = DummyNode(node_id=node_id, dtn_agent_socket="", node_type=node_type)

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
