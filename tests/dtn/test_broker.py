from typing import override

import pytest
from hypothesis import given

from rec.dtn.broker import Broker
from rec.dtn.eid import BROADCAST_ADDRESS
from rec.dtn.messages import BundleData, BundleType
from rec.dtn.node import NodeType
from tests.dtn.utils.helpers import *


@pytest.mark.asyncio
@given(
    broker_id=dtn_eid(),
    node_id=dtn_eid(),
    node_type=st.integers(min_value=2, max_value=4),
)
async def test_broker_discovery(broker_id: EID, node_id: EID, node_type: int) -> None:
    broker = Broker(node_id=broker_id, dtn_agent_socket="")

    # announcement from other broker
    bundle = BundleData(
        type=BundleType.BROKER_ANNOUNCE,
        source=node_id,
        destination=BROADCAST_ADDRESS,
        node_type=NodeType.BROKER,
    )
    reply = await broker._handle_bundle(bundle=bundle)
    assert reply is None
    if broker_id != node_id:
        assert node_id in broker.discovered_nodes[NodeType.BROKER]

    # other node responds to announcements
    bundle = BundleData(
        type=BundleType.BROKER_REQUEST,
        source=node_id,
        destination=broker_id,
        node_type=NodeType(node_type),
    )
    reply = await broker._handle_bundle(bundle=bundle)
    assert isinstance(reply, BundleData)
    assert reply.type == BundleType.BROKER_ACK
    assert reply.node_type == NodeType.BROKER
    assert reply.success
    assert reply.error == ""
    assert node_id in broker.discovered_nodes[NodeType(node_type)]
