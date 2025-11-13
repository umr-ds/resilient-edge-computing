from collections import Counter

import pytest
from hypothesis import given
from msgpack import unpackb

from rec.dtn.broker import Broker
from rec.dtn.eid import BROADCAST_ADDRESS
from rec.dtn.job import job_infos_from_dicts
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


@pytest.mark.asyncio
@given(
    broker_id=dtn_eid(),
    queued_jobs=st.lists(elements=randomized_job_info(submitter=EID.dtn("client"))),
    completed_jobs=st.sets(elements=randomized_job_info(submitter=EID.dtn("client"))),
)
async def test_broker_job_query(
    broker_id: EID, queued_jobs: list[JobInfo], completed_jobs: set[JobInfo]
) -> None:
    client_id = EID.dtn("client")
    broker = Broker(node_id=broker_id, dtn_agent_socket="")
    broker.completed_jobs = completed_jobs
    for job in queued_jobs:
        broker.queued_jobs.put(job)

    job_query = BundleData(
        type=BundleType.JOB_QUERY,
        source=client_id,
        destination=broker_id,
        submitter=client_id,
    )

    response = await broker._handle_bundle(bundle=job_query)
    assert response.type == BundleType.JOB_LIST
    assert response.source == broker_id
    assert response.destination == client_id
    assert response.success
    assert not response.error
    assert response.payload

    list_jobs = unpackb(response.payload)
    assert isinstance(list_jobs, dict)

    assert Counter(["completed", "queued"]) == Counter(list_jobs.keys())

    list_completed = job_infos_from_dicts(list_jobs["completed"])
    assert Counter(completed_jobs) == Counter(list_completed)

    list_queued = job_infos_from_dicts(list_jobs["queued"])
    assert Counter(queued_jobs) == Counter(list_queued)
