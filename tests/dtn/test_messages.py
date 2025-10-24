from hypothesis import given

from rec.dtn.messages import *
from tests.dtn.utils.helpers import *


@st.composite
def randomized_discovery_bundle(draw: st.DrawFn) -> BundleData:
    success = draw(st.booleans())
    if not success:
        error = draw(st.text(min_size=1))
    else:
        error = ""
    return BundleData(
        type=draw(
            st.sampled_from(
                (
                    BundleType.BROKER_ANNOUNCE,
                    BundleType.BROKER_REQUEST,
                    BundleType.BROKER_ACK,
                )
            )
        ),
        source=draw(dtn_eid()),
        destination=draw(dtn_eid()),
        success=success,
        error=error,
        node_type=draw(
            st.sampled_from(
                (
                    NodeType.BROKER,
                    NodeType.EXECUTOR,
                    NodeType.DATASTORE,
                    NodeType.CLIENT,
                )
            )
        ),
    )


@st.composite
def randomized_job_bundle(draw: st.DrawFn) -> BundleData:
    success = draw(st.booleans())
    if not success:
        error = draw(st.text(min_size=1))
    else:
        error = ""
    return BundleData(
        type=draw(
            st.sampled_from(
                (
                    BundleType.JOB_SUBMIT,
                    BundleType.JOB_RESULT,
                    BundleType.JOB_QUERY,
                    BundleType.JOB_LIST,
                )
            )
        ),
        source=draw(dtn_eid()),
        destination=draw(dtn_eid()),
        success=success,
        error=error,
        payload=draw(st.binary()),
        submitter=draw(dtn_eid()),
    )


@st.composite
def randomized_data_bundle(draw: st.DrawFn) -> BundleData:
    success = draw(st.booleans())
    if not success:
        error = draw(st.text(min_size=1))
    else:
        error = ""
    return BundleData(
        type=draw(
            st.sampled_from(
                (
                    BundleType.NDATA_PUT,
                    BundleType.NDATA_GET,
                    BundleType.NDATA_DEL,
                )
            )
        ),
        source=draw(dtn_eid()),
        destination=draw(dtn_eid()),
        success=success,
        error=error,
        payload=draw(st.binary()),
        named_data=draw(
            st.one_of(
                st.text(min_size=1), st.lists(elements=st.text(min_size=1), min_size=1)
            )
        ),
    )


@st.composite
def randomized_bundle(draw: st.DrawFn) -> BundleData:
    return draw(
        st.one_of(
            (
                randomized_discovery_bundle(),
                randomized_job_bundle(),
                randomized_data_bundle(),
            )
        )
    )


@st.composite
def randomized_reply(draw: st.DrawFn) -> Reply:
    success = draw(st.booleans())
    if not success:
        error = draw(st.text(min_size=1))
    else:
        error = ""
    return Reply(
        type=MessageType.REPLY,
        success=success,
        error=error,
    )


@st.composite
def randomized_register(draw: st.DrawFn) -> Register:
    return Register(
        type=MessageType.REGISTER,
        endpoint_id=draw(dtn_eid(singleton=draw(st.booleans()))),
    )


@st.composite
def randomized_fetch(draw: st.DrawFn) -> Fetch:
    return Fetch(
        type=MessageType.FETCH,
        endpoint_id=draw(dtn_eid(singleton=draw(st.booleans()))),
        node_type=NodeType(
            draw(st.integers(min_value=NodeType.BROKER, max_value=NodeType.CLIENT))
        ),
    )


@st.composite
def randomized_fetch_reply(draw: st.DrawFn) -> FetchReply:
    success = draw(st.booleans())
    if not success:
        error = draw(st.text(min_size=1))
    else:
        error = ""
    return FetchReply(
        type=MessageType.FETCH_REPLY,
        bundles=draw(st.lists(elements=randomized_bundle())),
        success=success,
        error=error,
    )


@st.composite
def randomized_bundle_create(draw: st.DrawFn) -> BundleCreate:
    return BundleCreate(
        type=MessageType.CREATE,
        bundle=draw(randomized_bundle()),
    )


@given(message=randomized_reply())
def test_reply_serialize(message: Reply) -> None:
    serialized = serialize(message)
    deserialized = deserialize(serialized)
    assert deserialized == message


@given(message=randomized_register())
def test_register_serialize(message: Register) -> None:
    serialized = serialize(message)
    deserialized = deserialize(serialized)
    assert deserialized == message


@given(message=randomized_fetch())
def test_fetch_serialize(message: Fetch) -> None:
    serialized = serialize(message)
    deserialized = deserialize(serialized)
    assert deserialized == message


@given(message=randomized_fetch_reply())
def test_fetch_reply_serialize(message: FetchReply) -> None:
    serialized = serialize(message)
    deserialized = deserialize(serialized)
    assert deserialized == message


@given(message=randomized_bundle_create())
def test_bundle_create_serialize(message: BundleCreate) -> None:
    serialized = serialize(message)
    deserialized = deserialize(serialized)
    assert deserialized == message
