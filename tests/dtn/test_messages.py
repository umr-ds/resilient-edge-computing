from hypothesis import given
from hypothesis import strategies as st

from rec.dtn.messages import (
    BundleCreate,
    BundleData,
    BundlePush,
    BundlePushStart,
    BundlePushStop,
    BundleType,
    MessageType,
    NodeType,
    Register,
    Reply,
    deserialize,
    serialize,
)
from tests.dtn.utils.helpers import dtn_eid


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
        source=draw(dtn_eid(not_none=True)),
        destination=draw(dtn_eid(not_none=True)),
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
        source=draw(dtn_eid(not_none=True)),
        destination=draw(dtn_eid(not_none=True)),
        success=success,
        error=error,
        payload=draw(st.binary()),
        submitter=draw(dtn_eid(not_none=True)),
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
        source=draw(dtn_eid(not_none=True)),
        destination=draw(dtn_eid(not_none=True)),
        success=success,
        error=error,
        payload=draw(st.binary()),
        named_data=draw(st.text(min_size=1)),
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
        endpoint_id=draw(dtn_eid(singleton=draw(st.booleans()), not_none=True)),
    )


@st.composite
def randomized_bundle_create(draw: st.DrawFn) -> BundleCreate:
    return BundleCreate(
        type=MessageType.BUNDLE_CREATE,
        bundle=draw(randomized_bundle()),
    )


@st.composite
def randomized_bundle_push_start(draw: st.DrawFn) -> BundlePushStart:
    return BundlePushStart(
        type=MessageType.BUNDLE_PUSH_START,
        endpoint_id=draw(dtn_eid(singleton=draw(st.booleans()), not_none=True)),
        node_type=NodeType(
            draw(st.integers(min_value=NodeType.BROKER, max_value=NodeType.CLIENT))
        ),
    )


@st.composite
def randomized_bundle_push_stop(draw: st.DrawFn) -> BundlePushStop:
    return BundlePushStop(
        type=MessageType.BUNDLE_PUSH_STOP,
        endpoint_id=draw(dtn_eid(singleton=draw(st.booleans()), not_none=True)),
    )


@st.composite
def randomized_bundle_push(draw: st.DrawFn) -> BundlePush:
    return BundlePush(
        type=MessageType.BUNDLE_PUSH,
        bundles=draw(st.lists(elements=randomized_bundle())),
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


@given(message=randomized_bundle_create())
def test_bundle_create_serialize(message: BundleCreate) -> None:
    serialized = serialize(message)
    deserialized = deserialize(serialized)
    assert deserialized == message


@given(message=randomized_bundle_push_start())
def test_bundle_push_start_serialize(message: BundlePushStart) -> None:
    serialized = serialize(message)
    deserialized = deserialize(serialized)
    assert deserialized == message


@given(message=randomized_bundle_push_stop())
def test_bundle_push_stop_serialize(message: BundlePushStop) -> None:
    serialized = serialize(message)
    deserialized = deserialize(serialized)
    assert deserialized == message


@given(message=randomized_bundle_push())
def test_bundle_push_serialize(message: BundlePush) -> None:
    serialized = serialize(message)
    deserialized = deserialize(serialized)
    assert deserialized == message
