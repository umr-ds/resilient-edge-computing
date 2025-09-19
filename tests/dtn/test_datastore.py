import pytest

from hypothesis import given, assume

from rec.dtn.datastore import *

from .test_helpers import *


@st.composite
def hierarchical_data(draw: st.DrawFn) -> tuple[str, list[tuple[str, bytes]]]:
    levels: list[str] = draw(st.lists(elements=st.text()))
    prefix = "/".join(levels)

    names: list[str] = draw(st.lists(st.text(), unique=True))
    data = []
    for name in names:
        data.append((f"{prefix}/{name}", draw(st.binary())))

    return prefix, data


@pytest.mark.asyncio
@given(dtn_id=dtn_eid(), data_name=st.text(), data=st.binary())
async def test_store(dtn_id: str, data_name: str, data: bytes) -> None:
    with TmpDirectory(prefix="/tmp") as tmp_path:
        store = Datastore(node_id=dtn_id, dtn_agent_socket="", root_directory=tmp_path)
        await store.store_data(name=data_name, data=data)


@pytest.mark.asyncio
@given(dtn_id=dtn_eid(), data_name=st.text(), other_name=st.text(), data=st.binary())
async def test_store_dedup(
    dtn_id: str, data_name: str, other_name: str, data: bytes
) -> None:
    assume(data_name != other_name)
    with TmpDirectory(prefix="/tmp") as tmp_path:
        blobs_path = tmp_path / "blobs"
        store = Datastore(node_id=dtn_id, dtn_agent_socket="", root_directory=tmp_path)
        await store.store_data(name=data_name, data=data)
        await store.store_data(name=other_name, data=data)
        files = [filename for filename in blobs_path.iterdir()]
        assert len(files) == 1, "there should only be 1 file due to dedup"


@pytest.mark.asyncio
@given(dtn_id=dtn_eid(), data_name=st.text(), data=st.binary())
async def test_store_retrieve(dtn_id: str, data_name: str, data: bytes) -> None:
    with TmpDirectory(prefix="/tmp") as tmp_path:
        store = Datastore(node_id=dtn_id, dtn_agent_socket="", root_directory=tmp_path)
        await store.store_data(name=data_name, data=data)
        retrieved = await store.load_data(name=data_name)
        assert len(retrieved) == 1
        assert retrieved[0][0] == data_name
        assert retrieved[0][1] == data


@pytest.mark.asyncio
@given(dtn_id=dtn_eid(), data=hierarchical_data())
async def test_prefixing(
    dtn_id: str, data: tuple[str, list[tuple[str, bytes]]]
) -> None:
    with TmpDirectory(prefix="/tmp") as tmp_path:
        store = Datastore(node_id=dtn_id, dtn_agent_socket="", root_directory=tmp_path)

        for name, datum in data[1]:
            await store.store_data(name=name, data=datum)

        retrieved = await store.load_data(data[0])
        assert len(retrieved) == len(data[1])

        for i in range(len(retrieved)):
            assert retrieved[i][0] == data[1][i][0]
            assert retrieved[i][1] == data[1][i][1]
