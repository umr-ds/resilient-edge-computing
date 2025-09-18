import pytest

from hypothesis import given, assume

from rec.dtn.datastore import *

from .test_helpers import *


@pytest.mark.asyncio
@given(dtn_id=dtn_eid(), data_name=st.text(), data=st.binary())
async def test_store(dtn_id: str, data_name: str, data: bytes) -> None:
    with TmpDirectory(prefix="/tmp") as tmp_path:
        store = Datastore(node_id=dtn_id, dtn_agent_socket="", root_directory=tmp_path)
        await store._store_data(name=data_name, data=data)


@pytest.mark.asyncio
@given(dtn_id=dtn_eid(), data_name=st.text(), other_name=st.text(), data=st.binary())
async def test_store_dedup(
    dtn_id: str, data_name: str, other_name: str, data: bytes
) -> None:
    assume(data_name != other_name)
    with TmpDirectory(prefix="/tmp") as tmp_path:
        blobs_path = tmp_path / "blobs"
        store = Datastore(node_id=dtn_id, dtn_agent_socket="", root_directory=tmp_path)
        await store._store_data(name=data_name, data=data)
        await store._store_data(name=other_name, data=data)
        files = [filename for filename in blobs_path.iterdir()]
        assert len(files) == 1, "there should only be 1 file due to dedup"


@pytest.mark.asyncio
@given(dtn_id=dtn_eid(), data_name=st.text(), data=st.binary())
async def test_store_retrieve(dtn_id: str, data_name: str, data: bytes) -> None:
    with TmpDirectory(prefix="/tmp") as tmp_path:
        store = Datastore(node_id=dtn_id, dtn_agent_socket="", root_directory=tmp_path)
        await store._store_data(name=data_name, data=data)
        retrieved = await store._load_data(name=data_name)
        assert retrieved == data
