import pytest

from hypothesis import given

from rec.dtn.datastore import *

from .test_helpers import *


@pytest.mark.asyncio
@given(dtn_id=dtn_eid(), data_name=st.text(), data=st.binary())
async def test_store_retrieve(dtn_id: str, data_name: str, data: bytes) -> None:
    with TmpDirectory(prefix="/tmp") as tmp_path:
        store = Datastore(node_id=dtn_id, dtn_agent_socket="", rootdir=tmp_path)
        await store._store_data(name=data_name, data=data)
        retrieved = await store._load_data(name=data_name)
        assert retrieved == data
