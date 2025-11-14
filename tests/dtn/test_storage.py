import pytest
from hypothesis import assume, given
from hypothesis import strategies as st

from rec.dtn.storage import Storage
from tests.dtn.utils.helpers import TmpDirectory, hierarchical_data


@pytest.mark.asyncio
@given(data_name=st.text(), data=st.binary())
async def test_store(data_name: str, data: bytes) -> None:
    with TmpDirectory(prefix="/tmp") as tmp_path:
        storage = Storage(
            db_path=tmp_path / "database.db", blob_directory=tmp_path / "blobs"
        )

        await storage.store_data(name=data_name, data=data)


@pytest.mark.asyncio
@given(data_name=st.text(), other_name=st.text(), data=st.binary())
async def test_store_dedup(data_name: str, other_name: str, data: bytes) -> None:
    assume(data_name != other_name)
    with TmpDirectory(prefix="/tmp") as tmp_path:
        blobs_path = tmp_path / "blobs"
        storage = Storage(db_path=tmp_path / "database.db", blob_directory=blobs_path)

        await storage.store_data(name=data_name, data=data)
        await storage.store_data(name=other_name, data=data)
        files = [filename for filename in blobs_path.iterdir()]
        assert len(files) == 1, "there should only be 1 file due to dedup"


@pytest.mark.asyncio
@given(data_name=st.text(), data=st.binary())
async def test_store_retrieve(data_name: str, data: bytes) -> None:
    with TmpDirectory(prefix="/tmp") as tmp_path:
        storage = Storage(
            db_path=tmp_path / "database.db", blob_directory=tmp_path / "blobs"
        )

        await storage.store_data(name=data_name, data=data)
        retrieved = await storage.load_data(name=data_name)
        assert len(retrieved) == 1
        assert retrieved[0][0] == data_name
        assert retrieved[0][1] == data


@pytest.mark.asyncio
@given(data_name=st.text())
async def test_retrieve_empty_storage(data_name: str) -> None:
    with TmpDirectory(prefix="/tmp") as tmp_path:
        storage = Storage(
            db_path=tmp_path / "database.db", blob_directory=tmp_path / "blobs"
        )

        retrieved = await storage.load_data(name=data_name)
        assert isinstance(retrieved, list)
        assert len(retrieved) == 0


@pytest.mark.asyncio
@given(data_name=st.text(), other_data_name=st.text(), data=st.binary())
async def test_wrong_lookup_name(
    data_name: str, other_data_name: str, data: bytes
) -> None:
    assume(not data_name.startswith(other_data_name))
    with TmpDirectory(prefix="/tmp") as tmp_path:
        storage = Storage(
            db_path=tmp_path / "database.db", blob_directory=tmp_path / "blobs"
        )

        await storage.store_data(name=data_name, data=data)
        retrieved = await storage.load_data(name=other_data_name)
        assert isinstance(retrieved, list)
        assert len(retrieved) == 0


@pytest.mark.asyncio
@given(data=hierarchical_data())
async def test_prefixing(data: tuple[str, list[tuple[str, bytes]]]) -> None:
    with TmpDirectory(prefix="/tmp") as tmp_path:
        storage = Storage(
            db_path=tmp_path / "database.db", blob_directory=tmp_path / "blobs"
        )

        for name, datum in data[1]:
            await storage.store_data(name=name, data=datum)

        retrieved = await storage.load_data(data[0])
        assert len(retrieved) == len(data[1])

        for i in range(len(retrieved)):
            assert retrieved[i][0] == data[1][i][0]
            assert retrieved[i][1] == data[1][i][1]


@pytest.mark.asyncio
@given(names=st.sets(st.text()), required=st.sets(st.text()))
async def test_find_missing(names: set[str], required: set[str]) -> None:
    with TmpDirectory(prefix="/tmp") as tmp_path:
        storage = Storage(
            db_path=tmp_path / "database.db", blob_directory=tmp_path / "blobs"
        )

        for name in names:
            await storage.store_data(name=name, data=b"test")

        missing = await storage.find_missing(required)
        expected_missing = required - names
        assert missing == expected_missing


@pytest.mark.asyncio
@given(data_name=st.text(), data=st.binary())
async def test_copy_to_file(data_name: str, data: bytes) -> None:
    with TmpDirectory(prefix="/tmp") as tmp_path:
        storage = Storage(
            db_path=tmp_path / "database.db", blob_directory=tmp_path / "blobs"
        )

        await storage.store_data(name=data_name, data=data)

        dest_file = tmp_path / "copied_data.bin"
        await storage.copy_to_file(data_name, dest_file)

        assert dest_file.exists()
        assert dest_file.read_bytes() == data
