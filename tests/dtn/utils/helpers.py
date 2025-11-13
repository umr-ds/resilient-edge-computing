from dataclasses import dataclass
from pathlib import Path
from shutil import rmtree
from uuid import uuid4

from hypothesis import strategies as st

from rec.dtn.eid import EID
from rec.dtn.job import Capabilities, JobInfo
from rec.dtn.messages import MSGPACK_MAXINT


@dataclass
class TmpDirectory:
    prefix: str

    def __enter__(self) -> Path:
        path = Path(f"{self.prefix}/{uuid4()}")
        self.path = path
        path.mkdir(parents=True)
        return path

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        rmtree(self.path)


@st.composite
def dtn_eid(draw: st.DrawFn, singleton=True) -> EID:
    node: str = draw(
        st.text(
            alphabet=st.characters(
                codec="ascii",
                categories=["L", "N"],
                include_characters=[
                    "-",
                    ".",
                    "_",
                    "~",
                    "!",
                    "$",
                    "&",
                    "'",
                    "(",
                    ")",
                    "*",
                    "+",
                    ",",
                    ";",
                    "=",
                ],
            )
        )
    )
    service: str = draw(st.text(alphabet=st.characters(codec="ascii")))
    if singleton:
        eid = EID.dtn(node=node, service=service)
    else:
        eid = EID.dtn(node=node, service=f"~{service}")
    return eid


@st.composite
def hierarchical_data(draw: st.DrawFn) -> tuple[str, list[tuple[str, bytes]]]:
    levels: list[str] = draw(st.lists(elements=st.text(min_size=1), min_size=1))
    prefix = "/".join(levels)

    names: list[str] = draw(st.lists(st.text(), unique=True))
    data = []
    for name in names:
        data.append((f"{prefix}/{name}", draw(st.binary())))

    return prefix, data


@st.composite
def randomized_capabilities(draw: st.DrawFn) -> Capabilities:
    capabilities = Capabilities(
        cpu_cores=draw(st.integers(min_value=1, max_value=MSGPACK_MAXINT)),
        free_cpu_capacity=draw(st.integers(min_value=0, max_value=MSGPACK_MAXINT)),
        free_memory=draw(st.integers(min_value=0, max_value=MSGPACK_MAXINT)),
        free_disk_space=draw(st.integers(min_value=0, max_value=MSGPACK_MAXINT)),
    )
    return capabilities


@st.composite
def randomized_job_info(draw: st.DrawFn, submitter: EID | None = None) -> JobInfo:
    if submitter is None:
        submitter = draw(dtn_eid())
    job_info = JobInfo(
        job_id=draw(st.uuids()),
        submitter=submitter,
        wasm_module=draw(st.text(min_size=1)),
        capabilities=draw(randomized_capabilities()),
        argv=draw(st.lists(elements=st.text())),
        env=draw(st.dictionaries(keys=st.text(), values=st.text())),
        stdin_file=draw(st.one_of(st.text(), st.none())),
        dirs=draw(st.lists(elements=st.text())),
        data=draw(st.dictionaries(keys=st.text(), values=st.text())),
        stdout_file=draw(st.one_of(st.text(), st.none())),
        stderr_file=draw(st.one_of(st.text(), st.none())),
        results=draw(st.lists(elements=st.text())),
        named_results=draw(st.dictionaries(keys=st.text(), values=st.text())),
        results_receiver=draw(st.one_of(dtn_eid(), st.none())),
    )
    return job_info
