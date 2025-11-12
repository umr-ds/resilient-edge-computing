from dataclasses import dataclass
from pathlib import Path
from shutil import rmtree
from uuid import uuid4

from hypothesis import strategies as st

from rec.dtn.eid import EID


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
