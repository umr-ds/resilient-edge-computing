from dataclasses import dataclass
from pathlib import Path
from shutil import rmtree
from uuid import uuid4

from hypothesis import strategies as st

from rec.dtn.eid import EID


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
