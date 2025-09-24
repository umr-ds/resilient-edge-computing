from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import override

from msgpack import packb, unpackb

from rec.dtn.eid import EID


@dataclass
class InvalidMessageError(Exception):
    data: dict

    def __str__(self) -> str:
        return f"Data is not valid message: {self.data}"


class NodeType(IntEnum):
    BROKER = 1
    EXECUTOR = 2
    DATASTORE = 3
    CLIENT = 4


class MessageType(IntEnum):
    REPLY = 1
    REGISTER = 2
    FETCH = 3
    FETCH_REPLY = 4
    CREATE = 5


@dataclass
class Message:
    type: MessageType

    def dictify(self) -> dict:
        return self.__dict__


@dataclass
class Reply(Message):
    success: bool
    error: str

    @override
    def dictify(self) -> dict:
        parent_dict = super().dictify()
        own_dict = self.__dict__
        return parent_dict | own_dict


@dataclass
class Register(Message):
    endpoint_id: EID

    @override
    def dictify(self) -> dict:
        parent_dict = super().dictify()
        own_dict = self.__dict__
        return parent_dict | own_dict


@dataclass
class Fetch(Message):
    endpoint_id: EID
    node_type: NodeType

    @override
    def dictify(self) -> dict:
        parent_dict = super().dictify()
        own_dict = self.__dict__
        return parent_dict | own_dict


@dataclass
class FetchReply(Reply):
    bundles: list[BundleData | dict]

    def __post_init__(self) -> None:
        self.bundles = [
            BundleData(**bundle) for bundle in self.bundles if isinstance(bundle, dict)
        ] + [bundle for bundle in self.bundles if isinstance(bundle, BundleData)]

    @override
    def dictify(self) -> dict:
        parent_dict = super().dictify()
        own_dict = {"bundles": [message.dictify() for message in self.bundles]}
        return parent_dict | own_dict


@dataclass
class BundleCreate(Message):
    bundle: BundleData | dict

    def __post_init__(self) -> None:
        if isinstance(self.bundle, dict):
            self.bundle = BundleData(**self.bundle)

    @override
    def dictify(self) -> dict:
        parent_dict = super().dictify()
        own_dict = {"bundle": self.bundle.dictify()}
        return parent_dict | own_dict


BROADCAST_ADDRESS = EID.dtn("rec.all", "~")
BROKER_MULTICAST_ADDRESS = EID.dtn("rec.broker", "~")
DATASTORE_MULTICAST_ADDRESS = EID.dtn("rec.store", "~")
EXECUTOR_MULTICAST_ADDRESS = EID.dtn("rec.executor", "~")
CLIENT_MULTICAST_ADDRESS = EID.dtn("rec.client", "~")


class BundleType(IntEnum):
    # 1-10: Broker discovery
    BROKER_ANNOUNCE = 1
    BROKER_REQUEST = 2
    BROKER_ACK = 3

    # 11-20: Jobs
    JOB_SUBMIT = 11
    JOB_RESULT = 12
    JOB_QUERY = 13
    JOB_LIST = 14

    # 21-30: Named Data
    NDATA_PUT = 21
    NDATA_GET = 22
    NDATA_DEL = 23


@dataclass
class BundleData:
    type: BundleType
    source: EID
    destination: EID
    payload: bytes = b""
    success: bool = True
    error: str = ""
    # used by broker discovery
    node_type: NodeType = 0
    # used by job query/list
    submitter: EID | None = None
    # used by named data
    named_data: str | list[str] | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.source, EID):
            self.source = EID(self.source)
        if not isinstance(self.destination, EID):
            self.destination = EID(self.destination)

        if self.submitter is not None:
            self.submitter = EID(self.submitter)

    def dictify(self) -> dict:
        own_dict = self.__dict__

        if self.payload == b"":
            del own_dict["payload"]

        if self.node_type == 0:
            del own_dict["node_type"]

        if self.submitter is None:
            del own_dict["submitter"]

        if self.named_data is None:
            del own_dict["named_data"]

        return own_dict


def serialize(message: Message) -> bytes:
    data = message.dictify()
    return packb(data)


MESSAGE_CONSTRUCTORS: dict[MessageType, type[Message]] = {
    MessageType.REPLY: Reply,
    MessageType.REGISTER: Register,
    MessageType.FETCH: Fetch,
    MessageType.FETCH_REPLY: FetchReply,
    MessageType.CREATE: BundleCreate,
}


def deserialize(data: bytes) -> Message:
    data_dict: dict = unpackb(data)

    if data_dict["type"] not in MESSAGE_CONSTRUCTORS:
        raise InvalidMessageError(data_dict)

    return MESSAGE_CONSTRUCTORS[data_dict["type"]](**data_dict)
