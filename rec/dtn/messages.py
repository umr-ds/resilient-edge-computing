from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import override

from msgpack import packb, unpackb

from rec.dtn.model.eid import EID


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


@dataclass(init=False)
class FetchReply(Reply):
    bundles: list[BundleData]

    def __init__(self, *args, bundles: list[dict], **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.bundles = [BundleData(**bundle) for bundle in bundles]

    @override
    def dictify(self) -> dict:
        parent_dict = super().dictify()
        own_dict = {"bundles": [message.dictify() for message in self.bundles]}
        return parent_dict | own_dict


@dataclass(init=False)
class BundleCreate(Message):
    bundle: BundleData

    def __init__(self, *args, bundle: dict | BundleData, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        if isinstance(bundle, BundleData):
            self.bundle = bundle
        else:
            self.bundle = BundleData(**bundle)

    @override
    def dictify(self) -> dict:
        parent_dict = super().dictify()
        own_dict = {"bundle": self.bundle.dictify()}
        return parent_dict | own_dict


BROKER_MULTICAST_ADDRESS = EID.dtn("rec.broker", "~")
DATASTORE_MULTICAST_ADDRESS = EID.dtn("rec.store", "~")
EXECUTOR_MULTICAST_ADDRESS = EID.dtn("rec.executor", "~")
CLIENT_MULTICAST_ADDRESS = EID.dtn("rec.client", "~")


class BundleType(IntEnum):
    JOBS_QUERY = 1
    JOBS_REPLY = 2


@dataclass
class BundleData:
    type: BundleType
    source: EID
    destination: EID
    payload: bytes
    submitter: EID = EID.none()

    def dictify(self) -> dict:
        own_dict = self.__dict__
        if self.submitter == EID.none():
            del own_dict["submitter"]

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
