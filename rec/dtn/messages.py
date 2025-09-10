from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import override

from msgpack import packb, unpackb


@dataclass
class InvalidMessageError(Exception):
    data: dict

    def __str__(self) -> str:
        return f"Data is not valid message: {self.data}"


class MessageType(IntEnum):
    REPLY = 1
    REGISTER = 2
    FETCH = 3
    FETCH_REPLY = 4
    CREATE = 5


class NodeType(IntEnum):
    BROKER = 1
    EXECUTOR = 2
    DATASTORE = 3
    CLIENT = 4


@dataclass
class Message:
    Type: MessageType

    def dictify(self) -> dict:
        return self.__dict__


@dataclass
class Reply(Message):
    Success: bool
    Error: str

    @override
    def dictify(self) -> dict:
        parent_dict = super().dictify()
        own_dict = self.__dict__
        return parent_dict | own_dict


@dataclass
class Register(Message):
    EndpointID: str

    @override
    def dictify(self) -> dict:
        parent_dict = super().dictify()
        own_dict = self.__dict__
        return parent_dict | own_dict


@dataclass
class Fetch(Message):
    EndpointID: str
    NodeType: NodeType

    @override
    def dictify(self) -> dict:
        parent_dict = super().dictify()
        own_dict = self.__dict__
        return parent_dict | own_dict


@dataclass(init=False)
class FetchReply(Reply):
    Bundles: list[BundleData]

    def __init__(self, *args, Bundles: list[dict], **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.Bundles = []
        for bundle in Bundles:
            self.Bundles.append(BundleData(**bundle))

    @override
    def dictify(self) -> dict:
        parent_dict = super().dictify()
        own_dict = {"Bundles": [message.dictify() for message in self.Bundles]}
        return parent_dict | own_dict


@dataclass(init=False)
class BundleCreate(Message):
    Bundle: BundleData

    def __init__(self, *args, Bundle: dict | BundleData, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        if isinstance(Bundle, BundleData):
            self.Bundle = Bundle
        else:
            self.Bundle = BundleData(**Bundle)

    @override
    def dictify(self) -> dict:
        parent_dict = super().dictify()
        own_dict = {"Bundle": self.Bundle.dictify()}
        return parent_dict | own_dict


class BundleType(IntEnum):
    JOBS_QUERY = 1
    JOBS_REPLY = 2


@dataclass
class BundleData:
    Type: BundleType
    Source: str
    Destination: str
    Payload: bytes
    Metadata: dict[str, str]

    def dictify(self) -> dict:
        return self.__dict__


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
    data_dict = unpackb(data)

    if data_dict["Type"] not in MESSAGE_CONSTRUCTORS:
        raise InvalidMessageError(data_dict)

    return MESSAGE_CONSTRUCTORS[data_dict["Type"]](**data_dict)
