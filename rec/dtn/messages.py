from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import override
from uuid import UUID

from msgpack import packb, unpackb

from rec.dtn.node import NodeType
from rec.util.log import LOG


@dataclass
class InvalidMessageError(Exception):
    data: dict

    def __str__(self) -> str:
        return f"Data is not valid message: {self.data}"


class MsgType(IntEnum):
    REPLY = 1
    REGISTER = 2
    FETCH = 3
    BUNDLE_REPLY = 4
    JOBS_QUERY = 5
    JOBS_REPLY = 6


class MsgStatus(IntEnum):
    SUCCESS = 1
    FAILURE = 2


@dataclass
class Message:
    Type: MsgType

    def dictify(self) -> dict:
        return self.__dict__


@dataclass
class Reply(Message):
    Status: MsgStatus
    Text: str

    @override
    def dictify(self) -> dict:
        parent_dict = super().dictify()
        own_dict = self.__dict__
        return parent_dict | own_dict


@dataclass
class Register(Message):
    EID: str

    @override
    def dictify(self) -> dict:
        parent_dict = super().dictify()
        own_dict = self.__dict__
        return parent_dict | own_dict


@dataclass
class Fetch(Message):
    EID: str
    NType: NodeType

    @override
    def dictify(self) -> dict:
        parent_dict = super().dictify()
        own_dict = self.__dict__
        return parent_dict | own_dict


@dataclass
class FetchReply(Reply):
    Messages: list[BundleMessage]

    @override
    def dictify(self) -> dict:
        parent_dict = super().dictify()
        own_dict = {"Messages": [message.dictify() for message in self.Messages]}
        return parent_dict | own_dict


@dataclass
class BundleMessage(Message):
    Sender: str
    Recipient: str

    @override
    def dictify(self) -> dict:
        parent_dict = super().dictify()
        own_dict = self.__dict__
        return parent_dict | own_dict


@dataclass
class BundleReply(Reply):
    Sender: str
    Recipient: str

    @override
    def dictify(self) -> dict:
        parent_dict = super().dictify()
        own_dict = self.__dict__
        return parent_dict | own_dict


@dataclass
class JobsQuery(BundleMessage):
    Submitter: str

    @override
    def dictify(self) -> dict:
        parent_dict = super().dictify()
        own_dict = self.__dict__
        return parent_dict | own_dict


@dataclass
class JobsReply(BundleReply):
    Queued: list[UUID]
    Completed: list[UUID]

    @override
    def dictify(self) -> dict:
        parent_dict = super().dictify()
        own_dict = {
            "Queued": [str(uid) for uid in self.Queued],
            "Completed": [str(uid) for uid in self.Completed],
        }
        return parent_dict | own_dict


def serialize(message: Message) -> bytes:
    data = message.dictify()
    return packb(data)


def deserialize(data: bytes) -> Message:
    data_dict = unpackb(data)

    match data_dict["Type"]:
        case MsgType.REPLY:
            LOG.debug("Message is control reply")
            return Reply(**data_dict)
        case _:
            raise InvalidMessageError(data_dict)
