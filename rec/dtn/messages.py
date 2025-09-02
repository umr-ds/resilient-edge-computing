from dataclasses import dataclass
from enum import IntEnum
from typing import override
from uuid import UUID

from msgpack import packb, unpackb

from rec.util.log import LOG


@dataclass
class InvalidMessageError(Exception):
    data: dict

    def __str__(self) -> str:
        return f"Data is not valid message: {self.data}"


class MsgType(IntEnum):
    GENERAL_REPLY = 1
    JOBS_QUERY = 2
    JOBS_REPLY = 3


class MsgStatus(IntEnum):
    SUCCESS = 1
    FAILURE = 2


@dataclass
class Message:
    Type: MsgType
    Sender: str
    Recipient: str

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
class JobsQuery(Message):
    Submitter: str

    @override
    def dictify(self) -> dict:
        parent_dict = super().dictify()
        own_dict = self.__dict__
        return parent_dict | own_dict


@dataclass
class JobsReply(Reply):
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

    if data_dict["Type"] == MsgType.JOBS_QUERY:
        LOG.debug("Message is JobsQuery")
        return JobsQuery(**data_dict)
    if data_dict["Type"] == MsgType.JOBS_REPLY:
        LOG.debug("Message is JobsReply")
        return JobsReply(**data_dict)

    raise InvalidMessageError(data_dict)
