from __future__ import annotations

import tempfile
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Self, override
from uuid import UUID, uuid4

from ormsgpack import packb, unpackb

from rec.eid import EID
from rec.errors import (
    EndpointMustNotBeNoneError,
    InvalidBundleNodeTypeError,
    MessageDeserializationFailedError,
    MessageTypeMismatchError,
    MissingBundleFieldError,
    MissingErrorForFailureError,
    MissingMessageConstructorError,
    MissingMessageTypeError,
    UnexpectedErrorForSuccessError,
    UnknownMessageTypeIdError,
)

# (2^64)-1
MSGPACK_MAXINT = 18446744073709551615


class NodeType(IntEnum):
    NONE = 0
    BROKER = 1
    EXECUTOR = 2
    DATASTORE = 3
    CLIENT = 4


class MessageType(IntEnum):
    REPLY = 1
    REGISTER = 2
    BUNDLE_CREATE = 3
    BUNDLE_PUSH_START = 4
    BUNDLE_PUSH_STOP = 5
    BUNDLE_PUSH = 6


@dataclass(frozen=True, kw_only=True)
class Message:
    type: MessageType
    message_id: UUID = field(default_factory=uuid4)

    def dictify(self) -> dict:
        data = dict(self.__dict__)
        data["message_id"] = self.message_id.bytes
        return data

    @classmethod
    def from_dict(cls, data: dict) -> Self:
        return cls(**data)


@dataclass(frozen=True)
class Reply(Message):
    success: bool
    error: str

    def __post_init__(self) -> None:
        if self.type != MessageType.REPLY:
            raise MessageTypeMismatchError(MessageType.REPLY, self.type)
        if not self.success and not self.error:
            raise MissingErrorForFailureError
        if self.success and self.error:
            raise UnexpectedErrorForSuccessError

    @override
    def dictify(self) -> dict:
        return self.__dict__ | super().dictify()

    @classmethod
    def from_dict(cls, data: dict) -> Self:
        return cls(**data)


@dataclass(frozen=True)
class Register(Message):
    endpoint_id: EID

    def __post_init__(self) -> None:
        if self.type != MessageType.REGISTER:
            raise MessageTypeMismatchError(MessageType.REGISTER, self.type)
        if not self.endpoint_id:
            raise EndpointMustNotBeNoneError

    @override
    def dictify(self) -> dict:
        return self.__dict__ | super().dictify()

    @classmethod
    def from_dict(cls, data: dict) -> Self:
        return cls(**data)


@dataclass(frozen=True)
class BundleCreate(Message):
    bundle: BundleData

    def __post_init__(self) -> None:
        if self.type != MessageType.BUNDLE_CREATE:
            raise MessageTypeMismatchError(MessageType.BUNDLE_CREATE, self.type)

    @override
    def dictify(self) -> dict:
        own_dict = {"bundle": self.bundle.dictify()}
        return self.__dict__ | super().dictify() | own_dict

    @classmethod
    def from_dict(cls, data: dict) -> Self:
        data["bundle"] = BundleData.from_dict(data["bundle"])
        return cls(**data)


@dataclass(frozen=True)
class BundlePushStart(Message):
    def __post_init__(self) -> None:
        if self.type != MessageType.BUNDLE_PUSH_START:
            raise MessageTypeMismatchError(MessageType.BUNDLE_PUSH_START, self.type)

    @override
    def dictify(self) -> dict:
        return self.__dict__ | super().dictify()

    @classmethod
    def from_dict(cls, data: dict) -> Self:
        return cls(**data)


@dataclass(frozen=True)
class BundlePushStop(Message):
    def __post_init__(self) -> None:
        if self.type != MessageType.BUNDLE_PUSH_STOP:
            raise MessageTypeMismatchError(MessageType.BUNDLE_PUSH_STOP, self.type)

    @override
    def dictify(self) -> dict:
        return self.__dict__ | super().dictify()

    @classmethod
    def from_dict(cls, data: dict) -> Self:
        return cls(**data)


@dataclass(frozen=True)
class BundlePush(Message):
    bundles: list[BundleData]

    def __post_init__(self) -> None:
        if self.type != MessageType.BUNDLE_PUSH:
            raise MessageTypeMismatchError(MessageType.BUNDLE_PUSH, self.type)

    @override
    def dictify(self) -> dict:
        own_dict = {"bundles": [bundle.dictify() for bundle in self.bundles]}
        return self.__dict__ | super().dictify() | own_dict

    @classmethod
    def from_dict(cls, data: dict) -> Self:
        data["bundles"] = [
            BundleData.from_dict(bundle_data) for bundle_data in data["bundles"]
        ]
        return cls(**data)


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


@dataclass(frozen=True)
class BundleData:
    type: BundleType
    source: EID
    destination: EID
    payload: bytes = b""
    success: bool = True
    error: str = ""
    # used by broker discovery
    node_type: NodeType = NodeType.NONE
    # used by job query/list
    submitter: EID = field(default_factory=EID.none)
    # used by named data
    named_data: str = ""

    def __post_init__(self) -> None:
        # general validity checks
        if not self.source:
            raise MissingBundleFieldError.for_source()
        if not self.destination:
            raise MissingBundleFieldError.for_destination()
        if not self.success and not self.error:
            raise MissingErrorForFailureError
        if self.success and self.error:
            raise UnexpectedErrorForSuccessError

        # checks for discovery bundles
        if (BundleType.BROKER_ANNOUNCE <= self.type <= BundleType.BROKER_ACK) and (
            self.node_type < NodeType.BROKER or self.node_type > NodeType.CLIENT
        ):
            raise InvalidBundleNodeTypeError(self.node_type)

        # checks for job query/list
        if (
            self.type in (BundleType.JOB_QUERY, BundleType.JOB_LIST)
        ) and not self.submitter:
            raise MissingBundleFieldError.for_submitter()

        # checks for named data
        if (
            BundleType.NDATA_PUT <= self.type <= BundleType.NDATA_DEL
        ) and not self.named_data:
            raise MissingBundleFieldError.for_named_data()

    def dictify(self) -> dict:
        data = {key: value for key, value in self.__dict__.items() if value}
        data["success"] = self.success
        return data

    @classmethod
    def from_dict(cls, data: dict) -> Self:
        data["source"] = EID(data["source"])
        data["destination"] = EID(data["destination"])
        if "submitter" in data:
            data["submitter"] = EID(data["submitter"])

        return cls(**data)


def serialize(message: Message) -> bytes:
    data = message.dictify()
    return packb(data)


MESSAGE_CONSTRUCTORS: dict[MessageType, Callable[[dict], Message]] = {
    MessageType.REPLY: Reply.from_dict,
    MessageType.REGISTER: Register.from_dict,
    MessageType.BUNDLE_CREATE: BundleCreate.from_dict,
    MessageType.BUNDLE_PUSH_START: BundlePushStart.from_dict,
    MessageType.BUNDLE_PUSH_STOP: BundlePushStop.from_dict,
    MessageType.BUNDLE_PUSH: BundlePush.from_dict,
}


def _extract_message_type(data_dict: dict) -> MessageType:
    if "type" not in data_dict:
        raise MissingMessageTypeError

    try:
        return MessageType(data_dict["type"])
    except ValueError as err:
        raise UnknownMessageTypeIdError(data_dict["type"]) from err


def _get_constructor(msg_type: MessageType) -> Callable[[dict], Message]:
    if msg_type not in MESSAGE_CONSTRUCTORS:
        raise MissingMessageConstructorError(msg_type)
    return MESSAGE_CONSTRUCTORS[msg_type]


def deserialize(serialized: bytes) -> Message:
    try:
        data_dict: dict = unpackb(serialized)

        msg_type = _extract_message_type(data_dict)
        constructor = _get_constructor(msg_type)

        if "message_id" in data_dict:
            data_dict["message_id"] = UUID(bytes=data_dict["message_id"])

        return constructor(data_dict)

    except Exception as err:
        # Write the serialized data to a temp file for debugging
        prefix = "rec_msg_dump_"

        with tempfile.NamedTemporaryFile(
            delete=False, prefix=prefix, suffix=".bin"
        ) as tmp_file:
            tmp_file.write(serialized)
            tmp_filename = tmp_file.name

        raise MessageDeserializationFailedError(tmp_filename, err) from err
