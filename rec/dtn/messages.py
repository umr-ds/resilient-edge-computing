from __future__ import annotations

import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from enum import IntEnum
from typing import override

from ormsgpack import packb, unpackb

from rec.dtn.eid import EID

# (2^64)-1
MSGPACK_MAXINT = 18446744073709551615


class InvalidMessageError(ValueError):
    """Raised when a message is malformed"""


class InvalidBundleError(ValueError):
    """Raised when a bundle is malformed"""


class NodeType(IntEnum):
    NONE = 0
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


@dataclass(frozen=True)
class Message:
    type: MessageType

    def dictify(self) -> dict:
        return self.__dict__

    @classmethod
    def from_dict(cls, data) -> Message:
        return cls(**data)


@dataclass(frozen=True)
class Reply(Message):
    success: bool
    error: str

    def __post_init__(self) -> None:
        if self.type != MessageType.REPLY:
            raise InvalidMessageError(
                f"Message needs MessageType {MessageType.REPLY}, but has {self.type}"
            )
        if not self.success and not self.error:
            raise InvalidMessageError(
                "If operation was unsuccessful, there should be an error"
            )
        if self.success and self.error:
            raise InvalidMessageError(
                "If operation was successful, there should be no error"
            )

    @override
    def dictify(self) -> dict:
        parent_dict = super().dictify()
        own_dict = self.__dict__
        return parent_dict | own_dict

    @classmethod
    def from_dict(cls, data) -> Reply:
        return cls(**data)


@dataclass(frozen=True)
class Register(Message):
    endpoint_id: EID

    def __post_init__(self) -> None:
        if self.type != MessageType.REGISTER:
            raise InvalidMessageError(
                f"Message needs MessageType {MessageType.REPLY}, but has {self.type}"
            )
        if not self.endpoint_id:
            raise InvalidMessageError("EndpointID must not be dtn:none")

    @override
    def dictify(self) -> dict:
        parent_dict = super().dictify()
        own_dict = self.__dict__
        return parent_dict | own_dict

    @classmethod
    def from_dict(cls, data) -> Register:
        return cls(**data)


@dataclass(frozen=True)
class Fetch(Message):
    endpoint_id: EID
    node_type: NodeType

    def __post_init__(self) -> None:
        if self.type != MessageType.FETCH:
            raise InvalidMessageError(
                f"Message needs MessageType {MessageType.REPLY}, but has {self.type}"
            )
        if not self.endpoint_id:
            raise InvalidMessageError("EndpointID must not be dtn:none")

    @override
    def dictify(self) -> dict:
        parent_dict = super().dictify()
        own_dict = self.__dict__
        return parent_dict | own_dict

    @classmethod
    def from_dict(cls, data) -> Fetch:
        return cls(**data)


@dataclass(frozen=True)
class FetchReply(Reply):
    bundles: list[BundleData]

    def __post_init__(self) -> None:
        if self.type != MessageType.FETCH_REPLY:
            raise InvalidMessageError(
                f"Message needs MessageType {MessageType.REPLY}, but has {self.type}"
            )

    @override
    def dictify(self) -> dict:
        parent_dict = super().dictify()
        own_dict = {"bundles": [bundle.dictify() for bundle in self.bundles]}
        return parent_dict | own_dict

    @classmethod
    def from_dict(cls, data) -> FetchReply:
        data["bundles"] = [
            BundleData.from_dict(bundle_data) for bundle_data in data["bundles"]
        ]
        return cls(**data)


@dataclass(frozen=True)
class BundleCreate(Message):
    bundle: BundleData

    def __post_init__(self) -> None:
        if self.type != MessageType.CREATE:
            raise InvalidMessageError(
                f"Message needs MessageType {MessageType.REPLY}, but has {self.type}"
            )

    @override
    def dictify(self) -> dict:
        parent_dict = super().dictify()
        own_dict = {"bundle": self.bundle.dictify()}
        return parent_dict | own_dict

    @classmethod
    def from_dict(cls, data) -> BundleCreate:
        data["bundle"] = BundleData.from_dict(data["bundle"])
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
    submitter: EID = EID.none()
    # used by named data
    named_data: str = ""

    def __post_init__(self) -> None:
        # general validity checks
        if not self.source:
            raise InvalidBundleError("Bundles must be sent by someone")
        if not self.destination:
            raise InvalidBundleError("Bundles must be addressed to someone")
        if not self.success and not self.error:
            raise InvalidBundleError(
                "If operation was unsuccessful, there should be an error"
            )
        if self.success and self.error:
            raise InvalidBundleError(
                "If operation was successful, there should be no error"
            )

        # checks for discovery bundles
        if BundleType.BROKER_ANNOUNCE <= self.type <= BundleType.BROKER_ACK:
            if self.node_type < NodeType.BROKER or self.node_type > NodeType.CLIENT:
                raise InvalidBundleError(f"Invalid node type: {self.node_type}")

        # checks for job query/list
        if self.type == BundleType.JOB_QUERY or self.type == BundleType.JOB_LIST:
            if not self.submitter:
                raise InvalidBundleError(
                    "Job query/list bundles must have a submitter set"
                )

        # checks for named data
        if BundleType.NDATA_PUT <= self.type <= BundleType.NDATA_DEL:
            if not self.named_data:
                raise InvalidBundleError(
                    "Named data bundles need to have a data name set"
                )

    def dictify(self) -> dict:
        data = {key: value for key, value in self.__dict__.items() if value}
        data["success"] = self.success
        return data

    @classmethod
    def from_dict(cls, data: dict) -> BundleData:
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
    MessageType.FETCH: Fetch.from_dict,
    MessageType.FETCH_REPLY: FetchReply.from_dict,
    MessageType.CREATE: BundleCreate.from_dict,
}


def deserialize(serialized: bytes) -> Message:
    try:
        data_dict: dict = unpackb(serialized)

        if "type" not in data_dict:
            raise InvalidMessageError("Message missing 'type' field")

        try:
            msg_type = MessageType(data_dict["type"])
        except ValueError:
            raise InvalidMessageError(f"Unknown MessageType ID: {data_dict['type']}")

        if msg_type not in MESSAGE_CONSTRUCTORS:
            raise InvalidMessageError(f"No constructor defined for {msg_type}")

        return MESSAGE_CONSTRUCTORS[msg_type](data_dict)

    except Exception as err:
        # Write the serialized data to a temp file for debugging
        prefix = "rec_msg_dump_"

        with tempfile.NamedTemporaryFile(
            delete=False, prefix=prefix, suffix=".bin"
        ) as tmp_file:
            tmp_file.write(serialized)
            tmp_filename = tmp_file.name

        raise InvalidMessageError(
            f"Deserialization failed: {err}. Raw data dumped to {tmp_filename}"
        ) from err
