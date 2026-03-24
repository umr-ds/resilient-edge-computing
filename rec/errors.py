from pathlib import Path
from typing import Self


# Base errors
class RecError(Exception):
    """Base class for all REC errors."""


class EIDError(RecError):
    """Base class for Endpoint ID-related errors."""


class JobError(RecError):
    """Base class for job-related errors."""


class CapabilityError(JobError):
    """Base class for capability validation errors."""


class WasmError(RecError, RuntimeError):
    """Base class for WASM setup/runtime errors."""


class MessageError(RecError, ValueError):
    """Base class for message-related validation errors."""


class BundleError(RecError, ValueError):
    """Base class for bundle-related validation errors."""


class NodeError(RecError):
    """Base class for node/transport related errors."""


class NodeConnectionError(NodeError, ConnectionError):
    """Base class for node connection-level transport errors."""


class ClientError(RecError):
    """Base class for client-related errors."""


class DataStoreError(RecError):
    """Base class for datastore-related errors."""


# EID errors
class InvalidDtnNodeError(EIDError):
    """Raised when an invalid DTN node name is used in an EID."""

    def __init__(self, node: str) -> None:
        self.node = node
        super().__init__(f"invalid dtn node '{node}'")


class InvalidDtnServiceError(EIDError):
    """Raised when an invalid DTN service name is used in an EID."""

    def __init__(self, service: str) -> None:
        self.service = service
        super().__init__(f"invalid dtn service '{service}'")


class InvalidIpnNodeError(EIDError):
    """Raised when an invalid IPN node number is used in an EID."""

    def __init__(self, node_number: int) -> None:
        self.node_number = node_number
        super().__init__(f"invalid ipn node number '{node_number}' (must be >= 1)")


class InvalidIpnServiceError(EIDError):
    """Raised when an invalid IPN service number is used in an EID."""

    def __init__(self, service_number: int) -> None:
        self.service_number = service_number
        super().__init__(
            f"invalid ipn service number '{service_number}' (must be >= 0)"
        )


class InvalidDtnNoneAliasError(EIDError):
    """Raised when dtn://none is used instead of the canonical dtn:none endpoint."""

    def __init__(self) -> None:
        super().__init__("invalid dtn host: use 'dtn:none', not 'dtn://none'")


class MissingDtnNodeError(EIDError):
    """Raised when a DTN EID omits the node component."""

    def __init__(self) -> None:
        super().__init__("invalid dtn eid: missing node")


class InvalidIpnSchemeSeparatorError(EIDError):
    """Raised when an IPN EID incorrectly uses // after the scheme."""

    def __init__(self) -> None:
        super().__init__("invalid ipn eid: must be 'ipn:N.S', not 'ipn://N.S'")


class InvalidIpnStructureError(EIDError):
    """Raised when an IPN EID is not in node.service form."""

    def __init__(self) -> None:
        super().__init__("invalid ipn eid: need exactly one dot (node.service)")


class InvalidIpnNumbersError(EIDError):
    """Raised when IPN node or service components are not valid integers."""

    def __init__(
        self,
        node_part: str,
        service_part: str,
    ) -> None:
        self.node_part = node_part
        self.service_part = service_part
        super().__init__(
            f"invalid ipn numbers: node='{node_part}', service='{service_part}'"
        )


class UnknownEIDSchemeError(EIDError):
    """Raised when an EID uses an unsupported URI scheme."""

    def __init__(self) -> None:
        super().__init__("unknown scheme (expected 'dtn:' or 'ipn:')")


# Job and capability errors
class InvalidCapabilityValueError(CapabilityError):
    """Raised when a capability field has an invalid value."""

    def __init__(self, field_name: str, value: int, requirement: str) -> None:
        self.field_name = field_name
        self.value = value
        self.requirement = requirement
        super().__init__(f"invalid {field_name} '{value}' ({requirement})")

    @classmethod
    def for_cpu_cores(cls, value: int) -> Self:
        return cls("cpu_cores", value, "must be positive")

    @classmethod
    def for_non_negative_field(cls, field_name: str, value: int) -> Self:
        return cls(field_name, value, "must be non-negative")

    @classmethod
    def for_free_cpu_capacity(cls, value: int) -> Self:
        return cls.for_non_negative_field("free_cpu_capacity", value)

    @classmethod
    def for_free_memory(cls, value: int) -> Self:
        return cls.for_non_negative_field("free_memory", value)

    @classmethod
    def for_free_disk_space(cls, value: int) -> Self:
        return cls.for_non_negative_field("free_disk_space", value)


class CapabilityValueTooLargeError(CapabilityError):
    """Raised when a capability field exceeds the MessagePack integer range."""

    def __init__(self, field_name: str, value: int, max_value: int) -> None:
        self.field_name = field_name
        self.value = value
        self.max_value = max_value
        super().__init__(f"invalid {field_name} '{value}' (must be <= {max_value})")


class MissingJobIdError(JobError):
    """Raised when a job is created without an ID."""

    def __init__(self) -> None:
        super().__init__("job needs an id")


class MissingWasmModuleError(JobError):
    """Raised when a job is created without a WASM module reference."""

    def __init__(self) -> None:
        super().__init__("job needs a wasm module")


class DataDirectoryEscapeError(JobError, ValueError):
    """Raised when a requested path escapes the configured data directory."""

    def __init__(self, path_value: str | Path) -> None:
        self.path_value = path_value
        super().__init__(f"path escapes data directory: {path_value}")


# WASM and executor filesystem errors
class WasmSetupError(WasmError):
    """Raised when Wasmtime/WASI setup or instantiation fails."""


class WasmTrapError(WasmError):
    """Raised when the module traps during execution (excluding proc_exit)."""


class WasmPathNotFoundError(WasmError, FileNotFoundError):
    """Raised when a required WASM-related path does not exist or is not a file."""

    def __init__(self, path_kind: str, path: Path) -> None:
        self.path_kind = path_kind
        self.path = path
        super().__init__(f"{path_kind} not found: {path}")

    @classmethod
    def for_wasm_binary(cls, path: Path) -> Self:
        return cls("wasm binary", path)

    @classmethod
    def for_stdin_file(cls, path: Path) -> Self:
        return cls("stdin file", path)


class DataDirectoryNotDirectoryError(WasmError, NotADirectoryError):
    """Raised when a provided data directory path exists but is not a directory."""

    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        super().__init__(f"data_dir is not a directory: {data_dir}")


class OutputPathNotFileError(WasmError, ValueError):
    """Raised when stdout/stderr output path exists but is not a regular file."""

    def __init__(self, stream_name: str, output_path: Path) -> None:
        self.stream_name = stream_name
        self.output_path = output_path
        super().__init__(f"{stream_name} is not a regular file: {output_path}")

    @classmethod
    def for_stdout(cls, output_path: Path) -> Self:
        return cls("stdout", output_path)

    @classmethod
    def for_stderr(cls, output_path: Path) -> Self:
        return cls("stderr", output_path)


class MissingStartExportError(WasmSetupError):
    """Raised when a module does not export the required WASI `_start` function."""

    def __init__(self) -> None:
        super().__init__("module does not export a `_start` function")


class UnexpectedWasmRuntimeError(WasmTrapError):
    """Raised for unexpected runtime errors while executing a WASI module."""

    def __init__(self) -> None:
        super().__init__("unexpected error during wasi module execution")


# Message and bundle validation errors
class InvalidMessageError(MessageError):
    """Raised when a message is malformed"""


class InvalidBundleError(BundleError):
    """Raised when a bundle is malformed"""


class MessageTypeMismatchError(InvalidMessageError):
    """Raised when a message instance has an unexpected MessageType."""

    def __init__(self, expected: object, actual: object) -> None:
        self.expected = expected
        self.actual = actual
        super().__init__(f"message needs message type {expected}, but has {actual}")


class MissingErrorForFailureError(InvalidMessageError):
    """Raised when an unsuccessful message/bundle omits its error string."""

    def __init__(self) -> None:
        super().__init__("if operation was unsuccessful, there should be an error")


class UnexpectedErrorForSuccessError(InvalidMessageError):
    """Raised when a successful message/bundle provides an error string."""

    def __init__(self) -> None:
        super().__init__("if operation was successful, there should be no error")


class EndpointMustNotBeNoneError(InvalidMessageError):
    """Raised when a registration endpoint is dtn:none."""

    def __init__(self) -> None:
        super().__init__("endpoint id must not be dtn:none")


class MissingBundleFieldError(InvalidBundleError):
    """Raised when a required bundle field is missing."""

    def __init__(self, field_name: str, requirement: str) -> None:
        self.field_name = field_name
        self.requirement = requirement
        super().__init__(f"bundle field '{field_name}' missing: {requirement}")

    @classmethod
    def for_source(cls) -> Self:
        return cls("source", "bundles must be sent by someone")

    @classmethod
    def for_destination(cls) -> Self:
        return cls("destination", "bundles must be addressed to someone")

    @classmethod
    def for_submitter(cls) -> Self:
        return cls("submitter", "job query/list bundles must have a submitter set")

    @classmethod
    def for_named_data(cls) -> Self:
        return cls("named_data", "named data bundles need to have a data name set")


class InvalidBundleNodeTypeError(InvalidBundleError):
    """Raised when a discovery bundle contains an unsupported node type."""

    def __init__(self, node_type: object) -> None:
        self.node_type = node_type
        super().__init__(f"invalid node type: {node_type}")


class MissingMessageTypeError(InvalidMessageError):
    """Raised when the message type field is missing."""

    def __init__(self) -> None:
        super().__init__("message missing 'type' field")


class UnknownMessageTypeIdError(InvalidMessageError):
    """Raised when a serialized type id cannot be mapped to MessageType."""

    def __init__(self, type_id: object) -> None:
        self.type_id = type_id
        super().__init__(f"unknown message type id: {type_id}")


class MissingMessageConstructorError(InvalidMessageError):
    """Raised when no constructor exists for a message type."""

    def __init__(self, msg_type: object) -> None:
        self.msg_type = msg_type
        super().__init__(f"no constructor defined for {msg_type}")


class MessageDeserializationFailedError(InvalidMessageError):
    """Raised when deserialization fails and raw payload has been dumped."""

    def __init__(self, dump_path: str, reason: object) -> None:
        self.dump_path = dump_path
        self.reason = reason
        super().__init__(
            f"deserialization failed: {reason}. Raw data dumped to {dump_path}"
        )


# Node transport errors
class BundlePushStartFailedError(NodeError):
    """Raised when dtnd rejects enabling push mode."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(f"failed to start push mode: {reason}")


class DtndConnectionClosedError(NodeConnectionError, ConnectionResetError):
    """Raised when dtnd closes the socket while data is expected."""

    def __init__(self) -> None:
        super().__init__("connection closed by dtnd")


class DtndSocketNotFoundError(NodeConnectionError, FileNotFoundError):
    """Raised when the configured dtnd Unix socket path does not exist."""

    def __init__(self, socket_path: Path) -> None:
        self.socket_path = socket_path
        super().__init__(f"dtnd socket not found: {socket_path}")


class DtndSocketOperationError(NodeConnectionError, OSError):
    """Raised when a socket operation against dtnd fails."""

    def __init__(self, operation: str) -> None:
        self.operation = operation
        super().__init__(f"dtnd socket {operation} failed")

    @classmethod
    def for_connect(cls) -> Self:
        return cls("connect")

    @classmethod
    def for_send(cls) -> Self:
        return cls("send")

    @classmethod
    def for_receive(cls) -> Self:
        return cls("receive")


class InvalidReplyLengthError(InvalidMessageError):
    """Raised when dtnd reports an invalid message length."""

    def __init__(self, reply_length: int) -> None:
        self.reply_length = reply_length
        super().__init__(f"invalid reply length: {reply_length}")


class NotConnectedToDtndError(NodeConnectionError):
    """Raised when trying to send on a missing dtnd connection."""

    def __init__(self) -> None:
        super().__init__("not connected to dtnd")


# Client errors
class BrokerNotAssociatedError(ClientError):
    """Raised when a broker is expected to be associated with a client but is not."""

    def __init__(self) -> None:
        super().__init__("no broker associated; this should not happen at this point")


class MissingDataError(ClientError):
    """Raised when an execution plan references data that is not available."""

    def __init__(self, missing_paths: list[str]) -> None:
        self.missing_paths = missing_paths
        super().__init__(f"data files with paths '{missing_paths}' are not available")


class InvalidContextError(ClientError):
    """Raised when client tries to parse an invalid context file"""

    def __init__(self, field: str) -> None:
        super().__init__(f"Invalid context field: {field}")
        self.field = field


# Datastore errors
class NameTakenError(DataStoreError):
    """Raised when trying to store data with a name that is already taken."""

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(f"name '{name}' already taken")


class NoSuchNameError(DataStoreError):
    """Raised when trying to access data with a name that does not exist."""

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(f"name '{name}' not found")
