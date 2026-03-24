import re
from typing import Self

from rec.errors import (
    InvalidDtnNodeError,
    InvalidDtnNoneAliasError,
    InvalidDtnServiceError,
    InvalidIpnNodeError,
    InvalidIpnNumbersError,
    InvalidIpnSchemeSeparatorError,
    InvalidIpnServiceError,
    InvalidIpnStructureError,
    MissingDtnNodeError,
    UnknownEIDSchemeError,
)

IPN_PARTS = 2


class EID(str):
    """Typed string for DTN/IPN Endpoint IDs."""

    # Just a string subclass, no per-instance dict
    __slots__ = ()

    _DTN_NONE = "dtn:none"
    _DTN_PREFIX = "dtn://"
    _IPN_PREFIX = "ipn:"
    # Based on:
    #  - RFC 3986
    #     - Section 3.2.2 and Appendix A (see `reg-name` and `unreserved`)
    #     - https://www.rfc-editor.org/rfc/rfc3986#appendix-A
    #  - RFC 9171
    #    - Section 4.2.5.1.1
    #    - https://www.rfc-editor.org/rfc/rfc9171.html#name-the-dtn-uri-scheme
    _NODE_RE = re.compile(r"(^$)|(^[A-Za-z0-9-._~!$&'()*+,;=]+$)")

    def __new__(cls, value: str) -> Self:
        norm = cls._normalize(value)
        return super().__new__(cls, norm)

    def __bool__(self) -> bool:
        return self != EID._DTN_NONE

    @classmethod
    def dtn(cls, node: str, service: str | None = None) -> Self:
        """
        Create a DTN EndpointID from node and optional service.
        Use `EID.none()` for the `dtn:none` endpoint.

        Args:
            node (str): DTN node name.
            service (str | None): DTN service name or `None`. Defaults to `None`.

        Returns:
            EID: The constructed DTN EndpointID.

        Raises:
            InvalidDtnNodeError: If the node name is invalid.
            InvalidDtnServiceError: If the service name is invalid.
        """
        if service == "":
            service = None

        if not cls._NODE_RE.match(node):
            raise InvalidDtnNodeError(node)
        if service is None:
            return cls(f"{cls._DTN_PREFIX}{node}/")
        if not service.isascii():
            raise InvalidDtnServiceError(service)
        return cls(f"{cls._DTN_PREFIX}{node}/{service}")

    @classmethod
    def ipn(cls, node_number: int, service_number: int) -> Self:
        """
        Create an IPN EndpointID from node and service numbers.

        Args:
            node_number (int): IPN node number (must be >= 1).
            service_number (int): IPN service number (must be >= 0).

        Returns:
            EID: The constructed IPN EndpointID.

        Raises:
            InvalidIpnNodeError: If the node number is invalid.
            InvalidIpnServiceError: If the service number is invalid.
        """
        if node_number < 1:
            raise InvalidIpnNodeError(node_number)
        if service_number < 0:
            raise InvalidIpnServiceError(service_number)
        return cls(f"{cls._IPN_PREFIX}{node_number}.{service_number}")

    @classmethod
    def none(cls) -> Self:
        """
        Get the singleton `dtn:none` endpoint.

        Returns:
            EID: The `dtn:none` EndpointID.
        """
        return cls(cls._DTN_NONE)

    def node(self) -> str | None:
        """
        Get the node part of the EndpointID, or `None` for `dtn:none`.

        Returns:
            str | None: The node part of the EndpointID, or `None`.
        """
        if self == self._DTN_NONE:
            return None
        if self.startswith(self._DTN_PREFIX):
            return self[len(self._DTN_PREFIX) :].split(sep="/", maxsplit=1)[0]
        return self[len(self._IPN_PREFIX) :].split(sep=".", maxsplit=1)[0]

    def service(self) -> str | None:
        """
        Get the service part of the EndpointID, or `None` for `dtn:none`.

        Returns:
            str | None: The service part of the EndpointID, or `None`.
        """
        if self == self._DTN_NONE:
            return None
        if self.startswith(self._DTN_PREFIX):
            return self[len(self._DTN_PREFIX) :].split(sep="/", maxsplit=1)[1]
        return self[len(self._IPN_PREFIX) :].split(sep=".", maxsplit=1)[1]

    @classmethod
    def _normalize(cls, eid: str) -> str:
        if eid == cls._DTN_NONE:
            return eid

        if eid.startswith(cls._DTN_PREFIX):
            return cls._normalize_dtn(eid=eid)

        if eid.startswith(cls._IPN_PREFIX):
            return cls._normalize_ipn(eid=eid)

        raise UnknownEIDSchemeError

    @classmethod
    def _normalize_dtn(cls, eid: str) -> str:
        ssp = eid[len(cls._DTN_PREFIX) :]
        if ssp == "none":
            raise InvalidDtnNoneAliasError
        if not ssp:
            raise MissingDtnNodeError

        # split once after node
        if "/" in ssp:
            node, service = ssp.split("/", 1)
        else:
            node, service = ssp, ""

        if not cls._NODE_RE.match(node):
            raise InvalidDtnNodeError(node)
        if not service:
            return f"{cls._DTN_PREFIX}{node}/"

        if not service.isascii():
            raise InvalidDtnServiceError(service)
        return f"{cls._DTN_PREFIX}{node}/{service}"

    @classmethod
    def _normalize_ipn(cls, eid: str) -> str:
        rest = eid[len(cls._IPN_PREFIX) :]
        if rest.startswith("//"):
            raise InvalidIpnSchemeSeparatorError
        parts = rest.split(".")
        if len(parts) != IPN_PARTS:
            raise InvalidIpnStructureError
        try:
            node_i = int(parts[0], 10)
            svc_i = int(parts[1], 10)
        except ValueError as e:
            raise InvalidIpnNumbersError(parts[0], parts[1]) from e
        if node_i < 1:
            raise InvalidIpnNodeError(node_i)
        if svc_i < 0:
            raise InvalidIpnServiceError(svc_i)
        return f"{cls._IPN_PREFIX}{node_i}.{svc_i}"


BROADCAST_ADDRESS = EID.dtn("rec.all", "~")
BROKER_MULTICAST_ADDRESS = EID.dtn("rec.broker", "~")
DATASTORE_MULTICAST_ADDRESS = EID.dtn("rec.store", "~")
EXECUTOR_MULTICAST_ADDRESS = EID.dtn("rec.executor", "~")
CLIENT_MULTICAST_ADDRESS = EID.dtn("rec.client", "~")


def get_multicast_address(node_type: int) -> EID | None:
    """
    Get the multicast EID for a given node type.

    Args:
        node_type (int): The node type as defined in NodeType enum.

    Returns:
        EID | None: The corresponding multicast EID, or None if not applicable.
    """
    from rec.messages import NodeType

    match node_type:
        case NodeType.BROKER:
            return BROKER_MULTICAST_ADDRESS
        case NodeType.EXECUTOR:
            return EXECUTOR_MULTICAST_ADDRESS
        case NodeType.DATASTORE:
            return DATASTORE_MULTICAST_ADDRESS
        case NodeType.CLIENT:
            return CLIENT_MULTICAST_ADDRESS
        case _:
            return None
