import pytest
from hypothesis import given

from rec.eid import EID
from rec.errors import (
    InvalidDtnNodeError,
    InvalidDtnNoneAliasError,
    InvalidIpnNodeError,
    InvalidIpnServiceError,
    UnknownEIDSchemeError,
)
from tests.utils.helpers import dtn_eid


class TestEIDBasics:
    def test_eid_is_string_subclass(self) -> None:
        eid = EID.dtn("node")
        assert isinstance(eid, str)
        assert isinstance(eid, EID)

    def test_eid_equality(self) -> None:
        eid1 = EID.dtn("node1", "service1")
        eid2 = EID.dtn("node1", "service1")
        eid3 = EID.dtn("node2", "service1")

        assert eid1 == eid2
        assert eid1 != eid3
        assert eid1 == "dtn://node1/service1"


class TestDTNEIDs:
    def test_dtn_with_node_only(self) -> None:
        eid = EID.dtn("node")
        assert str(eid) == "dtn://node/"
        assert eid.node() == "node"

    def test_dtn_with_node_and_service(self) -> None:
        eid = EID.dtn("node", "service")
        assert str(eid) == "dtn://node/service"
        assert eid.node() == "node"

    def test_dtn_with_complex_service_path(self) -> None:
        eid = EID.dtn("node", "path/to/service")
        assert str(eid) == "dtn://node/path/to/service"

    def test_dtn_valid_node_names(self) -> None:
        valid_nodes = [
            "simple",
            "with-dash",
            "with.dot",
            "with_underscore",
            "with~tilde",
            "with123numbers",
            "Mixed123Case",
        ]

        for node in valid_nodes:
            eid = EID.dtn(node)
            assert eid.node() == node

    def test_dtn_valid_service_names(self) -> None:
        valid_services = [
            "simple",
            "with-dash",
            "with.dot",
            "with_underscore",
            "with~tilde",
            "with123numbers",
            "Mixed123Case",
            "path/to/service",
        ]

        for service in valid_services:
            eid = EID.dtn("node", service)
            assert str(eid) == f"dtn://node/{service}"

    def test_dtn_node_error(self) -> None:
        with pytest.raises(InvalidDtnNodeError):
            EID.dtn("node with spaces")

    def test_dtn_none_endpoint(self) -> None:
        eid = EID.none()
        assert str(eid) == "dtn:none"
        assert eid.node() is None

    @given(eid=dtn_eid())
    def test_dtn_valid_all(self, eid: EID) -> None:
        # everything is happening as part of the generator
        pass


class TestIPNEIDs:
    def test_ipn_basic(self) -> None:
        eid = EID.ipn(1, 0)
        assert str(eid) == "ipn:1.0"
        assert eid.node() == "1"

    def test_ipn_minimum_valid_values(self) -> None:
        eid = EID.ipn(1, 0)
        assert str(eid) == "ipn:1.0"

    def test_ipn_node_error(self) -> None:
        with pytest.raises(InvalidIpnNodeError):
            EID.ipn(0, 1)
        with pytest.raises(InvalidIpnNodeError):
            EID.ipn(-1, 1)

    def test_ipn_service_error(self) -> None:
        with pytest.raises(InvalidIpnServiceError):
            EID.ipn(1, -1)


class TestEIDNormalization:
    def test_dtn_node_normalization(self) -> None:
        eid = EID("dtn://node")
        assert str(eid) == "dtn://node/"

    def test_dtn_none_normalization(self) -> None:
        eid = EID("dtn:none")
        assert str(eid) == "dtn:none"

    def test_invalid_dtn_none_format(self) -> None:
        with pytest.raises(InvalidDtnNoneAliasError):
            EID("dtn://none")

    def test_invalid_scheme(self) -> None:
        with pytest.raises(UnknownEIDSchemeError):
            EID("http://example.com")
