import pytest
from hypothesis import given

from rec.dtn.eid import EIDError

from .test_helpers import *


class TestEIDBasics:
    def test_eid_is_string_subclass(self):
        eid = EID.dtn("node")
        assert isinstance(eid, str)
        assert isinstance(eid, EID)

    def test_eid_equality(self):
        eid1 = EID.dtn("node1", "service1")
        eid2 = EID.dtn("node1", "service1")
        eid3 = EID.dtn("node2", "service1")

        assert eid1 == eid2
        assert eid1 != eid3
        assert eid1 == "dtn://node1/service1"


class TestDTNEIDs:
    def test_dtn_with_node_only(self):
        eid = EID.dtn("node")
        assert str(eid) == "dtn://node/"
        assert eid.node() == "node"

    def test_dtn_with_node_and_service(self):
        eid = EID.dtn("node", "service")
        assert str(eid) == "dtn://node/service"
        assert eid.node() == "node"

    def test_dtn_with_complex_service_path(self):
        eid = EID.dtn("node", "path/to/service")
        assert str(eid) == "dtn://node/path/to/service"

    def test_dtn_valid_node_names(self):
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

    def test_dtn_valid_service_names(self):
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

    def test_dtn_node_error(self):
        with pytest.raises(EIDError):
            EID.dtn("node with spaces")

    def test_dtn_none_endpoint(self):
        eid = EID.none()
        assert str(eid) == "dtn:none"
        assert eid.node() is None

    @given(eid=dtn_eid())
    def test_dtn_valid_all(self, eid: EID) -> None:
        assert eid


class TestIPNEIDs:
    def test_ipn_basic(self):
        eid = EID.ipn(1, 0)
        assert str(eid) == "ipn:1.0"
        assert eid.node() == "1"

    def test_ipn_minimum_valid_values(self):
        eid = EID.ipn(1, 0)
        assert str(eid) == "ipn:1.0"

    def test_ipn_node_error(self):
        with pytest.raises(EIDError):
            EID.ipn(0, 1)
        with pytest.raises(EIDError):
            EID.ipn(-1, 1)

    def test_ipn_service_error(self):
        with pytest.raises(EIDError):
            EID.ipn(1, -1)


class TestEIDNormalization:
    def test_dtn_node_normalization(self):
        eid = EID("dtn://node")
        assert str(eid) == "dtn://node/"

    def test_dtn_none_normalization(self):
        eid = EID("dtn:none")
        assert str(eid) == "dtn:none"

    def test_invalid_dtn_none_format(self):
        with pytest.raises(EIDError):
            EID("dtn://none")

    def test_invalid_scheme(self):
        with pytest.raises(EIDError):
            EID("http://example.com")
