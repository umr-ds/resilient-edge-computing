from typing import override
from uuid import UUID

from readerwriterlock.rwlock import RWLockWrite
from zeroconf import ServiceListener, Zeroconf

from rec.rest.model import Address
from rec.rest.nodes.node import Node
from rec.rest.nodetypes.broker import Broker
from rec.util.log import LOG


class BrokerListener(ServiceListener):
    brokers: dict[UUID, Broker]
    lock: RWLockWrite

    def __init__(self):
        self.brokers = {}
        self.lock = RWLockWrite()

    def remove_broker(self, node_id: UUID):
        with self.lock.gen_wlock():
            node = self.brokers.pop(node_id, None)
        if node:
            LOG.info(f"Lost connection to broker {node_id}")

    @override
    def update_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        pass

    @override
    def remove_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        self.remove_broker(Node.id_from_name(name))

    @override
    def add_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        info = zc.get_service_info(type_, name)
        node_id = Node.id_from_name(name)
        if info:
            for addr in info.parsed_addresses():
                broker = Broker(id=node_id, address=Address(host=addr, port=info.port))
                if broker.ping() == name:
                    with self.lock.gen_wlock():
                        self.brokers[node_id] = broker
                    LOG.info(f"Discovered broker {node_id}")
                    break
