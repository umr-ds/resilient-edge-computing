from uuid import UUID

import readerwriterlock.rwlock
from zeroconf import ServiceListener, Zeroconf

from urban_compute_platform.model import Address
from urban_compute_platform.nodes.node import Node
from urban_compute_platform.nodetypes.broker import Broker
from urban_compute_platform.util.log import LOG


class BrokerListener(ServiceListener):
    brokers: dict[UUID, Broker]
    lock: readerwriterlock.rwlock.RWLockWrite

    def __init__(self):
        self.brokers = {}
        self.lock = readerwriterlock.rwlock.RWLockWrite()

    def remove_broker(self, node_id: UUID):
        with self.lock.gen_wlock():
            node = self.brokers.pop(node_id, None)
        if node:
            LOG.info(f"Lost connection to broker {node_id}")

    def update_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        pass

    def remove_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        self.remove_broker(Node.id_from_name(name))

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
