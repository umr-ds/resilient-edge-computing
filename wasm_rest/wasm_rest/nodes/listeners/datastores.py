import threading
from uuid import UUID

import readerwriterlock
from zeroconf import ServiceListener, Zeroconf

from wasm_rest.model import Address
from wasm_rest.nodes.node import Node
from wasm_rest.nodetypes.datastore import Datastore
from wasm_rest.util.log import LOG


class DatastoreListener(ServiceListener):
    datastores: dict[UUID, Datastore] = {}
    lock = readerwriterlock.rwlock.RWLockWrite()

    def update_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        pass

    def remove_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        node_id = Node.id_from_name(name)
        with self.lock.gen_wlock():
            node = self.datastores.pop(node_id, None)
        if node:
            LOG.info(f"Lost connection to datastore {node_id}")

    def add_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        info = zc.get_service_info(type_, name)
        node_id = Node.id_from_name(name)
        if info:
            for addr in info.parsed_addresses():
                datastore = Datastore(id=node_id, address=Address(host=addr, port=info.port))
                if datastore.ping() == name:
                    with self.lock.gen_wlock():
                        self.datastores[node_id] = datastore
                    LOG.info(f"Discovered datastore {node_id}")
                    break
