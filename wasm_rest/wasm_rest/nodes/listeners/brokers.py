import threading

from zeroconf import ServiceListener, Zeroconf

from wasm_rest.model import Address
from wasm_rest.nodes.node import Node
from wasm_rest.nodetypes.broker import Broker


class BrokerListener(ServiceListener):
    brokers: dict[str, Broker] = {}
    lock = threading.Lock()

    def update_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        pass

    def remove_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        with self.lock:
            del self.brokers[name]

    def add_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        info = zc.get_service_info(type_, name)
        if info:
            with self.lock:
                self.brokers[name] = Broker(id=Node.id_from_name(name),
                                            address=Address(host=info.parsed_addresses()[0], port=info.port))
