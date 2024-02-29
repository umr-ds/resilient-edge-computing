import random
import argparse
import wasm_rest_executor
import wasm_rest_broker
from wasm_rest.model import NodeRole


def wasm_rest_parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cert", help="server cert file")
    parser.add_argument("--key", help="server key file")
    parser.add_argument("--rootdir", default="wasm_rest_server", help="root for the wasm rest file tree")
    parser.add_argument("-b", action="store_true", help="force broker")  # TODO make mutually exclusive
    parser.add_argument("-e", action="store_true", help="force executor")
    # parser.add_argument("--rootaddr", default="localhost:8001", help="address of the root server as host:port")
    parser.add_argument("--addr", default="localhost:8000", help="address of this server as host:port")
    return parser.parse_args()


def main() -> None:
    args = wasm_rest_parse_args()
    function = NodeRole.BROKER if random.random() < .1 else NodeRole.EXECUTOR
    
    host, port = args.addr.split(":")
    while True:
        if function is NodeRole.EXIT:
            break
        if args.b:
            function = NodeRole.BROKER
        elif args.e:
            function = NodeRole.EXECUTOR

        if function is NodeRole.BROKER:
            function = wasm_rest_broker.start(host, int(port))
        elif function is NodeRole.EXECUTOR:
            function = wasm_rest_executor.start(host, int(port), args.rootdir)  # TODO change back to 8001


if __name__ == '__main__':
    main()
