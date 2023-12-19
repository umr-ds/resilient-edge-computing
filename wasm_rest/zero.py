import random
import argparse
import wasm_rest_server
import wasm_rest_root_server
from wasm_rest.model import NodeRole


def wasm_rest_parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cert", help="server cert file")
    parser.add_argument("--key", help="server key file")
    parser.add_argument("--rootdir", default="wasm_rest_server", help="root for the wasm rest file tree")
    # parser.add_argument("--rootaddr", default="localhost:8001", help="address of the root server as host:port")
    parser.add_argument("--addr", default="localhost:8000", help="address of this server as host:port")
    return parser.parse_args()


def main() -> None:
    args = wasm_rest_parse_args()
    function = NodeRole.BROKER if random.random() < .1 else NodeRole.EXECUTOR
    host, port = args.addr.split(":")
    while True:
        if function is NodeRole.BROKER:
            function = wasm_rest_root_server.start(host, int(port))
        elif function is NodeRole.EXECUTOR:
            function = wasm_rest_server.start(host, 8001, args.rootdir)  # TODO change back to 8001
        else:
            break


if __name__ == '__main__':
    main()
