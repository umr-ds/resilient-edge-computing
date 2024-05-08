#! /usr/bin/env python3

import argparse
import logging

import wasm_rest.util.log
from wasm_rest.model import NodeRole
from wasm_rest.nodes import broker, executor, datastore, client


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    address_parser = argparse.ArgumentParser(add_help=False)
    address_parser.add_argument("--host", type=str, default="127.0.0.1", help="host ip address")
    address_parser.add_argument("--port", type=int, default="8000", help="host port")

    broker_parser = subparsers.add_parser("broker", parents=[address_parser])
    broker_parser.set_defaults(prog=NodeRole.BROKER)

    executor_parser = subparsers.add_parser("executor", parents=[address_parser])
    executor_parser.add_argument("--rootdir", type=str, default="executor.d", help="folder to store executor files")
    executor_parser.set_defaults(prog=NodeRole.EXECUTOR)

    datastore_parser = subparsers.add_parser("datastore", parents=[address_parser])
    datastore_parser.add_argument("--rootdir", type=str, default="datastore.d", help="folder to store data")
    datastore_parser.set_defaults(prog=NodeRole.DATASTORE)

    client_parser = subparsers.add_parser("client", parents=[address_parser])
    client_parser.add_argument("json", type=str, help="json file with the execution plan")
    client_parser.add_argument("--resultdir", type=str, default='', help="dir to store results in")
    client_parser.set_defaults(prog=NodeRole.CLIENT)

    args = parser.parse_args()

    uvicorn_args = {"log_level": logging.INFO}
    wasm_rest.util.log.LOG.setLevel(logging.INFO)

    if not hasattr(args, "prog") or args.prog is NodeRole.EXIT:
        parser.print_help()
    elif args.prog is NodeRole.BROKER:
        broker.run(args.host, args.port, uvicorn_args=uvicorn_args)
    elif args.prog is NodeRole.EXECUTOR:
        executor.run(args.host, args.port, args.rootdir, uvicorn_args=uvicorn_args)
    elif args.prog is NodeRole.DATASTORE:
        datastore.run(args.host, args.port, args.rootdir, uvicorn_args=uvicorn_args)
    elif args.prog is NodeRole.CLIENT:
        client.run(args.json, args.host, args.port, args.resultdir)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
