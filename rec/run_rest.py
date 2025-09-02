#! /usr/bin/env python3
import argparse
import logging
import random

from rec.rest.model import NodeRole
from rec.rest.nodes.broker import Broker
from rec.rest.nodes.client import Client
from rec.rest.nodes.datastore import Datastore
from rec.rest.nodes.executor import Executor
from rec.util.log import LOG


def main():
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers()

    log_levels = parser.add_mutually_exclusive_group()
    log_levels.add_argument("-v", action="store_true", help="verbose logging")
    log_levels.add_argument("-i", action="store_true", help="info level logging")
    log_levels.add_argument("-e", action="store_true", help="error logging (default)")

    address_parser = argparse.ArgumentParser(add_help=False)
    addresses_group = address_parser.add_mutually_exclusive_group()
    addresses_group.add_argument(
        "--host", type=str, default="127.0.0.1", help="host ip address"
    )
    addresses_group.add_argument(
        "--hosts",
        type=str,
        help="addresses to send to other nodes, will listen to 0.0.0.0",
    )
    address_parser.add_argument("--port", type=int, default="8000", help="host port")

    broker_parser = subparsers.add_parser("broker", parents=[address_parser])
    broker_parser.set_defaults(prog=NodeRole.BROKER)

    executor_parser = subparsers.add_parser("executor", parents=[address_parser])
    executor_parser.add_argument(
        "--rootdir",
        type=str,
        default="executor.d",
        help="folder to store executor files",
    )
    executor_parser.set_defaults(prog=NodeRole.EXECUTOR)

    datastore_parser = subparsers.add_parser("datastore", parents=[address_parser])
    datastore_parser.add_argument(
        "--rootdir", type=str, default="datastore.d", help="folder to store data"
    )
    datastore_parser.set_defaults(prog=NodeRole.DATASTORE)

    client_parser = subparsers.add_parser("client", parents=[address_parser])
    client_parser.add_argument(
        "json", type=str, help="json file with the execution plan"
    )
    client_parser.add_argument(
        "--resultdir", type=str, default="", help="dir to store results in"
    )
    client_parser.set_defaults(prog=NodeRole.CLIENT)

    combined_parser = subparsers.add_parser(
        "autobe", parents=[executor_parser], add_help=False
    )
    combined_parser.set_defaults(prog=NodeRole.AUTO)

    args = parser.parse_args()

    log_level = logging.DEBUG if args.v else logging.INFO if args.i else logging.ERROR
    uvicorn_args = {"log_level": log_level}
    LOG.setLevel(log_level)

    if hasattr(args, "hosts") and args.hosts is not None:
        host = args.hosts.split(",")
    elif hasattr(args, "host") and args.host is not None:
        host = [args.host]
    else:
        LOG.error("No hosts selected")
        return

    if not hasattr(args, "prog") or args.prog is NodeRole.EXIT:
        parser.print_help()
        return

    role = args.prog
    while role is not NodeRole.EXIT:
        if role == NodeRole.AUTO:
            role = NodeRole.BROKER if random.random() < 0.1 else NodeRole.EXECUTOR

        if role is NodeRole.BROKER:
            role = Broker(host, args.port, uvicorn_args=uvicorn_args).run()
        elif role is NodeRole.EXECUTOR:
            role = Executor(
                host, args.port, args.rootdir, uvicorn_args=uvicorn_args
            ).run()
        elif role is NodeRole.DATASTORE:
            role = Datastore(
                host, args.port, args.rootdir, uvicorn_args=uvicorn_args
            ).run()
        elif role is NodeRole.CLIENT:
            role = Client(args.json, host, args.port, args.resultdir).run()
        else:
            parser.print_help()
            return


if __name__ == "__main__":
    main()
