#! /usr/bin/env python3

import logging
import asyncio

from argparse import ArgumentParser, Namespace

from rec.dtn.broker import Broker
from rec.dtn.datastore import Datastore
from rec.util.log import LOG


def _run_broker(args: Namespace) -> None:
    LOG.info("Running in broker-mode")
    broker = Broker(node_id=args.id, dtn_agent_socket=args.socket)
    asyncio.run(broker.run())


def _run_datastore(args: Namespace) -> None:
    LOG.info("Running in datastore-mode")
    datastore = Datastore(
        node_id=args.id,
        dtn_agent_socket=args.socket,
        root_directory=args.root_directory,
    )
    asyncio.run(datastore.run())


def _run_client(args: Namespace) -> None:
    LOG.info("Running in client-mode")


def main() -> None:
    parser = ArgumentParser(prog="rec")
    parser.add_argument(
        "-i",
        "--id",
        help="NodeID (must be valid bpv7 node id)",
        required=True,
    )
    parser.add_argument(
        "-s",
        "--socket",
        default="/tmp/rec_test_1.sock",
        help="Path to the dtn application-agent's socket",
    )
    parser.add_argument("-v", action="store_true", help="verbose logging")

    subparsers = parser.add_subparsers()

    broker_parser = subparsers.add_parser("broker")
    broker_parser.set_defaults(run=_run_broker)

    datastore_parser = subparsers.add_parser("datastore")
    datastore_parser.set_defaults(run=_run_datastore)
    datastore_parser.add_argument(
        "root_directory", help="Root directory for database & data blobs"
    )

    client_parser = subparsers.add_parser("client")
    client_parser.set_defaults(run=_run_client)

    args = parser.parse_args()

    log_level = logging.DEBUG if args.v else logging.INFO
    LOG.setLevel(log_level)

    args.run(args)


if __name__ == "__main__":
    main()
