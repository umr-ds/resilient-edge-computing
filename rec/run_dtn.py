#! /usr/bin/env python3

import asyncio
import logging
from argparse import ArgumentParser, Namespace
from pathlib import Path

from rec.dtn.broker import Broker
from rec.dtn.client import main as client_main
from rec.dtn.datastore import Datastore
from rec.dtn.eid import EID
from rec.dtn.executor import Executor
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


def _run_executor(args: Namespace) -> None:
    LOG.info("Running in executor-mode")
    executor = Executor(
        node_id=args.id,
        dtn_agent_socket=args.socket,
        root_dir=args.root_directory,
    )
    asyncio.run(executor.run())


def _run_client(args: Namespace) -> None:
    LOG.info("Running in client-mode")
    client_main(args=args)


def main() -> None:
    parser = ArgumentParser(prog="rec")
    parser.add_argument(
        "-i",
        "--id",
        help="NodeID (must be valid bpv7 node id)",
        required=True,
        type=EID,
    )
    parser.add_argument(
        "-s",
        "--socket",
        default="/tmp/rec_test_1.sock",
        help="Path to the dtn application-agent's socket",
    )
    parser.add_argument("-v", action="store_true", help="verbose logging")

    subparsers = parser.add_subparsers()

    broker_parser = subparsers.add_parser(name="broker")
    broker_parser.set_defaults(run=_run_broker)

    datastore_parser = subparsers.add_parser(name="datastore")
    datastore_parser.set_defaults(run=_run_datastore)
    datastore_parser.add_argument(
        "root_directory", help="Root directory for database & data blobs"
    )

    executor_parser = subparsers.add_parser(name="executor")
    executor_parser.set_defaults(run=_run_executor)
    executor_parser.add_argument(
        "root_directory", help="Root directory for executor storage", type=Path
    )

    client_parser = subparsers.add_parser(name="client")
    client_parser.set_defaults(run=_run_client)
    client_parser.add_argument(
        "-c",
        "--context_file",
        help="File to store context information",
        default="context.toml",
    )

    client_subparsers = client_parser.add_subparsers(dest="command")

    client_job_query = client_subparsers.add_parser(
        name="query", help="Query broker for jobs"
    )
    client_job_query.add_argument("submitter", help="EndpointID of job submitter")

    client_named_data = client_subparsers.add_parser(
        name="data", help="Interact with datastore"
    )
    client_named_data.add_argument(
        "datastore_id", help="EndpointID of datastore", type=EID
    )
    client_named_data.add_argument("data_name", help="Name of data")

    client_named_data_subparsers = client_named_data.add_subparsers(dest="data_command")
    client_named_data_get = client_named_data_subparsers.add_parser(
        "get", help="Retrieve data from datastore"
    )

    client_named_data_put = client_named_data_subparsers.add_parser(
        "put", help="Send data for storage"
    )
    client_named_data_put.add_argument("data_file", help="Path to data file")

    args = parser.parse_args()

    log_level = logging.DEBUG if args.v else logging.INFO
    LOG.setLevel(log_level)

    args.run(args)


if __name__ == "__main__":
    main()
