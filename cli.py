"""Osprey Command-line interface."""
import argparse
import json
import logging

from pathlib import Path

from osprey.client import CONF

from osprey.client import create_source
from osprey.client import get_file
from osprey.client import list_sources
from osprey.client import source_versions

from osprey.client.error import ClientError

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="DSaaS client for querying stored data",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available actions")
    list_parser = subparsers.add_parser("list", help="List all stored sources")
    create_parser = subparsers.add_parser(
        "create", help="Create a source to store in DSaas"
    )
    get_parser = subparsers.add_parser("get", help="Get source table from server")
    _ = subparsers.add_parser("logout", help="Log out of Globus auth")

    parser.add_argument("-l", "--log", type=str, default="INFO", help="Set log level")

    list_parser.add_argument(
        "-s",
        "--source_id",
        type=int,
        required=False,
        help="list all versions associated with provided source id",
    )

    # create_parser arguments
    create_parser.add_argument(
        "-n",
        "--name",
        type=str,
        required=True,
        help="Name for the source",
    )
    create_parser.add_argument(
        "-u",
        "--url",
        type=str,
        required=True,
        help="URL to retrieve the source from",
    )
    create_parser.add_argument(
        "-t",
        "--timer",
        default=None,
        type=int,
        help="timer (in s) how often to refresh source",
    )
    create_parser.add_argument(
        "-d",
        "--description",
        default=None,
        type=str,
        help="description for the source",
    )
    create_parser.add_argument(
        "-v",
        "--verifier",
        default=None,
        type=str,
        help="globus-compute function uuid for the verifier",
    )
    create_parser.add_argument(
        "-m",
        "--modifier",
        default=None,
        type=str,
        help="globus-compute function uuid for the modifier",
    )
    create_parser.add_argument(
        "-e",
        "--email",
        type=str,
        required=True,
        help="email address to send notifications to in case of failure",
    )

    # Get parser arguments
    get_parser.add_argument(
        "-s_id",
        "--source_id",
        required=True,
        help="source id of the source to fetch",
    )
    get_parser.add_argument(
        "-ver",
        "--version",
        help="version of the source to fetch",
    )
    get_parser.add_argument(
        "-o",
        "--output_path",
        default=None,
        type=str,
        help="output path to save data to.",
    )
    args = parser.parse_args()

    log_level = getattr(logging, args.log.upper(), None)
    logging.basicConfig(level=log_level, handlers=[logging.StreamHandler()])

    # actions
    if args.command == "list":
        if args.source_id is not None:
            versions = source_versions(args.source_id)
            if len(versions) == 0:
                print("No versions available.")
            else:
                print(json.dumps(versions, indent=4))
        else:
            print(json.dumps(list_sources(), indent=4))

    # elif args.list_proxies:
    #    print(json.dumps(all_proxies(), indent=4))
    elif args.command == "create":
        create_source(
            name=args.name,
            url=args.url,
            timer=args.timer,
            description=args.description,
            verifier=args.verifier,
            modifier=args.modifier,
            email=args.email,
        )
    elif args.command == "get":
        try:
            file = get_file(
                source_id=args.source_id,
                version=args.version,
                output_path=args.output_path,
            )
            print(file.head(5))
        except ClientError as e:
            print(e)
    elif args.command == "logout":
        logger.debug("Removing Globus auth tokens.")
        Path(CONF.dsaas_dir, CONF.token_file).unlink()


if __name__ == "__main__":
    main()
