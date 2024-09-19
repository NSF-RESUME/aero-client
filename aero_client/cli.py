"""Osprey Command-line interface."""
import argparse
import json
import logging

from pprint import pprint


from aero_client.api import create_source
from aero_client.api import get_file
from aero_client.api import globus_logout
from aero_client.api import list_data
from aero_client.api import search_sources
from aero_client.api import source_versions

from aero_client.error import ClientError

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="DSaaS client for querying stored data",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available actions")
    list_parser = subparsers.add_parser("list", help="List monitored data")
    create_parser = subparsers.add_parser(
        "create", help="Create a source to store in DSaas"
    )
    get_parser = subparsers.add_parser("get", help="Get source table from server")
    search_parser = subparsers.add_parser("search", help="Search sources")
    register_parser = subparsers.add_parser("register", help="Register analysis flow")
    _ = subparsers.add_parser("logout", help="Log out of Globus auth")

    parser.add_argument("-l", "--log", type=str, default="INFO", help="Set log level")

    list_parser.add_argument(
        "-t",
        "--type",
        choices=["data", "flow", "provenance"],
        default="data",
        help="List metadata related to monitored data, flows or provenance. Defaults to `data`",
    )

    list_parser.add_argument(
        "-s",
        "--data-id",
        type=int,
        required=False,
        help="list all versions associated with provided data id",
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
        "-c",
        "--collection-url",
        type=str,
        required=True,
        help="The GCS Guest Collection domain name",
    )
    create_parser.add_argument(
        "-g",
        "--endpoint-uuid",
        type=str,
        required=True,
        help="The Globus Compute Endpoint UUID to run ingestion flow on",
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
        "-t",
        "--type",
        required=True,
        choices=["source", "output"],
        help="The file type to fetch",
    )
    get_parser.add_argument(
        "-i",
        "--id",
        required=True,
        help="source/output id of the file to fetch",
    )
    get_parser.add_argument(
        "-ver",
        "--version",
        help="version of the source to fetch",
    )
    get_parser.add_argument(
        "-o",
        "--output-path",
        default=None,
        type=str,
        help="output path to save data to.",
    )

    search_parser.add_argument("query", type=str, help="query to pass to search engine")

    # register arguments
    #     endpoint_uuid: str,
    # function_uuid: str,
    # sources: dict[int | int] | list[int] | None = None,
    # kwargs: JSON | None = None,
    # config: str | None = None,
    # description: str | None = None,
    register_parser.add_argument(
        "-e", "--endpoint-uuid", type=str, help="Globus Compute endpoint uuid"
    )
    register_parser.add_argument(
        "-f",
        "--function-uuid",
        type=str,
        help="Globus Compute registered function UUID",
    )
    register_parser.add_argument(
        "-s",
        "--sources",
        metavar="SOURCE_ID=VERSION_ID",
        nargs="+",
        default=None,
        help="Source id and version id of source. Set version id to -1 if latest version is desired.",
    )

    reg_conf_parser = register_parser.add_mutually_exclusive_group(required=False)
    reg_conf_parser.add_argument(
        "-k",
        "--kwargs",
        metavar="KEY=VALUE",
        nargs="+",
        default=None,
        help="Keyword arguments to pass to function",
    )
    reg_conf_parser.add_argument(
        "-c", "--config", type=str, default=None, help="Path to flow configuration file"
    )
    register_parser.add_argument(
        "-d", "--description", type=str, default=None, help="Description of task"
    )

    args = parser.parse_args()

    log_level = getattr(logging, args.log.upper(), None)
    logging.basicConfig(level=log_level, handlers=[logging.StreamHandler()])

    # actions
    if args.command == "list":
        if args.data_id is not None:
            versions = source_versions(args.source_id)
            if len(versions) == 0:
                print("No versions available.")
            else:
                print(json.dumps(versions, indent=4))
        else:
            print(json.dumps(list_data(args.type), indent=4))

    # elif args.list_proxies:
    #    print(json.dumps(all_proxies(), indent=4))
    elif args.command == "create":
        res = create_source(
            name=args.name,
            url=args.url,
            collection_url=args.collection_url,
            endpoint_uuid=args.endpoint_uuid,
            timer=args.timer,
            description=args.description,
            verifier=args.verifier,
            modifier=args.modifier,
            email=args.email,
        )
        pprint(res)
    elif args.command == "get":
        try:
            file = get_file(
                ftype=args.type,
                id=args.id,
                version=args.version,
                output_path=args.output_path,
            )
            if not isinstance(file, bytes):
                print(file.head(5))
        except ClientError as e:
            print(e)

    elif args.command == "search":
        res = search_sources(args.query)

        if len(res) == 0:
            print("Search returned no results")
        else:
            print(json.dumps(res, indent=4))

    elif args.command == "register":
        pass

    elif args.command == "logout":
        # TODO: fix bug where user needs to be authenticated before logging out
        globus_logout()


if __name__ == "__main__":
    main()
