"""Osprey Command-line interface."""

import argparse
import dataclasses
import json
import logging
import os

from pprint import pprint

from aero_client.config import load_conf


logger = logging.getLogger(__name__)


def main():
    """
    TODO
    """
    parser = argparse.ArgumentParser(
        description="AERO client for querying stored data",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available actions")
    list_parser = subparsers.add_parser("list", help="List aero metadata")
    create_parser = subparsers.add_parser(
        "create", help="Create a source to store in AERO"
    )
    # get_parser = subparsers.add_parser("get", help="Get source table from server")
    search_parser = subparsers.add_parser("search", help="Search sources")
    register_parser = subparsers.add_parser("register", help="Register analysis flow")
    config_parser = subparsers.add_parser("configure", help="Configure the client")
    _ = subparsers.add_parser("logout", help="Log out of Globus auth")

    parser.add_argument("-l", "--log", type=str, default="INFO", help="Set log level")

    lp_g = list_parser.add_mutually_exclusive_group()
    lp_g.add_argument(
        "-t",
        "--type",
        choices=["data", "flow", "prov"],
        default="data",
        help="List metadata related to monitored data, flows or provenance. Defaults to `data`",
    )

    lp_g.add_argument(
        "-i",
        "--id",
        type=str,
        required=False,
        help="list all versions associated with provided data id",
    )

    # create_parser arguments
    create_parser.add_argument(
        "-f",
        "--file",
        type=str,
        default=None,
        help="YAML file supplying any of the create arguments (name, url, "
        "collection_url, collection_uuid, endpoint_uuid, verifier, description). "
        "Explicit CLI flags override values from the file.",
    )
    create_parser.add_argument(
        "-n",
        "--name",
        type=str,
        required=False,
        help="Name for the source",
    )
    create_parser.add_argument(
        "-u",
        "--url",
        type=str,
        required=False,
        help="URL to retrieve the source from",
    )
    create_parser.add_argument(
        "-c",
        "--collection-url",
        type=str,
        required=False,
        help="The GCS Guest Collection domain name",
    )
    create_parser.add_argument(
        "-C",
        "--collection-uuid",
        type=str,
        required=False,
        help="The GCS Guest Collection UUID to store the pulled file in",
    )
    create_parser.add_argument(
        "-g",
        "--endpoint-uuid",
        type=str,
        required=False,
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
        default=None,
        help="email address to send notifications to in case of failure",
    )

    search_parser.add_argument("query", type=str, help="query to pass to search engine")

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

    config_parser.add_argument(
        "-f", "--file", type=str, default=None, help="Configuration file"
    )
    config_parser.add_argument(
        "-p",
        "--profile",
        type=str,
        default=None,
        help="Profile to load from the config file (defaults to AERO_PROFILE or 'default')",
    )

    args = parser.parse_args()

    log_level = getattr(logging, args.log.upper(), None)
    logging.basicConfig(level=log_level, handlers=[logging.StreamHandler()])

    # actions
    if args.command == "list":
        if args.id is not None:
            from aero_client.api import list_versions

            versions = list(list_versions(args.id))
            if len(versions) == 0:
                print("No versions available.")
            else:
                print(json.dumps(versions, indent=4))
        else:
            from aero_client.api import list_metadata

            for page in list_metadata(args.type):
                print(json.dumps(page, indent=4))
                try:
                    _ = input("Press enter to continue or CTRL-D to quit")
                except EOFError:
                    break

    # elif args.command == "get":
    #     try:
    #         file = get_file(
    #             ftype=args.type,
    #             id=args.id,
    #             version=args.version,
    #             output_path=args.output_path,
    #         )
    #         if not isinstance(file, bytes):
    #             print(file.head(5))
    #     except ClientError as e:
    #         print(e)

    elif args.command == "search":
        from aero_client.api import search_sources

        res = search_sources(args.query)

        if len(res) == 0:
            print("Search returned no results")
        else:
            print(json.dumps(res, indent=4))

    elif args.command == "create":
        # Optionally load defaults from a YAML file; explicit CLI flags win.
        cfg = {}
        if args.file is not None:
            import yaml

            with open(args.file) as fh:
                cfg = yaml.safe_load(fh) or {}

        def _pick(key, cli_val):
            return cli_val if cli_val is not None else cfg.get(key)

        name = _pick("name", args.name)
        url = _pick("url", args.url)
        collection_uuid = _pick("collection_uuid", args.collection_uuid)
        collection_url = _pick("collection_url", args.collection_url)
        endpoint_uuid = _pick("endpoint_uuid", args.endpoint_uuid)
        description = _pick("description", args.description)
        # --verifier optional: when omitted (CLI and file), create_source
        # registers a raw-passthrough `stage` function (stores the file as-is).
        function_uuid = _pick("verifier", args.verifier) or cfg.get("function_uuid")

        required = {
            "name": name,
            "url": url,
            "collection_url": collection_url,
            "collection_uuid": collection_uuid,
            "endpoint_uuid": endpoint_uuid,
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            parser.error(
                "create is missing required values "
                f"{missing}; provide them via CLI flags or --file"
            )

        from aero_client.api import create_source

        result = create_source(
            name=name,
            url=url,
            collection_uuid=collection_uuid,
            collection_url=collection_url,
            endpoint_uuid=endpoint_uuid,
            function_uuid=function_uuid,
            description=description,
        )
        print(json.dumps(result, indent=4))

        try:
            source_id = result["contributed_to"][0]["id"]
        except (KeyError, IndexError, TypeError):
            source_id = None
        if source_id is not None:
            print(f"\nSource data id: {source_id}")
            print("  - trigger an ingestion (notify webhook):")
            print(f"      POST <server>/data/{source_id}/notify")
            print("  - reference it as an analysis flow input_data entry:")
            print(f'      {{"<name>": {{"id": "{source_id}", "version": null}}}}')

    elif args.command == "register":
        pass

    elif args.command == "configure":
        profile = args.profile or os.environ.get("AERO_PROFILE", "default")
        pprint(
            dataclasses.asdict(load_conf(args.file, update=True, profile=profile))
        )

    elif args.command == "logout":
        from aero_client.api import globus_logout

        # TODO: fix bug where user needs to be authenticated before logging out
        globus_logout()


if __name__ == "__main__":
    main()
