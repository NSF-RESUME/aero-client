"""Osprey Client API."""
import argparse
import json
import os
import pandas as pd
import requests
import sys

from io import StringIO
from pathlib import Path

from globus_sdk import AccessTokenAuthorizer
from globus_sdk import NativeAppAuthClient
from globus_sdk import RefreshTokenAuthorizer
from globus_sdk import TransferClient

from osprey.server.lib.serializer import serialize
from osprey.server.lib.globus_auth import authenticate

CLIENT_UUID = 'c78511ef-8cf7-4802-a7e1-7d56e27b1bf8'
ENDPOINT_ID = '6dec76ea-e7fd-492e-947e-f2a92073a275'
COLLECTION_UUID = '52f7f6bc-444f-439a-ad48-a4569d10c3d1'
HTTPS_SERVER = 'https://g-c952d0.1305de.36fe.data.globus.org' 
DSAAS_DIR = Path(Path.home(), '.local/share/dsaas')
TOKEN_FILE = 'client_tokens.json'

SERVER_ADDRESS = os.getenv('DSAAS_SERVER')

if SERVER_ADDRESS is None:
    SERVER_ADDRESS = '129.114.27.115:5001'

SERVER_URL = f"http://{SERVER_ADDRESS}/osprey/api/v1.0/"

class ClientError(Exception):
    def __init__(self, *args: object, **kwargs) -> None:
        self.code = kwargs.get('code', 500)
        self.message = kwargs.get('message', 'Unexpected error')
        super().__init__(*args)
    
    def __repr__(self) -> str:
        return f"ClientError Code ({self.code}) : {self.message}"

def register_function(func):
    pickled_function = serialize(func)
    res = requests.get(f'{SERVER_URL}/function', params={'function': pickled_function})
    return res.json()

def list_sources() -> list[dict[str, str | int]]:
    """Get the dictionary of all the sources.

    Returns:
        list[dict[str, str | int ]]: a list containing all available sources data
    """
    req = requests.get(f'{SERVER_URL}/source')
    resp = json.loads(req.content)
    return resp

def get_file(source_id: int,
             version: int | None = None,
             output_path: str | None = None
) -> pd.DataFrame:
    """Gets the version for the source.

    Args:
        source_id (int): ID of the source data to fetch
        version (int, optional): Version of the source data to fetch.
            If none provided, fetches latest version. Defaults to None.
        output_path (str, optional): Path to save data to.

    Returns:
        pd.DataFrame: DataFrame representation of the data.

    Raises:
        ClientError: If data was unable to be transferred
    """
    params = {}
    if version is not None:
        params['version'] = version
    req= requests.get(f'{SERVER_URL}/source/{source_id}/file', params=params)

    if req.status_code == 200:
        # initiate Globus transfer
        TRANSFER_ACCESS_TOKEN = _client_auth()
        headers = {'Authorization': f'Bearer {TRANSFER_ACCESS_TOKEN}'}

        url = f'{HTTPS_SERVER}/{"/".join(req.json()["file_name"].split("/")[2:])}'
        df = pd.DataFrame(requests.get(url, headers=headers).json())

        if output_path is not None:
            df.to_json(output_path)
        return df
    else:
        raise ClientError(req.status_code, req.text)

        
def _client_auth() -> str:
    """Authorizes the client for HTTPS transfers to and from the GCS server
    
    Returns:
        str: Access token of the authorizer
    """
    token_path = Path(DSAAS_DIR, TOKEN_FILE)
    client = NativeAppAuthClient(client_id=CLIENT_UUID)
    if token_path.is_file():
        with open(token_path, 'r') as f:
            tokens = json.load(f)
        refresh_token = tokens["refresh_token"]
        transfer_token = tokens["access_token"]
        authorizer = RefreshTokenAuthorizer(refresh_token=refresh_token, auth_client=client)
    else:
        scopes = f'https://auth.globus.org/scopes/{COLLECTION_UUID}/https'
        token_response = authenticate(client=client, scope=scopes)
        globus_transfer_data = token_response.by_resource_server[COLLECTION_UUID]

        DSAAS_DIR.mkdir(exist_ok=True)
        with open(token_path, 'w+') as f:
            json.dump(globus_transfer_data, f)
        
        transfer_token = globus_transfer_data["access_token"]
        authorizer=AccessTokenAuthorizer(access_token=transfer_token)

    transfer_client = TransferClient(authorizer=authorizer)
    return transfer_token

def create_source(
    name: str,
    url: str, 
    email: str,
    timer: int | None = None,
    description: str | None = None,
    verifier: str | None = None,
    modifier: str | None = None
) -> None:
    """Create a source and store it in the database/server.

    Args:
        name (str): Source name
        url (str): URL to fetch the source data from
        email (str): Email to send timer flow updates to.
        timer (int, optional): Update timer frequency in seconds. Defaults to None.
        description (str, optional): Description of the data. Defaults to None.
        verifier (str, optional): Globus Compute function UUID for the verification function. Defaults to None.
        modifier (str, optional): Globus Compute function UUID for the modifier function. Defaults to None.
    """
    data = {'name': name, 'url': url}
    if timer is not None:
        data['timer'] = timer
    if description is not None:
        data['description'] = description
    if verifier is not None:
        data['verifier'] = verifier
    if modifier is not None:
        data['modifier'] = modifier
    if email is not None:
        data['email'] = email
    else:
        data['email'] = '' # todo make email optional

    req = requests.post(f'{SERVER_URL}/source', json=data)
    res = json.loads(req.content)
    return res

def main():
    parser = argparse.ArgumentParser(
        description='DSaaS client for querying stored data',
    )
    subparsers = parser.add_subparsers(dest='command', help='Available actions')
    list_parser = subparsers.add_parser('list', help='List all stored sources')
    create_parser = subparsers.add_parser('create', help='Create a source to store in DSaas')
    get_parser = subparsers.add_parser('get', help='Get source table from server') 

    # create_parser arguments
    create_parser.add_argument(
            '-n',
            '--name',
            type=str,
            required=True,
            help='Name for the source',
    )
    create_parser.add_argument(
        '-u',
        '--url',
        type=str,
        required=True,
        help='URL to retrieve the source from',
    )
    create_parser.add_argument(
        '-t',
        '--timer',
        default=None,
        type=int,
        help='timer (in s) how often to refresh source',
    )
    create_parser.add_argument(
        '-d',
        '--description',
        default=None,
        type=str,
        help='description for the source',
    )
    create_parser.add_argument(
        '-v',
        '--verifier',
        default=None,
        type=str,
        help='globus-compute function uuid for the verifier',
    )
    create_parser.add_argument(
        '-m',
        '--modifier',
        default=None,
        type=str,
        help='globus-compute function uuid for the modifier',
    )
    create_parser.add_argument(
        '-e',
        '--email',
        type=str,
        required=True,
        help='email address to send notifications to in case of failure'
    )

    # Get parser arguments
    get_parser.add_argument(
        '-s_id',
        '--source_id',
        required=True,
        help='source id of the source to fetch',
    )
    get_parser.add_argument(
        '-ver',
        '--version',
        help='version of the source to fetch',
    )
    get_parser.add_argument(
        '-o',
        '--output_path',
        default=None,
        type=str,
        help='output path to save data to.'
    )
    args = parser.parse_args()

    # actions
    if args.command == 'list':
        print(json.dumps(list_sources(), indent=4))
    # elif args.list_proxies:
    #    print(json.dumps(all_proxies(), indent=4))
    elif args.command == 'create':
        create_source(name=args.name,
                    url=args.url,
                    timer=args.timer,
                    description=args.description,
                    verifier=args.verifier,
                    modifier=args.modifier,
                    email=args.email,
                    )
    elif args.command == 'get':
        try:
            file = get_file(
                source_id=args.source_id,
                version=args.version,
                output_path=args.output_path
            )
            print(file.head(5))
        except ClientError as e:
            print(e)
    

if __name__ == '__main__':
    main()