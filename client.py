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

def all_sources() -> list[dict[str, str | int]]:
    """Get the dictionary of all the sources.

    Returns:
        a list containing all available sources data
    """
    req = requests.get(f'{SERVER_URL}/source')
    resp = json.loads(req.content)
    return resp

def all_proxies() -> None:
    """Get the dictionary of all the sources.

    Returns:
        a list containing all available sources data
    """
    req = requests.get(f'{SERVER_URL}/proxies')
    resp = json.loads(req.content)
    print(json.dumps(resp, indent=4))

def get_file(source_id: str, version: str) -> None:
    """Gets the version for the source.

    Returns:
        prints the file
    """
    params = {}
    if version is not None:
        params['version'] = version
    req= requests.get(f'{SERVER_URL}/source/{source_id}/file', params=params)

    if req.status_code == 200:
        # initiate Globus transfer
        TRANSFER_ACCESS_TOKEN = client_auth()
        headers = {'Authorization': f'Bearer {TRANSFER_ACCESS_TOKEN}'}

        url = f'{HTTPS_SERVER}/{"/".join(req.json()["file_name"].split("/")[2:])}'
        return pd.DataFrame(requests.get(url, headers=headers).json())
    else:
        raise ClientError(req.status_code, req.text)

        
def client_auth() -> str:
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

def source_file(source_id, version = None):
    response = get_file(source_id, version)
    if(response['file_type'] == 'json'):
        return json.loads(response['file'])
    if(response['file_type'] == 'csv' or response['file_type'] is None):
        return pd.read_csv(StringIO(response['file']))

def create_source(name: str, url: str, timer: int = None, description: str = None, verifier: str = None, email: str = None, modifier: str = None) -> None:
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
    print(res)
    return res

def main():
    parser = argparse.ArgumentParser(
        description='Osprey client to create sources',
    )

    command_group = parser.add_mutually_exclusive_group(required=True)
    command_group.add_argument('-list_sources', action='store_true')
    command_group.add_argument('-list_proxies',  action='store_true')
    command_group.add_argument('-create_source',  action='store_true')
    command_group.add_argument('-get_file',  action='store_true')
    parser.add_argument(
            '-n',
            '--name',
            required='-create_source' in sys.argv,
            help='Name for the source',
        )
    parser.add_argument(
        '-u',
        '--url',
        required='-create_source' in sys.argv,
        help='url for the source',
    )
    parser.add_argument(
        '-s_id',
        '--source_id',
        required='-get_file' in sys.argv,
        help='source id for the source',
    )
    parser.add_argument(
        '-ver',
        '--version',
        help='version for the source',
    )
    parser.add_argument(
        '-t',
        '--timer',
        help='timer (in s) how often to refresh source',
    )
    parser.add_argument(
        '-d',
        '--description',
        help='description for the source',
    )
    parser.add_argument(
        '-v',
        '--verifier',
        help='globus-compute function uuid for the verifier',
    )
    parser.add_argument(
        '-m',
        '--modifier',
        help='globus-compute function uuid for the modifier',
    )
    parser.add_argument(
        '-e',
        '--email',
        help='email address to send notifications to in case of failure'
    )
    args = parser.parse_args()
    if args.list_sources:
        print(json.dumps(all_sources(), indent=4))
    elif args.list_proxies:
        print(json.dumps(all_proxies(), indent=4))
    elif args.create_source:
        create_source(name=args.name,
                    url=args.url,
                    timer=args.timer,
                    description=args.description,
                    verifier=args.verifier,
                    modifier=args.modifier,
                    email=args.email
                    )
    elif args.get_file:
        try:
            file = source_file(source_id=args.source_id, version=args.version)
            print(file.head(5))
        except ClientError as e:
            print(e)
    

if __name__ == '__main__':
    main()