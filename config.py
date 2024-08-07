import os
import requests
from pathlib import Path

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster

from db import DB


# Load secret keys into env vars
_secrets_dir = Path('secrets')
if not _secrets_dir.is_dir():
    raise(Exception('Secrets directory not found'))
for secret_file in _secrets_dir.iterdir():
    if secret_file.is_file() and not secret_file.name.endswith('.zip'):
        env_var_name = secret_file.name.upper()
        with open(secret_file, 'r') as f:
            secret_value = f.read().strip()
        os.environ[env_var_name] = secret_value


def _get_astra_bundle_url(dbid, token):
    # set up the request
    url = f"https://api.astra.datastax.com/v2/databases/{dbid}/secureBundleURL"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    response = requests.post(url, headers=headers, data="").json()
    # happy path
    if 'downloadURL' in response:
        return response['downloadURL']
    # handle errors
    if 'errors' in response:
        raise Exception(response['errors'][0]['message'])
    raise Exception('Unknown error in ' + response)


# Configure DB for astra or localhost
_astra_token = os.environ.get('ASTRA_TOKEN')
_astra_db_id = os.environ.get('ASTRA_DB_ID')
if _astra_token:
    print('Connecting to Astra')
    bundle_path = os.path.join('secrets', 'secure-connect-%s.zip' % _astra_db_id)
    if not os.path.exists(bundle_path):
        print('Downloading SCB')
        url = _get_astra_bundle_url(_astra_db_id, _astra_token)
        r = requests.get(url)
        with open(bundle_path, 'wb') as f:
            f.write(r.content)
    cloud_config = {
      'secure_connect_bundle': bundle_path
    }
    _auth_provider = PlainTextAuthProvider('token', _astra_token)
    cluster = Cluster(cloud=cloud_config, auth_provider=_auth_provider)
    db = DB(cluster)
    tr_data_dir = '/home/ubuntu/trserver/data'
else:
    print('Connecting to local Cassandra')
    db = DB(Cluster())

# FIXME this literally only works on my machine
if os.path.exists('/home/jonathan'):
    tr_data_dir = '/home/jonathan/Projects/trserver/data'
