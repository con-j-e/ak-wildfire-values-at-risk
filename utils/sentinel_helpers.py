import aiohttp
import asyncio
from datetime import datetime, timezone, timedelta
import io
import json
import os
from oauthlib.oauth2 import BackendApplicationClient
from pathlib import Path
from requests_oauthlib import OAuth2Session
import tarfile
import time
import traceback
from typing import Iterable

##TEMP
# dev logs
from utils.general import basic_file_logger, format_logged_exception
logger = basic_file_logger('sentinel_helpers.log')

# global constant for accessing evalscripts by file name
EVALSCRIPT = {}
evalscript_directory = Path.cwd() / 'sentinel2_evalscript'
for entry in evalscript_directory.glob('*.js'):
    with open(entry, 'r') as file:
        script = file.read()
    EVALSCRIPT[entry.stem] = script

def checkout_sentinel_token(token_filepath: str | Path, client_id_env_var: str, client_secret_env_var: str, minutes_needed: int) -> str:
    '''
    Checkout an existing token that is still valid or generate a new token for sentinel hub api requests.

    Arguments:
        token_filepath -- .txt file where tokens that get generated and token expiration timestamps are saved.
        client_id_env_var -- Name of environment variable where planet client id is saved.
        client_secret_env_var -- Name of environment variable where planet client secret is saved.
        minutes_needed -- Estimated minutes that a token is going to be needed for.

    Returns:
        * str -- token for sentinel hub api access
    '''    

    token_filepath = Path(token_filepath) if isinstance(token_filepath, str) else token_filepath

    if token_filepath.exists():
        with open(token_filepath, 'r') as file:
            token, expires = file.read().split(',')
        if ((float(expires) - time.time()) / 60) <= minutes_needed:
            token, expires = _generate_sentinel_token(client_id_env_var, client_secret_env_var)
            with open(token_filepath, 'w') as file:
                file.write(f'{token},{expires}')

    else:
        token, expires = _generate_sentinel_token(client_id_env_var, client_secret_env_var)
        with open(token_filepath, 'w') as file:
            file.write(f'{token},{expires}')

    return token

def _generate_sentinel_token(client_id_env_var: str, client_secret_env_var: str) -> tuple[str, int]:
    '''
    Generate a new token for sentinel hub api requests.

    Arguments:
        client_id_env_var -- Name of environment variable where planet client id is saved.
        client_secret_env_var -- Name of environment variable where planet client secret is saved.

    Raises:
        KeyError: Expected environment variables are not set.

    Returns:
        * tuple[str, int] -- formatted ( sentinel hub token , expiration time as an epoch timestamp in seconds )
    '''

    client_id = os.getenv(client_id_env_var)
    client_secret = os.getenv(client_secret_env_var)

    for env_var in (client_id, client_secret):
        if env_var is None:
            raise KeyError(f'Environment variable "{env_var}" is not set.')
        
    client = BackendApplicationClient(client_id=client_id)
    oauth = OAuth2Session(client=client)

    token = oauth.fetch_token(
        token_url=r'https://services.sentinel-hub.com/auth/realms/main/protocol/openid-connect/token',
        client_secret=client_secret,
        include_client_id=True
    )

    epoch_seconds_expiration = time.time() + token['expires_in']

    return (token['access_token'], epoch_seconds_expiration)

def parse_response_list(responses: list) -> list[tuple[list, bytes]]:
    '''
    '''
    responses_with_images = []

    for resp in responses:
        _, content = resp
        tar = tarfile.open(fileobj=io.BytesIO(content))

        userdata = json.load(tar.extractfile(tar.getmember('userdata.json')))
        if not userdata['orbits']:
            continue

        dates = []
        for orbit in userdata['orbits']:
            for tile in orbit['tiles']:
                dates.append(tile['date'])
        tif = tar.extractfile(tar.getmember('default.tif'))
        responses_with_images.append((dates, tif))
        
    return responses_with_images



class AsyncSentinelRequester():
    def __init__(self, timeout: int = 900):
        self.timeout = timeout

    async def __aenter__(self):
        print("Entering async context...")
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout))
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):

        if exc_type or exc_val or exc_tb:

            self.exception = (exc_type, exc_val, exc_tb)

            exc_format = traceback.format_exception(exc_type, exc_val, exc_tb)
            exc_format_str = ''.join(exc_format)

            print(f'{exc_type} caused AsyncSentinelRequester context manager to exit -- access exception attribute of the instance for details.')
            print(exc_format_str)

        else:
            self.exception = None

        if not self.session.closed:
            await self.session.close()

        print("Exiting async context...")
        return True        

    async def sentinel_l2a_orbits(self, bbox: Iterable, bbox_epsg: int, epoch_milliseconds_from: int, evalscript: str,  token: str, aoi_identifier: str | int = None) -> tuple[list, float]:
        '''
        '''
        url = r'https://services.sentinel-hub.com/api/v1/process'

        date_range_from = datetime.fromtimestamp(epoch_milliseconds_from / 1000, tz=timezone.utc)
        date_range_to = datetime.now(tz=timezone.utc)
        
        date_range_chunks = self._dt_hour_chunks(date_range_from, date_range_to, hours_per_chunk=24)

        date_range_chunks = [
            (start.strftime('%Y-%m-%dT%H:%M:%SZ'), end.strftime('%Y-%m-%dT%H:%M:%SZ')) 
            for start, end in date_range_chunks 
        ]

        headers = {
            'Authorization': f'Bearer {token}',
            'Accept': 'application/x-tar'
        }

        all_posts = [
            {
                'input': {
                    'bounds': {
                        'properties': {
                            'crs': fr'http://www.opengis.net/def/crs/EPSG/0/{bbox_epsg}'
                        },
                        'bbox': [coord for coord in bbox]
                    }, 
                    'data': [
                        {
                            'type': 'sentinel-2-l2a',
                            'dataFilter': {
                                'timeRange': {
                                    'from': chunk[0],
                                    'to': chunk[1]
                                }
                            }
                        } 
                    ]
                },
                'output': {
                    'responses': [
                        {'identifier': 'default', 'format': {'type': 'image/tiff'}},
                        {'identifier': 'userdata', 'format': {'type': 'application/json'}}
                    ]
                },
                'evalscript': evalscript
            } for chunk in date_range_chunks
        ]

        logger.info(json.dumps(all_posts, indent=4))

        responses = await asyncio.gather(
            *(self._persistent_post_request(url, headers, post_json) for post_json in all_posts),
            return_exceptions=True
        )

        for resp in responses:
            if isinstance(resp, Exception):
                exc_type, exc_val, exc_tb = type(resp), resp, resp.__traceback__
                logger.error(format_logged_exception(exc_type, exc_val, exc_tb))

        headers = [resp[0] for resp in responses]
        processing_units_spent = sum([float(head['x-processingunits-spent']) for head in headers])

        return (responses, processing_units_spent, aoi_identifier)
    
    async def _post_request(self, url: str, headers: dict | None = None, post_json: dict | None = None) -> list:
        '''
        POST request helper. Raise aiohttp.ClientResponseError if response status is 400 or higher, else return responses headers and content.
        '''
        async with self.session.post(url, headers=headers, json=post_json) as response:
            response.raise_for_status()
            headers = response.headers
            content = await response.read()
            return [headers, content]
        
    async def _persistent_post_request(self, url: str, headers: dict | None = None, post_json: dict | None = None) -> list:
        '''
        POST request helper with persistent retry logic.
        '''
        retry_delay = 1
        attempts, max_attempts = 0, 3
        while attempts < max_attempts:
            try:
                return await self._post_request(url, headers, post_json)
            except aiohttp.ClientError:
                attempts += 1
                if attempts == max_attempts:
                    raise
                await asyncio.sleep(retry_delay)
                retry_delay *= 2

    def _dt_hour_chunks(self, DT_0: datetime, DT_1: datetime, hours_per_chunk: int) -> list:
        '''
        Create a list of datetime tuples representing time ranges of hours_per_chunk in between DT_0 and DT_1.
        '''
        chunks = []
        current_start = DT_0
        while current_start < DT_1:
            current_end = current_start + timedelta(hours=hours_per_chunk)
            if current_end > DT_1:
                current_end = DT_1
            chunks.append((current_start, current_end))
            current_start = current_end
        return chunks