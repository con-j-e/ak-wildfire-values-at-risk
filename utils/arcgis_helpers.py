import aiohttp
from arcgis.features import FeatureSet
from arcgis.gis import GIS
import asyncio
from copy import deepcopy
import geopandas as gpd
import json
import os
import pandas as pd
from pathlib import Path
import pickle as pkl
import shapely as shp
import subprocess
import time
from typing import Iterable
import traceback

class AsyncArcGISRequester():
    '''
    A class to handle asynchronous requests to ArcGIS services using aiohttp.
    Intended for use with an async context manager that manages the lifecycle of an aiohttp.ClientSession.
    It is the responsibility of the caller to log or otherwise handle exceptions that cause the context manager to exit.
    These exceptions will not be raised by the class but can be accessed in full detail using the exception attribute of a class instance.

    Attributes:
        - timeout (int) -- total timeout for aiohttp.ClientSession() in seconds. Default is 900.
        - exception (tuple | None) -- exception details present when __aexit__ was called.
    
    Methods:
        - arcgis_rest_api_get() -- GET request with persistent retry logic.
        - arcgis_rest_api_post() -- POST request with persistent retry logic.
        - paginate_arcgis_features() -- Query an ArcGIS Online feature layer using pagination. 
        - send_query_bundle() -- Bundles paginated query with a result identifier and a url alias.
        - applyEdits_request() -- applyEdits POST request to an ArcGIS Online feature layer endpoint, uses SQL query to get OIDs for deletions.
        - applyEdits_archiver() -- Loads features to be deleted into a GDF, then deletes features from the online feature layer.

    '''
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

            print(f'{exc_type} caused AsyncArcGISRequester context manager to exit -- access exception attribute of the instance for details.')
            print(exc_format_str)

        else:
            self.exception = None

        if not self.session.closed:
            await self.session.close()

        print("Exiting async context...")
        return True

    async def _get_request(self, url: str, params: dict | None = None, is_raw: bool = False) -> dict:
        '''
        GET request helper. Raise aiohttp.ClientResponseError if response status is 400 or higher, else return JSON response. Set optional argument is_raw=True to return the response as-is.
        '''
        async with self.session.get(url, params=params) as response:
            response.raise_for_status()
            return await response.read() if is_raw else await response.json()
        
    async def _post_request(self, url: str, data: dict | None = None, is_raw: bool = False) -> dict:
        '''
        POST request helper. Raise aiohttp.ClientResponseError if response status is 400 or higher, else return JSON response. Set optional argument is_raw=True to return the response as-is.
        '''
        async with self.session.post(url, data=data) as response:
            response.raise_for_status()
            return await response.read() if is_raw else await response.json()
        
    async def arcgis_rest_api_post(self, base_url: str, data: dict | None = None, operation: str | None = None, is_raw: bool = False) -> dict:
        '''
        POST request with persistent retry logic.

        Args:
            - base_url (str) -- REST API endpoint.
            - data (dict | None, optional) -- Data to send with POST request. Defaults to None.
            - operation (str | None, optional) -- Operation to perform at REST API endpoint (i.e. 'applyEdits'). Defaults to None.
            - is_raw (bool, optional) -- If True, returns the response as-is. Defaults to False and JSON is returned.

        Returns:
            - dict -- JSON formatted response.
        Raises:
            - aiohttp.ClientError -- Base class for all client specific exceptions.
        '''      
        url = f'{base_url}/{operation}' if operation else base_url

        retry_delay = 1
        attempts, max_attempts = 0, 3

        while attempts < max_attempts:
            try:
                return await self._post_request(url, data, is_raw)
            except aiohttp.ClientError:
                attempts += 1
                if attempts == max_attempts:
                    raise
                await asyncio.sleep(retry_delay)
                retry_delay *= 2

    async def arcgis_rest_api_get(self, base_url: str, params: dict | None = None, operation: str | None = None, is_raw: bool = False) -> dict:
        '''
        GET request with persistent retry logic.

        Args:
            - base_url (str) -- REST API endpoint.
            - params (dict | None, optional) -- Parameters to include in GET request. Defaults to None.
            - operation (str | None, optional) -- Operation to perform at REST API endpoint (i.e. 'query?'). Defaults to None.
            - is_raw (bool, optional) -- If True, returns the response as-is. Defaults to False and JSON is returned.

        Returns:
            - dict -- JSON formatted response | raw data.
        Raises:
            - aiohttp.ClientError -- Base class for all client specific exceptions.
        '''  
        url = f'{base_url}/{operation}' if operation else base_url

        retry_delay = 1
        attempts, max_attempts = 0, 3

        while attempts < max_attempts:
            try:
                return await self._get_request(url, params, is_raw)
            except aiohttp.ClientError:
                attempts += 1
                if attempts == max_attempts:
                    raise
                await asyncio.sleep(retry_delay)
                retry_delay *= 2

    async def paginate_arcgis_features(self, url: str, params: dict | None = None) -> dict:
        '''
        Query an ArcGIS Online feature layer using pagination.
        { 'features' : [...] } is the expected structure of returned JSON after successful pagination.
        If at any point the key 'features' is not in a response, the response itself will be returned.

        Args:
            - url (str) -- REST API endpoint for an ArcGIS Online feature layer.
            - params (dict | None, optional) -- Parameters to include in GET request. Defaults to None.
        Returns: 
            - dict -- JSON formatted response.
        '''  
        layer_info_params = {'f': 'json'}
        token = params.get('token', None)
        if token:
            layer_info_params['token'] = token
        props = await self.arcgis_rest_api_get(base_url=url, params=layer_info_params)
        try:
            result_rec_count = props['maxRecordCount']
        except KeyError:
            # a KeyError here likely indicates a bad request or unexpected response
            # pagination attempt below will expose the issue more clearly
            # default to 1000 if maxRecordCount is not available or cannot be retrieved
            result_rec_count = 1000

        # setting pagination parameters
        params['resultOffset'] = 0
        params['resultRecordCount'] = result_rec_count

        # container for all features returned by the query
        features = []

        # begin pagination loop
        paginating = True
        while paginating:
            data = await self.arcgis_rest_api_get(url, params, 'query?')

            try:
                features.extend(data['features'])
            except KeyError:
                return data
            
            try:
                # this will trigger an exception if conditions for pagination don't exist
                data['exceededTransferLimit'] == True 
                params['resultOffset'] += result_rec_count
            except:
                paginating = False

        return {'features': features}
    
    async def send_query_bundle(self, result_identifier: str | int, url_alias: str, url: str, params: dict | None = None) -> tuple:
        '''
        Bundles paginated query with a result identifier and a url alias.
        This is useful for tracking results gathered from many asynchronous requests.
        Exceptions will not propagate.
        It is the responsibility of the caller to log or otherwise handle any exception details that are returned.

        Args:
            - result_identifier (str | int) -- A unique value to use for identifying query results.
            - url_alias (str) -- A human-friendly string describing the URL that was queried.
            - url (str) -- REST API endpoint.
            - params (dict | None, optional) -- Parameters to include in GET request. Defaults to None.

        Returns:
            - tuple -- (result_identifier, url_alias, {response data} | (exc_type, exc_val, exc_tb)).   
        '''       
        try:
            response =  await self.paginate_arcgis_features(url, params)
            return (result_identifier, url_alias, response)
        except Exception as e:
            return (result_identifier, url_alias, e.__reduce__())

        
    async def applyEdits_request(self, url: str, token: str, features_to_add: list, get_oids_to_delete_query: str) -> dict:
        '''
        Specific use case for an applyEdits POST request to an ArcGIS Online feature layer endpoint.
        Specify a list of features to add, and a query "where" clause for retrieving ObjectIDs of features to delete.
        
        Args:
            url (str): Target URL.
            token (str): Required token for editing target URL.
            features_to_add (list): ArcGIS JSON formatted features to be added.
            get_oids_to_delete_query (str): SQL query which will retrieve ObjectIDs of features to delete.

        Raises:
            * KeyError -- Key "objectIds" not found in query response

        Returns:
            dict: JSON formatted response from applyEdits POST request.
        '''        

        get_oids_params = {
            'f': 'json',
            'token': token,
            'returnIdsOnly': 'true',
            'where': get_oids_to_delete_query
        }

        get_oids_response = await self.arcgis_rest_api_get(
            base_url=url,
            params=get_oids_params,
            operation='query?'
            )
        
        try:
            oids = get_oids_response['objectIds']
        except KeyError:
            raise KeyError(f'Key "objectIds" not found in query response: {get_oids_response}')

        apply_edits_data = {
            'adds': json.dumps(features_to_add),
            'deletes': json.dumps(oids),
            'rollbackOnFailure': 'true',
            'f': 'json',
            'token': token
        }

        apply_edits_response = await self.arcgis_rest_api_post(
            base_url=url,
            data=apply_edits_data,
            operation='applyEdits'
        )

        return apply_edits_response
    
    async def applyEdits_archiver(self, url: str, token: str, get_archive_feats_query: str) -> tuple[gpd.GeoDataFrame, dict] | None:
        '''
        Specific use case combining a query? GET request and applyEdits POST request to an ArcGIS Online feature layer endpoint.
        Pass a query "where" clause for retrieving features that will then be loaded into a GeoDataFrame.
        OBJECTIDs of these features will then be used to delete features from the feature layer.

        Arguments:
            * url -- Target URL.
            * token -- Required token for querying and editing target URL.
            * get_archive_feats_query -- SQL query which will retrieve features to archive.

        Raises:
            * KeyError -- Key "features" not found in response from query? GET request.

        Returns:
            * tuple[gpd.GeoDataFrame, dict] | None
                * gpd.GeoDataFrame -- Data returned by query? GET request.
                * dict -- Response from applyEdits POST request.
        '''        
        get_archive_feats_params = {
            'f': 'json',
            'token': token,
            'outfields': '*',
            'where': get_archive_feats_query,
        }

        get_archive_feats_response = await self.arcgis_rest_api_get(
            base_url=url,
            params=get_archive_feats_params,
            operation='query?'
            )
        
        try:
            archive_feats = get_archive_feats_response['features']
        except KeyError:
            raise KeyError(f'Key "features" not found in query response: {get_archive_feats_response}')
        if len(archive_feats) < 1:
            return None

        archive_feats_gdf = arcgis_features_to_gdf({'features': archive_feats})
        oids_to_delete = archive_feats_gdf['OBJECTID'].to_list()

        apply_edits_data = {
            'deletes': json.dumps(oids_to_delete),
            'rollbackOnFailure': 'true',
            'f': 'json',
            'token': token
        }

        apply_edits_response = await self.arcgis_rest_api_post(
            base_url=url,
            data=apply_edits_data,
            operation='applyEdits'
        )

        return (archive_feats_gdf, apply_edits_response)

    # DRAFTING
    # Should gain a better understanding Of what response formats to expect from land fire, and plan how to best handle them within and between functions
    # Using LFPS will require chaining functions together and passing data between them
    # This will have implications for the async arc gis requester class. Already the class has been modified So that there is the option to return a raw response
    # Situationally it might make sense to return a response stream for streaming chunks of data
    # Or to ask for another precanned format Like I do with JSON, if there is one which can clearly be expected and is useful
    async def submit_landfire_job(
        self,
        layers: Iterable | str,
        wgs84_bbox: Iterable,
        output_wkid: int = None,
        resample_resolution: int = None,
        edit_rule: dict = None,
        edit_mask: dict = None
        ):
        '''
        Submit a job to the Landfire Products Service (LFPS).
            * https://lfps.usgs.gov/lfps/helpdocs/LFProductsServiceUserGuide.pdf

        Args:
            - layers
            - wgs84_bbox (Iterable) -- Float values in the order of ( lower left longitude, lower left latitude, upper right longitude, upper right latitude ).
            - output_wkid (int) -- The desired output spatial reference WKID.
            - resample_resolution (int) -- Requested when the desired resolution is coarser than the native 30m of the LF products. The valid values for this box are integers between 31 and 9999. If not used, the default value is 30.
            - edit_rule: dict -- JSON structure describing the edit rule to be applied. See LandFire documentation for more details.
            - edit_mask: str -- Item ID of the edit mask to be applied. Item name must be specified in edit_rule parameters.

        Returns:
            - ?
        
        '''

        base_url: str = r'https://lfps.usgs.gov/arcgis/rest/services/LandfireProductService/GPServer/LandfireProductService/submitJob'

        all_params = {
            'Layer_List': layers if isinstance(layers, str) else ';'.join(layers),
            'Area_Of_Interest': '%'.join((str(coord) for coord in wgs84_bbox)),
            'Output_Projection': output_wkid,
            'Resample_Resolution': resample_resolution,
            'Edit_Rule': json.dumps(edit_rule) if edit_rule is not None else None,
            'Edit_Mask': json.dumps(edit_mask) if edit_mask is not None else None
        }

        user_params = {key: value for key, value in all_params.items() if value is not None}

        response = await self.arcgis_rest_api_get(base_url, params=user_params, is_raw=True)

        pass

def assign_wkid_3338(feature: dict) -> dict:
    '''
    Sometimes assigning an ArcGIS JSON feature or features their own spatialReference property is useful.
    This does NOT actually alter whatever projection the data is in!
    One use case is after converting GeoJSON that is in a projection other than 4326 to ArcGIS JSON,
    during conversions from GeoDataFrame -> GeoJSON -> ArcGIS JSON.

    Args:
        - feature (dict) -- A single ArcGIS JSON feature.

    Returns:
        - dict -- A single ArcGIS JSON feature.
    '''  
    feature["geometry"]["spatialReference"] = {"wkid": 3338}
    return feature

def arcgis_features_to_gdf(features: dict) -> gpd.GeoDataFrame:
    '''
    Quick and easy conversion of ArcGIS JSON features to a GeoPandas GeoDataFrame.
    '''
    fset = FeatureSet.from_dict(features)
    geojson = json.loads(fset.to_geojson)
    gdf = gpd.GeoDataFrame.from_features(geojson['features'], crs='EPSG:3338')
    return gdf

def arcgis_features_to_dataframe(features: dict) -> pd.DataFrame:
    '''
    Load ArcGIS JSON features into a regular Pandas DataFrame (no spatial properties or methods).
    Geometry column is created by default, will remain None if features do not have geometry.
    Assumes all features have the same attribute keys. 
    
    Arguments:
        features -- ArcGIS JSON features

    Returns:
        DataFrame where each row represents one feature
    '''    
    attrs_template = features['features'][0]['attributes']
    df_dict = {attrName: [] for attrName,_ in attrs_template.items()}
    df_dict['geometry'] = []

    for feat in features['features']:
        df_dict['geometry'].append(feat.get('geometry', None))
        for attrName, attrVal in feat['attributes'].items():
            df_dict[attrName].append(attrVal)

    return pd.DataFrame(df_dict)

def arcgis_polygon_features_to_gdf(polygon_features: dict) -> gpd.GeoDataFrame:
    '''
    Converts ArcGIS JSON polygon features to a GeoDataFrame, first applying a cleanup function
    to all geometries. See doc string for _arcgis_polygon_cleanup(), in arcgis_json_feature_helpers.py, for details.
    
    '''
    polys_df = arcgis_features_to_dataframe(polygon_features)

    polys_df['geometry'] = polys_df.apply(_arcgis_polygon_cleanup, axis=1)

    polys_gdf = gpd.GeoDataFrame(polys_df, geometry='geometry', crs='EPSG:3338')

    return polys_gdf

def checkout_token(credentials_env_var: str, token_minutes: int, token_env_var: str, minutes_needed: int):
    '''
    Written for accessing token to use with the ArcGIS REST API. Checks out an existing token saved to
    a system environment variable, or generates a new token if existing token is no longer usable.
    Intention is to eliminate unnecessary repeated token generation. 

    Arguments:
        * credentials_env_var -- OS environment variable. Must hold comma seperated string like '{url},{username},{password}'.
        * token_minutes -- Requested lifespan for the newly generated token if a new token is generated.
        * token_env_var -- OS environment variable. Must hold comma seperated string like '{token},{expiration_utc_time_as_seconds_since_epoch}'
        * minutes_needed -- Amount of time that the caller expects it will need a token for. Caller should over-estimate to be on the side of caution.

    Returns:
        * str -- API access token
    '''    
    token_env_var_value = os.getenv(token_env_var)

    if token_env_var_value is None:
        token, expires = _generate_token(credentials_env_var, token_minutes)
        subprocess.run(f'setx {token_env_var} "{token},{expires}"')
        print(f'Environment variable "{token_env_var}" is now set and will be available in future command windows. Restart may be required.')
        return
        
    token, expires = token_env_var_value.split(',')

    if ((float(expires) - time.time()) / 60) <= minutes_needed:
        token, expires = _generate_token(credentials_env_var, token_minutes)
        subprocess.run(f'setx {token_env_var} "{token},{expires}"')

    return token

def fresh_pickles(jar: str | Path, json_features: list[dict], identifier: str | int, ignore_attributes: Iterable[str] = None) -> list[dict]:
    '''
    Compare JSON features with a matching .pkl file to determine whether relevant changes have occurred.

    Arguments:
        * jar -- Pickle storage (file directory)
        * json_features -- Contains ESRI json features
        * identifier -- Unique attribute used to match .pkl files with a json feature

    Keyword Arguments:
        * ignore_attributes -- Specify attributes that should not be considered when assessing equivalency (default: {None})

    Returns:
        * list[dict] -- Input json features, minus those that were equivalent to json that has already been processed
    '''    

    jar = Path(jar) if isinstance(jar, str) else jar

    del_idx = set()

    for idx, feat in enumerate(json_features):

        file_path = jar / f'{feat['attributes'][identifier]}.pkl'

        if file_path.exists():

            compare_feat = deepcopy(feat)
            with open(file_path, 'rb') as file:
                old_feat = pkl.load(file)

            ignore_attributes = ignore_attributes or tuple()
            for attr in ignore_attributes:
                del compare_feat['attributes'][attr]
                del old_feat['attributes'][attr]

            if compare_feat == old_feat:
                del_idx.add(idx)
                continue
            else:
                with open(file_path, 'wb') as file:
                    pkl.dump(feat, file)
        
        else:
            with open(file_path, 'wb') as file:
                pkl.dump(feat, file)

    return [feat for idx, feat in enumerate(json_features) if idx not in del_idx]


def _arcgis_polygon_cleanup(row: pd.Series) -> shp.Polygon | shp.MultiPolygon:
    """
    - This helper function is written to be used in a pandas.apply() function.
    - Converts ArcGIS polygon geometries to Shapely polygon / multipolygon geometries accordingly.
    - Applies following heuristic *on a feature-by-feature basis* to enforce CW vs CCW ordering of rings.
        - A ring that IS contained by any other ring is an interior ring representing a hole.
        - A ring that IS NOT contained by any other ring is an exterior ring representing a boundary.
    - The primary use-case for this function is to ensure accurate area calculations.

    Args:
        - arcgis_geom_json (dict): JSON representation of ArcGIS geometry object of type polygon.
            - https://developers.arcgis.com/rest/services-reference/enterprise/geometry-objects/#polygon

    Returns:
        - shp.Polygon | shp.MultiPolygon: 
            - https://shapely.readthedocs.io/en/2.0.6/reference/shapely.Polygon.html#shapely-polygon
            - https://shapely.readthedocs.io/en/2.0.6/reference/shapely.MultiPolygon.html#shapely-multipolygon

    """
    arcgis_geom_json = row['geometry']
    rings = arcgis_geom_json['rings']

    ## single part polygon processing
    if len(rings) == 1:
        coords = tuple(tuple(coord) for coord in rings[0])
        output_polygon = shp.Polygon(coords)
        if not output_polygon.exterior.is_ccw:
            output_polygon = shp.reverse(output_polygon)
        # early return, single part polygon processing complete
        return output_polygon
    
    ## multi-part polygon processing
    # intermediary container for all rings
    all_rings = []
    # container for exterior rings
    exterior_rings = []
    # container for interior rings
    interior_rings = []

    for r in rings:
        coords = tuple(tuple(coord) for coord in r)
        all_rings.append(shp.Polygon(coords))

    ## sort interior vs exterior rings based on presence of a contained spatial relationship
    for idx_a, ring_a in enumerate(all_rings):
        a_is_contained = False
        for idx_b, ring_b in enumerate(all_rings):
            if idx_a == idx_b:
                continue
            # bbox intersection gatekeeper for shp.contains function
            if not shp.box(*ring_a.bounds).intersects(shp.box(*ring_b.bounds)):
                continue
            # condition for switching flag and breaking inner loop (presence of ANY contained spatial relationship is all that matters)
            if shp.contains(ring_b, ring_a):
                a_is_contained = True
                break
        if a_is_contained:
            interior_rings.append(ring_a)
        else:
            exterior_rings.append(ring_a)

    ## ensure all individual rings are clockwise (meaning they have a positive area calculation)
    ## rings classified as 'interior' will have their areas removed using shp.difference()
    #? not certain this is truly a necessary step for shp.difference() to work as expected?
    for ring_lst in interior_rings, exterior_rings:
        for idx, ring in enumerate(ring_lst):
            if not ring.exterior.is_ccw:
                ring_lst[idx] = shp.reverse(ring)
            
    # create shapely multi polygons
    exterior_multipoly = shp.MultiPolygon(exterior_rings)
    interior_multipoly = shp.MultiPolygon(interior_rings)

    # attempt to repair any geometry issues
    if not exterior_multipoly.is_valid:
        exterior_multipoly = shp.make_valid(exterior_multipoly)
    if not interior_multipoly.is_valid:
        interior_multipoly = shp.make_valid(interior_multipoly)

    # remove interior areas
    multipoly = shp.difference(exterior_multipoly, interior_multipoly)

    return multipoly

def _generate_token(credentials_env_var: str, token_minutes: int = 60) -> tuple[str, int]:
    '''
    Uses credentials stored as an environment variable to generate a new token using
    the ArcGIS API for Python's GIS module. 

    Arguments:
        credentials_env_var -- OS environment variable. Must hold comma seperated string like '{url},{username},{password}'.

    Keyword Arguments:
        token_minutes -- Amount of time that a newly generated token will be valid for. (default: {60})

    Raises:
        KeyError -- Environment variable expected to hold '{url},{username},{password}' is not set.

    Returns:
        tuple -- ( token , expiration_utc_time_as_seconds_since_epoch )
    '''    
    credentials_env_var_value = os.getenv(credentials_env_var)

    if credentials_env_var_value is None:
        raise KeyError(f'Environment variable "{credentials_env_var}" is not set.')

    url, username, password = credentials_env_var_value.split(',')

    attempts, max_attempts = 0,3
    while attempts < max_attempts:
        try:
            gis = GIS(url, username, password, expiration=token_minutes)
            expires = time.time() + (gis._expiration * 60)
            return (gis._con.token, expires)
        except Exception as e:
            attempts += 1
            print(f'Failed to connect to {url}. Error: {e}. Attempt {attempts} of {max_attempts}.')
            if attempts == max_attempts:
                raise
            time.sleep(5)

