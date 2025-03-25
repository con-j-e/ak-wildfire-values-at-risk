import aiohttp
import asyncio
from arcgis.features import FeatureSet
import geopandas as gpd
import json
import pandas as pd
import shapely as shp
from typing import Sequence
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
        - applyEdits_request() -- basic applyEdits POST request to an ArcGIS Online feature layer endpoint.

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
            exc_type, exc_val, exc_tb = type(e), e, e.__traceback__
            return (result_identifier, url_alias, (exc_type, exc_val, exc_tb))
        
    async def applyEdits_request(self, url: str, token: str, features_to_add: list, get_oids_to_delete_query: str) -> dict:
        '''
        Specific use case for an applyEdits POST request to an ArcGIS Online feature layer endpoint.
        Specify a list of features to add, and a query "where" clause for retrieving ObjectIDs of features to delete.
        
        Args:
            url (str): Target URL.
            token (str): Required token for editing target URL.
            features_to_add (list): ArcGIS JSON formatted features to be added.
            get_oids_to_delete_query (str): SQL query which will retrieve ObjectIDs of features to delete. 

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

        apply_edits_data = {
            'adds': json.dumps(features_to_add),
            'deletes': json.dumps(get_oids_response.get('objectIds', [])),
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

    # DRAFTING
    # Should gain a better understanding Of what response formats to expect from land fire, and plan how to best handle them within and between functions
    # Using LFPS will require chaining functions together and passing data between them
    # This will have implications for the async arc gis requester class. Already the class has been modified So that there is the option to return a raw response
    # Situationally it might make sense to return a response stream for streaming chunks of data
    # Or to ask for another precanned format Like I do with JSON, if there is one which can clearly be expected and is useful
    async def submit_landfire_job(
        self,
        layers: Sequence | str,
        wgs84_bbox: Sequence,
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
            - wgs84_bbox (Sequence) -- Float values in the order of ( lower left longitude, lower left latitude, upper right longitude, upper right latitude ).
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