import asyncio
from arcgis.geometry import Geometry
from collections import defaultdict
from copy import deepcopy
import geopandas as gpd
import json
from multiprocessing import Pool
import pandas as pd
import pathlib
import sys
from typing import NamedTuple

proj_root = pathlib.Path(__file__).parent.parent
sys.path.append(proj_root)

from utils.arcgis_helpers import AsyncArcGISRequester
from utils.arcgis_helpers import arcgis_features_to_gdf, arcgis_polygon_features_to_gdf
from utils.project import write_analysis_types_dict, batch_write_attr_tups

def gather_query_bundles(analysis_gdf: gpd.GeoDataFrame, query_plan: pd.DataFrame, token_dict: dict[str]) -> tuple[tuple]:
    '''
    Gather all query bundles, which will then be sent asynchronously.

    Args:
        * analysis_gdf (gpd.GeoDataFrame) -- Contains all fire features used in values-at-risk analysis.
        * query_plan (pd.DataFrame) -- Loaded from ..planning\\query_plan.tsv, determines how each query is constructed.
        * token_dict (dict[str]) -- For retrieving tokens to use in queries. Formatted { ago_org : token }.

    Returns:
        * tuple[tuple] -- Tuple containing sub-tuples which are query bundles.
            * Query bundles are formatted: ( IrwinID, URL alias, URL, query parameters ).
    '''
    # use max buffer for spatial query
    max_buf = analysis_gdf['buf_dist'].max()
    analysis_gdf_max_buf = analysis_gdf[analysis_gdf['buf_dist'] == max_buf]

    # writing query bundles
    query_bundles = tuple(
        _write_query_bundle(
            fire_max_buf=fire_max_buf,
            url_alias=alias,
            var_url=url,
            var_params=params,
            ago_org=ago_org,
            token_dict=token_dict
        ) 
        for fire_max_buf in analysis_gdf_max_buf[['IrwinID','geometry']].itertuples(index=False)
        for alias, url, params, ago_org in query_plan[['ALIAS','URL','QUERY_PARAMETERS','AGO_ORGANIZATION']].itertuples(index=False)
    )

    return query_bundles

async def send_all_queries(query_bundles: tuple[tuple]) -> tuple[list, Exception | None]:
    '''
    Asynchronously send all query bundles.

    Args:
        * query_bundles (tuple[tuple]) -- Tuple containing sub-tuples which are query bundles.

    Returns:
        * tuple[list, Exception | None] -- List of query responses, Exception if requester instance exits early with Exception else None.
    '''  
    async with AsyncArcGISRequester() as requester:
        query_responses = await asyncio.gather(
            *(requester.send_query_bundle(*tup) for tup in query_bundles),
            return_exceptions=True
        )

    return (query_responses, requester.exception)

def handle_query_response_pools(query_responses: list[tuple], analysis_plan: pd.DataFrame, batch_size: int) -> list[tuple]:
    '''
    Multiprocessing wrapper for _handle_query_responses().
    Looks for patterns in query responses and handles each accordingly.
    Some responses will not require any analysis, and can generate attribution tuples right away.
    Other responses will be prepared for subsequent analysis.

    Args:
        * query_responses (list[tuple]) -- Returned by send_all_queries().
        * analysis_plan (pd.DataFrame) -- Loaded from ..planning\\analysis_plan.tsv, determines which analyses to run and ultimately which attributes to create.
        * batch_size (int) -- Number of query responses that will be handled by each _handle_query_responses() function call.

    Returns:
        * tuple[dict, list, dict] -- 
            * dict -- to hold { IrwinID : [(var_alias, geodataframe), ... } pairs for analysis.
            * list -- to hold attribution tuples for response types that do not require any analysis.
            * dict -- to hold { log_level : [message_1, message_2, ... ], ... } pairs that will later get logged from main
    '''    
    response_chunks = [tuple(query_responses[i: i + batch_size]) for i in range(0, len(query_responses), batch_size)]

    with Pool() as p:
        results = p.starmap(
            func=_handle_query_responses,
            iterable=[(chunk, deepcopy(analysis_plan)) for chunk in response_chunks],
            chunksize=1
        )

    query_features_dict, results_no_analysis, logger_dict = _merge_pool_results(results)

    return (query_features_dict, results_no_analysis, logger_dict)

def _merge_pool_results(results: list[tuple]) -> tuple:
    '''
    Combines many tuples returned by _handle_query_responses() into a single tuple of identical structure.
    
    Args:
        * results (list[tuple]) -- contains tuples structured ( query_features_dict , results_no_analysis , logger_dict )
            * see _handle_query_responses() for descriptions of these objects

    Returns:
        * tuple[dict, list, dict] -- 
            * dict -- to hold { IrwinID : [(var_alias, geodataframe), ... } pairs for analysis.
            * list -- to hold attribution tuples for response types that do not require any analysis.
            * dict -- to hold { log_level : [message_1, message_2, ... ], ... } pairs that will later get logged from main

    '''
    # create list of all query_features_dict objects 
    chunked_query_features_dicts = [tup[0] for tup in results]

    # define template for the completed query_features_dict object
    query_features_dict = {irwin: [] for obj in chunked_query_features_dicts for irwin,_ in obj.items()}

    # add all (var_alias, geodataframe) pairs to lists associated with respective IrwinIDs
    for obj in chunked_query_features_dicts:
        for irwin, analysis_pairs in obj.items():
            query_features_dict[irwin].extend(analysis_pairs)

    # unpack attribute tuples from all results_no_analysis lists into a single list
    results_no_analysis = [item for tup in results for item in tup[1]]

    # create list of all logger_dict objects 
    chunked_logger_dicts = [tup[2] for tup in results]

    # define template for the completed logger_dict object
    logger_dict = {log_level: [] for obj in chunked_logger_dicts for log_level,_ in obj.items()}

    # add all log messages to lists associated with respective logging level
    for obj in chunked_logger_dicts:
        for log_level, messages in obj.items():
            logger_dict[log_level].extend(messages)

    return (query_features_dict, results_no_analysis, logger_dict)

def _handle_query_responses(query_responses: tuple[tuple], analysis_plan: pd.DataFrame) -> tuple[dict, list]:
    '''
    Looks for patterns in query responses and handles each accordingly.
    Some responses will not require any analysis, and can generate attribution tuples right away.
    Other responses will be prepared for subsequent analysis.

    Args:
        * query_responses (list[tuple]) -- returned by send_all_queries().
        * analysis_plan (pd.DataFrame) -- Loaded from ..planning\\analysis_plan.tsv, determines which analyeses to run and ultimately which attributes to create.

    Returns:
        * tuple[dict, list, dict] -- 
            * dict -- to hold { IrwinID : [(var_alias, geodataframe), ... } pairs for analysis.
            * list -- to hold attribution tuples for response types that do not require any analysis.
            * dict -- to hold { log_level : [message_1, message_2, ... ], ... } pairs that will later get logged from main
    ''' 
    # to hold logging info on atypical or error related query responses
    # returned object
    logger_dict = defaultdict(list)

    # to hold attribution tuples for response types that do not require any analysis
    # returned object
    results_no_analysis = []
    
    # to hold { IrwinID : (var_alias, geodataframe) } pairs
    # will only hold responses where there is at least 1 feature present
    # returned object
    query_features_dict = defaultdict(list)

    for response in query_responses:

        # this should not be possible, exception details should be passed along in the expected tuple structure
        if isinstance(response, tuple) and isinstance(response[0], type):
            if issubclass(response[0], Exception):
                logger_dict['critical'].append(f'Reduced exception returned while gathering asynchronous query results: {response}')
                continue          

        # unpack expected tuple structure
        identifier, var_alias, data = response

        # this determines which attributes get written by batch writes
        analysis_types = write_analysis_types_dict(analysis_plan, var_alias)

        if isinstance(data, tuple) and isinstance(data[0], type):
            if issubclass(data[0], Exception):
                logger_dict['error'].append(json.dumps(
                    {
                        'identifier': identifier,
                        'var_alias': var_alias,
                        'exception_info': str(data)
                    }
                ))
                for buf_dist in (0,1,3,5):
                    attr_tups = batch_write_attr_tups(identifier, buf_dist, var_alias, analysis_types, '!EXCEPTION!')
                    if 'NEAREST_FEATS_FIELDS' in analysis_types and buf_dist == 5:
                        attr_tups.append((identifier, 0, f'{var_alias}_nearest_feats', '!EXCEPTION!'))
                        attr_tups.append((identifier, 0, f'{var_alias}_interior_feats', '!EXCEPTION!'))
                    results_no_analysis.append(attr_tups)

        elif 'error' in data and len(data) == 1:
            logger_dict['error'].append(json.dumps(
                {
                    'identifier': identifier,
                    'var_alias': var_alias,
                    'query_error_info': data
                }
            ))
            for buf_dist in (0,1,3,5):
                attr_tups = batch_write_attr_tups(identifier, buf_dist, var_alias, analysis_types, '!QUERYERROR!')
                if 'NEAREST_FEATS_FIELDS' in analysis_types and buf_dist == 5:
                    attr_tups.append((identifier, 0, f'{var_alias}_nearest_feats', '!QUERYERROR!'))
                    attr_tups.append((identifier, 0, f'{var_alias}_interior_feats', '!QUERYERROR!'))
                results_no_analysis.append(attr_tups)

        elif 'features' in data:
            if len(data['features']) < 1:
                for buf_dist in (0,1,3,5):
                    attr_tups = batch_write_attr_tups(identifier, buf_dist, var_alias, analysis_types, None)
                    if 'NEAREST_FEATS_FIELDS' in analysis_types and buf_dist == 5:
                        attr_tups.append((identifier, 0, f'{var_alias}_nearest_feats', None))
                        attr_tups.append((identifier, 0, f'{var_alias}_interior_feats', None))
                    results_no_analysis.append(attr_tups)

            else:
                sample_feature = data['features'][0]

                if 'geometry' in sample_feature and 'rings' in sample_feature['geometry']:
                    gdf = arcgis_polygon_features_to_gdf(data)
                    query_features_dict[identifier].append((var_alias, gdf))

                elif 'geometry' in sample_feature:
                    gdf = arcgis_features_to_gdf(data)
                    query_features_dict[identifier].append((var_alias, gdf))

                #* this function currently has no use-case for features without geometry
                #* could perform key check for 'attributes' and write to pandas df, if needed at some point
        
        else:
            logger_dict['error'].append(json.dumps(
                {
                    'identifier': identifier,
                    'var_alias': var_alias,
                    'unexpected_query_response': True
                }
            ))
            for buf_dist in (0,1,3,5):
                attr_tups = batch_write_attr_tups(identifier, buf_dist, var_alias, analysis_types, '!UNEXPECTED!')
                if 'NEAREST_FEATS_FIELDS' in analysis_types and buf_dist == 5:
                    attr_tups.append((identifier, 0, f'{var_alias}_nearest_feats', '!UNEXPECTED!'))
                    attr_tups.append((identifier, 0, f'{var_alias}_interior_feats', '!UNEXPECTED!'))
                results_no_analysis.append(attr_tups)

    return (query_features_dict, results_no_analysis, logger_dict)

def _write_query_bundle(
    fire_max_buf: NamedTuple,
    url_alias: str,
    var_url: str,
    var_params: dict,
    ago_org: str,
    token_dict: dict
    ) -> tuple: 
    '''
    Writes a single query bundle that will be gathered and sent asynchronously.

    Args:
        * fire_max_buf (NamedTuple) -- (IrwinID, geometry) for maximum buffer created for a given fire.
        * url_alias (str) -- Short string identifier for URL, used in tracking query responses.
        * var_url (str) --  URL to be queried.
        * var_params (dict) -- Query parameters specific to the URL to be queried.
        * ago_org (str) -- Organization requiring authentication for query.
        * token_dict (dict) -- For retrieving tokens to use in queries. Formatted { ago_org : token }

    Returns:
        * tuple -- formatted ( IrwinID, URL alias, URL, query parameters )
    '''  

    # to be used for tracking responses
    irwin = fire_max_buf.IrwinID
    # to be used for spatial query
    fire_max_buf_geom = fire_max_buf.geometry

    # get envelope for spatial query
    #! there should be a more efficient way to get my bbox geometry from shapely directly
    envelope = Geometry.from_shapely(fire_max_buf_geom, {'wkid': 3338}).envelope

    # query template
    constant_params = {
        'f': 'json',
        'geometry': json.dumps(envelope),
        'geometryType': 'esriGeometryEnvelope',
        'inSR': 3338,
        'outSR': 3338,
        'spatialRel': 'esriSpatialRelIntersects',
        'returnGeometry': 'true',
    }

    # modify query template using url-specific parameters
    var_params = json.loads(var_params)
    var_params.update(constant_params)
    if not pd.isna(ago_org):
        var_params['token'] = token_dict[ago_org]

    return (irwin, url_alias, var_url, var_params)