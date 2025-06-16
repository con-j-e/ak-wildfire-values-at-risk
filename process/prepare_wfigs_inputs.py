import asyncio
from datetime import datetime, timezone
import geopandas as gpd
import numpy as np
import pandas as pd
import pathlib
import pickle as pkl
import sys

proj_root = pathlib.Path(__file__).parent.parent
sys.path.append(proj_root)

from utils.arcgis_helpers import AsyncArcGISRequester
from utils.arcgis_helpers import arcgis_features_to_gdf, arcgis_polygon_features_to_gdf

async def get_wfigs_updates(akdof_var_service_url: str, wfigs_locations_url: str, wfigs_perimeters_url: str, token: str, testing: bool = False) -> tuple[dict]:
    '''
    This function sacrifices modularity for the convenience of using a single AsyncArcGISRequester() instance.

    * Determines the maximum ModifiedOnDateTime value currently in the AK values-at-risk service.
    * Determines IrwinIDs of any features in the AK values-at-risk service with incomplete attribution due to a previous error of any kind.
    * Fetches WFIGS incident perimeters and locations that are newer than the newest feature in the AK values-at-risk service.
    * Fetches WFIGS incident perimeters and locations that share an IrwinID with a feature in the AK values-at-risk service with incomplete attribution.
    * Filters out returned incident location features that share an IrwinID with a returned perimeter feature.

    Args:
        - akdof_var_service_url (str)
        - wfigs_locations_url (str)
        - wfigs_perimeters_url (str)
        - token (str) -- ArcGIS REST API token with NIFC portal access. 
        - testing (bool) -- Indicates whether this is a testing run or a production run. Ensures token is passed when querying a private WFIGS proxy. Defaults to False.

    Returns:
        - tuple[dict, dict, set, bool, tuple] -- 
            - wfigs points json | None,
            - wfigs polygons json | None,
            - irwins with errors | None,
            - boolean flag determining whether to compare returned json with saved pickle files,
            - (exc_type, exc_val, exc_tb) | None)
    '''
    # None placeholders for return objects, in case the requester instance exits early with an exception
    wfigs_points, wfigs_polys, irwins_with_errors = None, None, None

    # Returned boolean flag defaults to True.
    # A discrepency between maximum 'wfigs_ModifiedOnDateTime_dt' value or 'has_error' values for different layers in the AK WF VAR service sets flag to False,
    # meaning that no retrieved WFIGS features should be filtered out, regardless of whether or not they have been seen previously.
    # This controls for an edge case where some layers in the AK WF VAR service update succesfully while others do not, and ensures new updates are applied uniformly.  
    check_json_pickles = True

    async with AsyncArcGISRequester() as requester:

        max_timestamp_params = {
            'f': 'json',
            'where': '1=1',
            'token': token,
            'outStatistics': '''[
                {
                    'statisticType': 'max',
                    'onStatisticField': 'wfigs_ModifiedOnDateTime_dt',
                }
            ]'''
        }        
        tmax_responses = await asyncio.gather(
            *(requester.arcgis_rest_api_get(
                base_url=f'{akdof_var_service_url}/{lyr_idx}',
                params=max_timestamp_params,
                operation='query?'
            ) for lyr_idx in (0,1,2,3))
        )
        seconds = set()
        for resp in tmax_responses:
            sec = resp['features'][0]['attributes']['MAX_wfigs_ModifiedOnDateTime_dt'] / 1000
            seconds.add(sec)
        if len(seconds) > 1:
            check_json_pickles = False
        max_timestamp = datetime.fromtimestamp(min(seconds), timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')
        
        reprocess_errors_params = {
            'f': 'json',
            'where': "HasError = 1",
            'outfields': 'wfigs_IrwinID',
            'returnGeometry': 'false',
            'token': token,
        }
        err_responses = await asyncio.gather(
            *(requester.arcgis_rest_api_get(
                base_url=f'{akdof_var_service_url}/{lyr_idx}',
                params=reprocess_errors_params,
                operation='query?'
            ) for lyr_idx in (0,1,2,3))  
        )
        irwins_with_errors = set()
        for resp in err_responses:
            irwins = {feat['attributes']['wfigs_IrwinID'] for feat in resp['features']}
            if irwins_with_errors and irwins != irwins_with_errors:
                check_json_pickles = False
            irwins_with_errors.update(irwins)

        # 20250602.
        # Possible edge case requiring >= operator for timestamp query instead of >.
        # If location data is retrieved for a fire prior to when the information update flows to the perimeter, 
        # subsequent queries will not retrieve the perimeter data, as modified datetimes will be equivalent.
        # Using the >= operator should control for this.
        # The inefficiency of always requesting the latest updated fire from wfigs may be acceptable,
        # especially given the caching of json pickles that prevents the full process from executing with inputs that have already been seen.
        if irwins_with_errors:

            wfigs_points_where = f"""DispatchCenterID IN ('AKACDC', 'AKCGFC', 'AKNFDC', 'AKTNFC', 'AKYFDC') AND
                                    IncidentTypeCategory = 'WF' AND
                                    (
                                        ModifiedOnDateTime_dt >= timestamp '{max_timestamp}' OR
                                        IrwinID IN ({','.join(f"'{irwin}'" for irwin in irwins_with_errors)})
                                    )"""
            
            wfigs_polys_where = f"""attr_DispatchCenterID IN ('AKACDC', 'AKCGFC', 'AKNFDC', 'AKTNFC', 'AKYFDC') AND
                                    attr_IncidentTypeCategory = 'WF' AND
                                    (
                                        attr_ModifiedOnDateTime_dt >= timestamp '{max_timestamp}' OR
                                        poly_DateCurrent >= timestamp '{max_timestamp}' OR
                                        attr_IrwinID IN ({','.join(f"'{irwin}'" for irwin in irwins_with_errors)})
                                    )"""
        else:
            wfigs_points_where = f"""DispatchCenterID IN ('AKACDC', 'AKCGFC', 'AKNFDC', 'AKTNFC', 'AKYFDC') AND
                                    IncidentTypeCategory = 'WF' AND
                                    ModifiedOnDateTime_dt >= timestamp '{max_timestamp}'"""
            
            wfigs_polys_where = f"""attr_DispatchCenterID IN ('AKACDC', 'AKCGFC', 'AKNFDC', 'AKTNFC', 'AKYFDC') AND
                                    attr_IncidentTypeCategory = 'WF' AND
                                    (
                                        attr_ModifiedOnDateTime_dt >= timestamp '{max_timestamp}' OR
                                        poly_DateCurrent >= timestamp '{max_timestamp}'
                                    )"""

        wfigs_points_outfields = (
            'IncidentName',
            'UniqueFireIdentifier',
            'IncidentSize',
            'InitialResponseAcres',
            'DiscoveryAcres',
            'FinalAcres',
            'IrwinID',
            'ModifiedOnDateTime_dt',
            'ContainmentDateTime',
            'ControlDateTime',
            'FireOutDateTime'
        )

        wfigs_polys_outfields = (
            'attr_IncidentName',
            'attr_UniqueFireIdentifier', 
            'attr_IncidentSize',
            'attr_InitialResponseAcres',
            'attr_DiscoveryAcres',
            'attr_FinalAcres',
            'attr_IrwinID',
            'attr_ModifiedOnDateTime_dt',
            'attr_ContainmentDateTime',
            'attr_ControlDateTime',
            'attr_FireOutDateTime',
            'poly_GISAcres',
            'poly_MapMethod',
            'poly_PolygonDateTime',
            'poly_DateCurrent'
        )

        wfigs_points_params = {
                'f': 'json',
                'where': wfigs_points_where,
                'outfields': ",".join(wfigs_points_outfields),
                'outSR': 3338,
            }
        wfigs_polys_params = {
                'f': 'json',
                'where': wfigs_polys_where,
                'outfields': ",".join(wfigs_polys_outfields),
                'outSR': 3338,
            }
        
        if testing:
            wfigs_points_params['token'] = token
            wfigs_polys_params['token'] = token

        wfigs_points, wfigs_polys = await asyncio.gather(
            requester.arcgis_rest_api_get(
                base_url=wfigs_locations_url,
                params=wfigs_points_params,
                operation='query?'
            ),
            requester.arcgis_rest_api_get(
                base_url=wfigs_perimeters_url,          
                params=wfigs_polys_params,
                operation='query?'
            )
        )

        poly_irwins = {feat['attributes']['attr_IrwinID'] for feat in wfigs_polys['features']}
        wfigs_points = {
            'features': [feat for feat in wfigs_points['features'] if feat['attributes']['IrwinID'] not in poly_irwins]
        }

    return (wfigs_points, wfigs_polys, irwins_with_errors, check_json_pickles, requester.exception)

def prevent_perimeter_overwrite_by_point(wfigs_cache: str | pathlib.Path, wfigs_point_features: list[dict]) -> list[dict]:
    '''
    Addresses edge case where timestamp queries retrieve updated information from the WFIGS fire locations service
    before this information flows to the WFIGS perimeters service,
    causing an existing perimeter in the target service to be temporarily replaced by a reported location geometry.

    Parameters
    ----------
    wfigs_cache : str | pathlib.Path
        Location of cached .pkl files containing the last seen json feature for a fire.
    wfigs_point_features : list[dict]
        Current batch of WFIGS fire locations that have timestamps greater than the max timestamp in the target service.

    Returns
    -------
    list[dict]
        WFIGS fire locations, minus any features that have previously had a perimeter update.
    '''
    del_idx = set()
    wfigs_cache = pathlib.Path(wfigs_cache)
    for idx, feat in enumerate(wfigs_point_features):
        file_path = wfigs_cache / f'{feat['attributes']['IrwinID']}.pkl'
        if file_path.exists():
            with open(file_path, 'rb') as file:
                old_feat = pkl.load(file)
            if 'poly_PolygonDateTime' in old_feat['attributes']:
                del_idx.add(idx)
    return [feat for idx, feat in enumerate(wfigs_point_features) if idx not in del_idx]
            
def create_wfigs_fire_points_gdf(wfigs_points: dict) -> gpd.GeoDataFrame:
    '''
    Converts ArcGIS JSON features for WFIGS fire locations to a GeoDataFrame that is processed and formatted for analysis
    alongside WFIGS fire perimeters.
    
    Args:
        - wfigs_points (dict) -- ArcGIS features JSON retrieved from WFIGS fire locations REST endpoint.

    Returns:
        - gpd.GeoDataFrame
    '''
    wfigs_points_gdf = arcgis_features_to_gdf(wfigs_points)
    wfigs_points_gdf['ReportedAcres'] = wfigs_points_gdf.apply(_assign_reported_acres, axis=1)
    wfigs_points_gdf['FireActivityStatus'] = wfigs_points_gdf.apply(_assign_fire_activity_status, axis=1)
    wfigs_points_gdf['AkFireRegion'] = wfigs_points_gdf.apply(_assign_ak_fire_region, axis=1)
    wfigs_points_gdf['AkFireNumber'] = wfigs_points_gdf.apply(_assign_ak_fire_number, axis=1)
    wfigs_points_ac_bufs_gdf = _create_reported_acres_buffers(wfigs_points_gdf)
    wfigs_points_ac_bufs_gdf[['MapMethod', 'GISAcres', 'PolygonDateTime']] = None
    wfigs_points_ac_bufs_gdf['ReportedAcOverPerimAc' ] = 0
    return wfigs_points_ac_bufs_gdf

def create_wfigs_fire_polys_gdf(wfigs_polys: dict) -> gpd.GeoDataFrame:
    '''
    Converts ArcGIS JSON features for WFIGS fire perimeters to a GeoDataFrame that is processed and formatted for analysis
    alongside WFIGS fire locations.
    
    Args:
        - wfigs_polys (dict) -- ArcGIS features JSON retrieved from WFIGS fire perimeters REST endpoint.

    Returns:
        - gpd.GeoDataFrame
    '''
    wfigs_polys_gdf = arcgis_polygon_features_to_gdf(wfigs_polys)

    wfigs_polys_gdf['ReportedAcres'] = wfigs_polys_gdf.apply(_assign_reported_acres, axis=1)
    wfigs_polys_gdf['ReportedAcOverPerimAc'] = wfigs_polys_gdf['ReportedAcres'] > wfigs_polys_gdf['poly_GISAcres']
    # if poly_GISAcres or ReportedAcres is np.nan, we can get null values from the above comparison.
    wfigs_polys_gdf['ReportedAcOverPerimAc'] = wfigs_polys_gdf['ReportedAcOverPerimAc'].fillna(0)

    wfigs_polys_gdf['FireActivityStatus'] = wfigs_polys_gdf.apply(_assign_fire_activity_status, axis=1)
    wfigs_polys_gdf['AkFireRegion'] = wfigs_polys_gdf.apply(_assign_ak_fire_region, axis=1)
    wfigs_polys_gdf['AkFireNumber'] = wfigs_polys_gdf.apply(_assign_ak_fire_number, axis=1)

    # It appears that event polygon updates in the NIFS do not trigger the WFIGS attr_ModifiedOnDateTime_dt attribute to update.
    # poly_DateCurrent represents information with equivalent meaning for our use case (the last moment in time a record for a fire was altered).
    # Introducing a second timestamp field to the target service would require refactoring code, updating schemas, complicating query logic, and so on.
    # For the time being (20250615) we see if we can instead achieve the desired functionality by
    # overwriting the polygons attr_ModifiedOnDateTime_dt attribute with any poly_DateCurrent attribute that is greater.
    def _safe_max(attr_mod_dt: float, poly_curr_dt: float) -> float:
        if pd.isna(poly_curr_dt):
            return attr_mod_dt
        if pd.isna(attr_mod_dt):
            return poly_curr_dt
        return max(attr_mod_dt, poly_curr_dt)
    wfigs_polys_gdf["attr_ModifiedOnDateTime_dt"] = wfigs_polys_gdf.apply(
        lambda x: _safe_max(x["attr_ModifiedOnDateTime_dt"], x["poly_DateCurrent"]),
        axis=1
    )

    wfigs_polys_field_rename = {
        'attr_IncidentName': 'IncidentName',
        'attr_IrwinID': 'IrwinID',
        'attr_ModifiedOnDateTime_dt': 'ModifiedOnDateTime_dt',
        'poly_MapMethod': 'MapMethod',
        'poly_GISAcres': 'GISAcres',
        'poly_PolygonDateTime': 'PolygonDateTime',
    }
    
    wfigs_polys_gdf.rename(wfigs_polys_field_rename, axis=1, inplace=True)
    return wfigs_polys_gdf

def create_analysis_gdf(wfigs_points_ac_bufs_gdf: gpd.GeoDataFrame, wfigs_polys_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    '''
    Combines pre-processed and formatted WFIGS fire location and fire perimeter data,
    creates buffer features for all fires, assigns a multi-index for feature identification.
    
    Args:
        - wfigs_points_ac_bufs_gdf (gpd.GeoDataFrame) -- Pre-processed and formatted WFIGS fire locations.
        - wfigs_polys_gdf (gpd.GeoDataFrame) -- Pre-processed and formatted WFIGS fire perimeters.

    Returns:
        - gpd.GeoDataFrame -- Contains all fire features used in values-at-risk analysis.
    '''
    new_fires_gdf = pd.concat([wfigs_points_ac_bufs_gdf, wfigs_polys_gdf], ignore_index=True, axis=0)
    new_fires_gdf = new_fires_gdf[[
        'AkFireRegion',
        'AkFireNumber',
        'IncidentName',
        'ReportedAcres',
        'GISAcres',
        'MapMethod',
        'PolygonDateTime',
        'ReportedAcOverPerimAc',
        'ModifiedOnDateTime_dt',
        'FireActivityStatus',
        'IrwinID',
        'geometry'
    ]]
    
    # fire perimeters & reported size buffers will have 'buf_dist' of 0
    # 'buf_dist' will be used in a multi-index and later as a dictionary key, so this can't be None
    new_fires_gdf['buf_dist'] = 0

    # container for appending gdfs and then concatenating them
    analysis_gdf_lst = [new_fires_gdf]

    # distances for buffer creation & attribution
    buf_dist_meters = (1609.34, 4828.03, 8046.72)
    buf_dist_miles = (1, 3, 5)

    # create buffer geodataframe, append to analysis_gdf_lst for concatenation
    for meters, miles in zip(buf_dist_meters, buf_dist_miles):
        buf_gdf = gpd.GeoDataFrame(
            {attrName : new_fires_gdf[attrName] for attrName
            in new_fires_gdf.columns if attrName not in ('geometry', 'buf_dist')}
        )
        buf_gdf['buf_dist'] = miles
        buf_gdf.set_geometry(new_fires_gdf['geometry'].buffer(meters), inplace=True, crs='EPSG:3338')
        analysis_gdf_lst.append(buf_gdf)

    # combine gdfs
    analysis_gdf = pd.concat(analysis_gdf_lst, ignore_index=True, axis=0)

    # create buf_dist & IrwinID multi-index
    analysis_gdf.set_index(keys=['buf_dist','IrwinID'], drop=False, inplace=True)

    return analysis_gdf

def _assign_reported_acres(row: pd.Series) -> int:
    '''
    This helper function is written to be used in a pandas.apply() function.
    It is critical that all WFIGS features have an acreage attribute.
    We preferentially takes an acreage attribute from columns in this order: 
    ['FinalAcres', 'IncidentSize', 'InitialResponseAcres', 'DiscoveryAcres'].
    If an acreage attribute is not found, 0.1 is automatically assigned.
    '''
    try:
        for col in ['FinalAcres', 'IncidentSize', 'InitialResponseAcres', 'DiscoveryAcres']:
            if pd.notna(row[col]):
                return row[col]
    except KeyError:
        for col in ['attr_FinalAcres', 'attr_IncidentSize', 'attr_InitialResponseAcres', 'attr_DiscoveryAcres']:
            if pd.notna(row[col]):
                return row[col]
    return 0.1

def _assign_fire_activity_status(row: pd.Series) -> str:
    '''
    This helper function is written to be used in a pandas.apply() function.
    Assigns FireActivityStatus based on presence of values in associated datetime fields.
    Preferentially takes values in order of ['FireOutDateTime', 'ControlDateTime', 'ContainmentDateTime']
    If no value is found, the fire is considered active.
    '''
    try:
        for col,val in {
            'FireOutDateTime': 'Out',
            'ControlDateTime': 'Controlled',
            'ContainmentDateTime': 'Contained'
        }.items():
            if pd.notna(row[col]):
                return val
    except KeyError:
        for col,val in {
            'attr_FireOutDateTime': 'Out',
            'attr_ControlDateTime': 'Controlled',
            'attr_ContainmentDateTime': 'Contained'
        }.items():
            if pd.notna(row[col]):
                return val
    return 'Active'

def _assign_ak_fire_region(row: pd.Series) -> str:
    '''
    This helper function is written to be used in a pandas.apply() function.
    Parses the UniqueFireIdentifier attribute from either WFIGS locations or perimeters
    and returns the three-digit Alaska fire region ('MSS', for example).
    '''
    try:
        unique_fire_id = row['UniqueFireIdentifier']
    except KeyError:
        unique_fire_id = row['attr_UniqueFireIdentifier']

    id_parts = unique_fire_id.split('-')
    full_region = id_parts[1]
    ak_region = full_region.replace('AK','')

    return ak_region
    
def _assign_ak_fire_number(row: pd.Series) -> int:
    '''
    This helper function is written to be used in a pandas.apply() function.
    Parses the UniqueFireIdentifier attribute from either WFIGS locations or perimeters
    and returns the three-digit Alaska fire number.
    '''
    try:
        unique_fire_id = row['UniqueFireIdentifier']
    except KeyError:
        unique_fire_id = row['attr_UniqueFireIdentifier']

    id_parts = unique_fire_id.split('-')
    full_number = id_parts[2]
    ak_number = full_number[-3:]

    return ak_number

def _create_reported_acres_buffers(wfigs_points_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    '''
    Replaces point geometries of WFIGS fire location features with polygon geometries 
    that are buffers on the initial point geometries with an area equal to the reported fire acres.
    '''
    wfigs_points_gdf['ReportedAcres_m2'] = wfigs_points_gdf['ReportedAcres'] * 4046.86
    wfigs_points_gdf['buf_radius'] = np.sqrt(wfigs_points_gdf['ReportedAcres_m2'] / np.pi)
    wfigs_points_gdf['geometry'] = wfigs_points_gdf.apply(
        lambda row: row['geometry'].buffer(row['buf_radius']),
        axis=1
    )
    return wfigs_points_gdf