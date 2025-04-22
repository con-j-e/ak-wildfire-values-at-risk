from arcgis.features import FeatureSet
import asyncio
import geopandas as gpd
import json
import numpy as np
import pandas as pd
import pathlib
import sys

proj_root = pathlib.Path(__file__).parent.parent
sys.path.append(proj_root)

from utils.arcgis_helpers import AsyncArcGISRequester, assign_wkid_3338

def format_fields(fires_bufs_attrs_gdf: gpd.GeoDataFrame, schema_plan: pd.DataFrame) -> gpd.GeoDataFrame:
    '''
    Enforces schema required for updating hosted services.

    Args:
        fires_bufs_attrs_gdf (gpd.GeoDataFrame): Contains all features to be used in the update.
        schema_plan (pd.DataFrame): Determines how formatting is handled for different dtypes.

    Returns:
        gpd.GeoDataFrame: Contains all features to be used in the update.
    '''

    fires_bufs_attrs_gdf = _write_default_label(fires_bufs_attrs_gdf)

    fires_bufs_attrs_gdf['HasError'] = 0

    rename_dict = dict(zip(schema_plan['PROCESSING_NAME'], schema_plan['FIELD_NAME']))

    fires_bufs_attrs_gdf.rename(rename_dict, axis=1, inplace=True)

    esri_string_cols = schema_plan[schema_plan['ESRI_FIELD_TYPE'] == 'esriFieldTypeString']['FIELD_NAME']

    esri_small_integer_cols = schema_plan[schema_plan['ESRI_FIELD_TYPE'] == 'esriFieldTypeSmallInteger']['FIELD_NAME']

    esri_integer_cols = schema_plan[schema_plan['ESRI_FIELD_TYPE'] == 'esriFieldTypeInteger']['FIELD_NAME']

    esri_double_cols = schema_plan[schema_plan['ESRI_FIELD_TYPE'] == 'esriFieldTypeDouble']['FIELD_NAME']

    ## string formatting
    fires_bufs_attrs_gdf[esri_string_cols] = fires_bufs_attrs_gdf[esri_string_cols].astype('string')
    string_replace_dict = {
        '': pd.NA,
        '{}': pd.NA,
        '!EXCEPTION!': '!error!',
        '!QUERYERROR!': '!error!',
        '!UNEXPECTED!': '!error!',
        '!ANALYSISERROR!': '!error!'
    }
    fires_bufs_attrs_gdf[esri_string_cols] = fires_bufs_attrs_gdf[esri_string_cols].replace(string_replace_dict)

    ## small integer formatting
    int_small_int_replace_dict = {
        0: pd.NA,
        '!EXCEPTION!': -1,
        '!QUERYERROR!': -1,
        '!UNEXPECTED!': -1,
        '!ANALYSISERROR!': -1
    }

    # some small integer columns need to retain 0 as a value
    small_int_nullable_cols = [col for col in esri_small_integer_cols if col not in ('AnalysisBufferMiles', 'ReportedAcOverPerimAc', 'HasError')]

    fires_bufs_attrs_gdf[small_int_nullable_cols] = fires_bufs_attrs_gdf[small_int_nullable_cols].replace(int_small_int_replace_dict)
    fires_bufs_attrs_gdf[esri_small_integer_cols] = fires_bufs_attrs_gdf[esri_small_integer_cols].apply(pd.to_numeric, errors='coerce')
    fires_bufs_attrs_gdf[esri_small_integer_cols] = fires_bufs_attrs_gdf[esri_small_integer_cols].astype('Int16')

    ## integer formatting
    fires_bufs_attrs_gdf[esri_integer_cols] = fires_bufs_attrs_gdf[esri_integer_cols].replace(int_small_int_replace_dict)
    fires_bufs_attrs_gdf[esri_integer_cols] = fires_bufs_attrs_gdf[esri_integer_cols].apply(pd.to_numeric, errors='coerce')
    fires_bufs_attrs_gdf[esri_integer_cols] = fires_bufs_attrs_gdf[esri_integer_cols].astype('Int32')

    ## double formatting
    double_replace_dict = {
        0.0: np.nan,
        '!EXCEPTION!': -1,
        '!QUERYERROR!': -1,
        '!UNEXPECTED!': -1,
        '!ANALYSISERROR!': -1
    }
    fires_bufs_attrs_gdf[esri_double_cols] = fires_bufs_attrs_gdf[esri_double_cols].replace(double_replace_dict)
    fires_bufs_attrs_gdf[esri_double_cols] = fires_bufs_attrs_gdf[esri_double_cols].apply(pd.to_numeric, errors='coerce')
    fires_bufs_attrs_gdf[esri_double_cols] = fires_bufs_attrs_gdf[esri_double_cols].astype('float64')

    ## HasError attribution
    # if a single record has any error, all records sharing that IrwinID will receive HasError = 1. 
    error_condition = (fires_bufs_attrs_gdf == '!error!') | (fires_bufs_attrs_gdf == -1)
    irwins_with_errors = fires_bufs_attrs_gdf[error_condition.any(axis=1)]['wfigs_IrwinID']
    fires_bufs_attrs_gdf.loc[fires_bufs_attrs_gdf['wfigs_IrwinID'].isin(irwins_with_errors), 'HasError'] = 1

    ## final column ordering
    #? this doesn't really matter, order is ultimately determined by layer definitions
    fires_bufs_attrs_gdf = fires_bufs_attrs_gdf[schema_plan['FIELD_NAME'].to_list() + ['geometry']]

    return fires_bufs_attrs_gdf

def create_output_feature_lists(fires_bufs_attrs_gdf: gpd.GeoDataFrame) -> dict[int, list]:
    '''
    Produce lists of ArcGIS features that will be used to update hosted services.

    Args:
        * fires_bufs_attrs_gdf (gpd.GeoDataFrame) -- contains all features to be used in the update.

    Returns:
        * dict[int, list] -- Contains {buffer_distance : list of arcgis feature JSONs} pairs for each buf_dist value in fires_bufs_attrs_gdf.
    '''

    # will hold {buf_dist : feature_json_list} pairs, to be returned
    feat_dict = {}

    gdf_groups = fires_bufs_attrs_gdf.groupby('AnalysisBufferMiles')
    for group in gdf_groups:
        buf_dist = group[0]
        gdf = group[1]

        #^ should write an arcgis helper that reads gdf crs, converts to arcgis json, assigns gdf crs to json feats
        geojson = json.loads(gdf.to_json())
        feature_set = FeatureSet.from_geojson(geojson).to_dict()
        # arcgis ignores the crs property of geojson, so we must set the spatial reference for our features explicitly
        features_3338 = [assign_wkid_3338(feature) for feature in feature_set['features']]

        feat_dict[buf_dist] = features_3338

    return feat_dict

async def apply_edits_to_dof_var_service(perims_locs_url: str, token: str, irwins_with_updates: list, feat_dict: dict[int, list]) -> tuple[list[dict], Exception | None]:
    '''
    Update all layers in the target service.

    Args:
        perims_locs_url (str): Target service layer at index 0.
        token (str): ArcGIS Online token with NIFC portal access.
        irwins_with_updates (list): Used to delete features from the target service that have updated information in this round of edits.
        feat_dict (dict[int, list]): All features that will be used to update the target service.

    Returns:
        tuple[list[dict], Exception | None]: 
    '''
    irwins_with_updates_query = f"wfigs_IrwinID IN ({','.join(f"'{irwin}'" for irwin in irwins_with_updates)})"

    one_mile_bufs_url = f'{perims_locs_url[:-1]}1'
    three_mile_bufs_url = f'{perims_locs_url[:-1]}2'
    five_mile_bufs_url = f'{perims_locs_url[:-1]}3'

    layer_edits_dict = {
        perims_locs_url: feat_dict[0],
        one_mile_bufs_url: feat_dict[1],
        three_mile_bufs_url: feat_dict[3],
        five_mile_bufs_url: feat_dict[5]
    }

    async with AsyncArcGISRequester() as requester:
        all_edits_response = await asyncio.gather(
            *(requester.applyEdits_request(
                url,
                token,
                features_to_add,
                irwins_with_updates_query
            ) for url, features_to_add in layer_edits_dict.items())
        )

    return (all_edits_response, requester.exception)

def find_apply_edits_failure(all_edits_response: list[dict]) -> list[tuple | dict]:
    '''
    Checks list of applyEdits responses for any edit results that are an error message or where {'success': False}.
    
    Args:
        * all_edits_response (list[dict]) -- Contains applyEdits responses

    Returns:
        * list -- Contains tuples of (result type, result objects where {'success': False}). Can contain an API error message as a dictionary.
    '''    
    failures = []
    for result_group in all_edits_response:
        if 'error' in result_group:
            failures.append(result_group)
            continue
        for result_type in ['addResults', 'updateResults', 'deleteResults']:
            for item in result_group.get(result_type, []):
                if item.get('success') == False:
                    failures.append((result_type, item))
    return failures

def find_apply_edits_success(all_edits_response: list[dict]) -> list[tuple]:
    '''
    Checks list of applyEdits responses for any edit results where {'success': True}.
    
    Args:
        * all_edits_response (list[dict]) -- Contains applyEdits responses

    Returns:
        * list[tuple] -- Contains tuples of (result type, result objects where {'success': True})
    '''    
    successes = []
    for result_group in all_edits_response:
        for result_type in ['addResults', 'updateResults', 'deleteResults']:
            for item in result_group.get(result_type, []):
                if item.get('success') == True:
                    successes.append((result_type, item))
    return successes

def _write_default_label(fires_bufs_attrs_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

    fires_bufs_attrs_gdf['DefaultLabel'] = None
    for idx, row in fires_bufs_attrs_gdf.iterrows():
        fire_number = row['AkFireNumber']
        fire_name = row['IncidentName']
        if row['buf_dist'] == 0:
            if pd.isna(row['GISAcres']):
                geom_class = 'Reported Location'
            else:
                geom_class = 'Perimeter'
        else:
            geom_class = f'{row['buf_dist']} Mile Buffer'

        fires_bufs_attrs_gdf.at[idx, 'DefaultLabel'] = f'{fire_number}-{fire_name}, {geom_class}'

    return fires_bufs_attrs_gdf