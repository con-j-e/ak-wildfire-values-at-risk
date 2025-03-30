import ast
import geopandas as gpd
import pandas as pd
import pathlib
from typing import Sequence
import sys

proj_root = pathlib.Path(__file__).parent.parent
sys.path.append(proj_root)

from utils.general import send_email, utc_epoch_to_ak_time_str

def write_analysis_types_dict(analysis_plan: pd.DataFrame, var_alias: str) -> dict:
    '''
    Create a dictionary of {'ANALYSIS TYPE' : (input_field_a, input_field_b, ...)} pairs
    from the analysis_plan DF for a given var_alias.
    '''
    analysis_types = analysis_plan[
        analysis_plan['ALIAS'] == var_alias
        ].drop(columns='ALIAS').iloc[0].to_dict()
    
    analysis_types = {key: ast.literal_eval(value) for key, value in analysis_types.items() if not pd.isna(value) and value != ''}

    return analysis_types

def batch_write_attr_tups(identifier, buf_dist, var_alias, analysis_types, value=None):
    '''
    Writes all attribution tuples for a given fire feature and value-at-risk input, giving
    each attribute the same value. This value defaults to None.
    '''
    attr_tups = []

    if 'FEATURE_COUNT' in analysis_types:
        attr_tups.append((identifier, buf_dist, f'{var_alias}_feat_count', value))

    if 'TOTAL_ACRES' in analysis_types:
        attr_tups.append((identifier, buf_dist, f'{var_alias}_total_acres', value))

    if 'TOTAL_LENGTH_FT' in analysis_types:
        attr_tups.append((identifier, buf_dist, f'{var_alias}_total_feet', value))

    if 'ACRES_SUM_FIELDS' in analysis_types:
        for field in analysis_types['ACRES_SUM_FIELDS']:
            attr_tups.append((identifier, buf_dist, f'{var_alias}_{field}_acres_sum', value))

    if 'LENGTH_FT_SUM_FIELDS' in analysis_types:
        for field in analysis_types['LENGTH_FT_SUM_FIELDS']:
            attr_tups.append((identifier, buf_dist, f'{var_alias}_{field}_feet_sum', value))

    if 'VALUE_SUM_FIELDS' in analysis_types:
        for field in analysis_types['VALUE_SUM_FIELDS']:
            attr_tups.append((identifier, buf_dist, f'{var_alias}_{field}_value_sum', value))

    if 'ATTR_COUNT_FIELDS' in analysis_types:
        for field in analysis_types['ATTR_COUNT_FIELDS']:
            attr_tups.append((identifier, buf_dist, f'{var_alias}_{field}_attr_count', value))

    return attr_tups

def acdc_update_email(analysis_gdf: gpd.GeoDataFrame, sender: str, recipients: str | Sequence[str], password: str) -> None:
    '''
    Sends an email containing basic information on any AKACDC fires in the latest round of WFIGS updates.

    Args:
        * analysis_gdf (gpd.GeoDataFrame)
        * sender (str)
        * recipients (str | Sequence[str]) 
        * password (str)
    '''   

    acdc_fires = analysis_gdf[
        (analysis_gdf['AkFireRegion'].isin(('CRS','KKS','MSS','SWS'))) &
        (analysis_gdf['buf_dist'] == 0)
    ].copy()

    if len(acdc_fires) < 1:
        return
    
    acdc_fires['PolygonDateTime'] = acdc_fires['PolygonDateTime'].apply(
            lambda x: utc_epoch_to_ak_time_str(x, format_milliseconds=False) + ' AK Time' if pd.notna(x) else x
        )

    acdc_fires['ModifiedOnDateTime_dt'] = acdc_fires['ModifiedOnDateTime_dt'].apply(
        lambda x: utc_epoch_to_ak_time_str(x, format_milliseconds=False) + ' AK Time' if pd.notna(x) else x
    )

    acdc_fires = acdc_fires[
        [
            'AkFireRegion',
            'AkFireNumber',
            'IncidentName',
            'FireActivityStatus',
            'ModifiedOnDateTime_dt',
            'ReportedAcres',
            'GISAcres',
            'MapMethod',
            'PolygonDateTime',
        ]
    ]

    all_fire_updates = []
    for _,row in acdc_fires.iterrows():
        fire_update_lst = []
        attrs = {c : row[c] for c in acdc_fires.columns}
        for attr, val in attrs.items():
            fire_update_lst.append(f'{attr}: {val}')
        fire_update = '\n'.join(fire_update_lst)
        all_fire_updates.append(fire_update)
    subject = 'ACDC WFIGS Features Update'
    body = '\n\n'.join(all_fire_updates)
    send_email(subject, body, sender, recipients, password)

        

