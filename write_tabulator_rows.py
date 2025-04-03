import asyncio
from datetime import datetime, timezone
import json
import numpy as np
import os
import pathlib
import pandas as pd
import pytz
import sys
import traceback

from utils.general import basic_file_logger, format_logged_exception, send_email
from utils.arcgis_helpers import AsyncArcGISRequester, arcgis_features_to_dataframe, arcgis_features_to_gdf, checkout_token

async def get_recent_fires_info(dof_perims_locs_url: str, wfigs_locs_url: str, query_epoch_milliseconds: int, irwins_with_errors: set, token: str, testing: bool = False) -> tuple[dict | None, Exception | None]:
    '''
    Query all data required for producing dataframes that will be used to generate JSON inputs for Tabulator JS. 

    Arguments:
        * dof_perims_locs_url -- Layer at index 0 for the AKDOF values-at-risk service.
        * query_epoch_millisceonds -- Timestamp used when querying AKDOF services.
        * irwins_with_errors -- IrwinIDs of any fires with any errors in any of the current Tabulator JS tables. 
        Used when querying AKDOF services to capture features that may have resolved errors but an unchanged wfig_ModifiedOnDateTime_dt value.
        * wfigs_locs_url -- WFIGS Incident Locations hosted by NIFC.
        * token -- ArcGIS REST API token with NIFC portal access. 

    Keyword Arguments:
        * testing -- Indicates whether this is a testing run or a production run. Ensures token is passed when querying a private WFIGS proxy. Defaults to False.

    Returns:
        tuple[dict | None, Exception | None] -- ({ arcgis json features | None } , ...x5 , Exception | None )
        
    '''
    wfigs_locs, akdof_perims_locs, buf_1, buf_3, buf_5 = None, None, None, None, None

    seconds = query_epoch_milliseconds / 1000
    query_timestamp = datetime.fromtimestamp(seconds, timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')

    async with AsyncArcGISRequester() as requester:

        if irwins_with_errors:
            where_clause = f"""wfigs_ModifiedOnDateTime_dt > timestamp '{query_timestamp}' OR
                        wfigs_IrwinID IN ({','.join(f"'{irwin}'" for irwin in irwins_with_errors)})"""
        else:
            where_clause = f"wfigs_ModifiedOnDateTime_dt > timestamp '{query_timestamp}'"

        perims_locs_bufs_params = {
            'f': 'json',
            'where': where_clause,
            'outfields': '*',
            'outSR': 3338,
            'geometryPrecision': 3,
            'token': token
        }

        akdof_perims_locs, buf_1, buf_3, buf_5 = await asyncio.gather(
            requester.arcgis_rest_api_get(
                base_url=dof_perims_locs_url,    
                params=perims_locs_bufs_params,
                operation='query?'
            ),
            requester.arcgis_rest_api_get(
                base_url=f'{dof_perims_locs_url[:-1]}1',    
                params=perims_locs_bufs_params,
                operation='query?'
            ),
            requester.arcgis_rest_api_get(
                base_url=f'{dof_perims_locs_url[:-1]}2',   
                params=perims_locs_bufs_params,
                operation='query?'
            ),
            requester.arcgis_rest_api_get(
                base_url=f'{dof_perims_locs_url[:-1]}3',
                params=perims_locs_bufs_params,
                operation='query?'
            )
        )

        wfigs_query_irwins = set()
        for fset_json in (akdof_perims_locs, buf_1, buf_3, buf_5):
            if fset_json['features']:
                irwins = [feat['attributes'].get('wfigs_IrwinID', None) for feat in fset_json['features']]
                wfigs_query_irwins.update(irwins)

        if wfigs_query_irwins:

            wfigs_locs_outfields = (
                'IrwinID',
                'CpxName',
                'DispatchCenterID',
                'PercentContained',
                'PercentPerimeterToBeContained',
                'FireMgmtComplexity',
                'IncidentComplexityLevel',
                'EstimatedCostToDate',
                'TotalIncidentPersonnel',
                'POOProtectingAgency',
                'POOJurisdictionalAgency',
                'POOJurisdictionalUnit',
                'FireDiscoveryDateTime',
                'ICS209ReportDateTime',
                'ICS209ReportStatus',
                'IncidentManagementOrganization'
            )

            wfigs_locs_params = {
                'f': 'json',
                'where': f"IrwinID IN ({','.join(f"'{irwin}'" for irwin in wfigs_query_irwins)})",
                'outfields': ",".join(wfigs_locs_outfields),
                'returnGeometry': 'false'
            }
            if testing:
                wfigs_locs_params['token'] = token

            wfigs_locs = await requester.arcgis_rest_api_get(
                base_url=wfigs_locs_url,
                params=wfigs_locs_params,
                operation='query?'
            )

        else:
            wfigs_locs = {'features': []}


    return (wfigs_locs, akdof_perims_locs, buf_1, buf_3, buf_5, requester.exception)

def prepare_dataframe_for_tabulator(wfigs_features_json: dict, akdof_features_json: dict, tabulator_plan: pd.DataFrame) -> pd.DataFrame:
    '''
    Converts features JSON to dataframes, createds a bounding box column based on akdof feature geometry,
    joins wfigs and akdof features based on IrwinID, and formats columns for use by Tabulator JS.

    Arguments:
        wfigs_features_json -- ArcGIS JSON features from WFIGS
        akdof_features_json -- ArcGIS JSON features from AKDOF values-at-risk service.

    Returns:
        Dataframe used as input for creating rows of data for Tabulator JS.
        
    '''    

    wfigs_feats_df = arcgis_features_to_dataframe(wfigs_features_json)
    wfigs_feats_df.set_index('IrwinID', inplace=True)
    wfigs_feats_df.drop('geometry', axis=1, inplace=True)

    akdof_feats_gdf = arcgis_features_to_gdf(akdof_features_json)
    akdof_feats_gdf.set_index('wfigs_IrwinID', inplace=True)

    akdof_feats_gdf['centroid_3338'] = akdof_feats_gdf['geometry'].centroid
    
    def _estimate_scale(row: pd.Series) -> int:
        bbox = row['geometry'].bounds

        # bbox == (minx, miny, maxx, maxy)
        x_distance = bbox[2] - bbox[0]
        y_distance = bbox[3] - bbox[1]
        max_distance = max((x_distance, y_distance))

        # max_distance is in meters (wkid 3338)
        # assuming average screen size of 16 inches, converted to meters
        #! need to ground truth this on a variety of features / hyperlinks
        #! hopefully there is some equation that will give me a reasonable scale based on bbox and centroid
        scale = int(max_distance / (16 * 0.025))

        return scale
    
    akdof_feats_gdf['map_scale'] = akdof_feats_gdf.apply(_estimate_scale, axis=1)

    akdof_feats_gdf['VarAppURL'] = akdof_feats_gdf.apply(
        lambda x: f'https://experience.arcgis.com/experience/e44a6857abe84578971add4c5f862c7d/page/VALUES-AT-RISK/#widget_1=center:{round(x["centroid_3338"].x, 3)}%2C{round(x["centroid_3338"].y, 3)}%2C3338,scale:{x["map_scale"]}', axis=1
    )

    tabulator_df = akdof_feats_gdf.join(wfigs_feats_df, validate='1:1').drop('geometry', axis=1)
    tabulator_df.reset_index(inplace=True)

    tabulator_df['AkFireNumber'] = tabulator_df['AkFireNumber'].astype(str).str.zfill(3)

    tabulator_df['SpatialInfoType'] = tabulator_df['DefaultLabel'].apply(
        lambda x: x.split(',')[-1]
    )
    
    def tabulator_rows_from_json(cell, key_head, value_head):

        if pd.isna(cell) or cell == '!error!':
            return cell
        
        rows = []

        for k,v in json.loads(cell).items():
            row = {key_head: k, value_head: v}
            rows.append(row)
        
        return rows

    json_obj_fields = tabulator_plan[tabulator_plan['PRE_PROCESSING'] == 'json_object']['FIELD_NAME'].to_list()
    tabulator_df[json_obj_fields] = tabulator_df[json_obj_fields].map(lambda x: json.loads(x) if (pd.notna(x) and x != '!error!') else x)

    nested_table_fields = tabulator_plan[tabulator_plan['PRE_PROCESSING'] == 'nested_tabulator']['FIELD_NAME'].to_list()
    tabulator_df[nested_table_fields] = tabulator_df[nested_table_fields].map(lambda x: tabulator_rows_from_json(x, 'key head', 'value head'))

    return tabulator_df

def main():

    # whole process is wrapped in a general try-except (fail safe for sending notifications if something unexpected goes wrong)
    # there is targeted exception handling at lower levels
    try:

        sender, recipient, password = os.getenv('SEND_EMAIL_PARAMS').split(',')

        logger = basic_file_logger('write_tabulator_rows_info.log')
        logger.info('STARTING PROCESS')

        proj_dir = pathlib.Path().cwd()
        plans_dir = proj_dir / 'planning'
        input_json_dir = proj_dir / 'docs' / 'input_json'

        tabulator_plan = pd.read_csv(plans_dir / 'tabulator_plan.tsv', delimiter='\t')

        # AKDOF VAR service field names carried over from ./planning/schema_plan.tsv
        # a null 'PRE_PROCESSING' value means the field is not being used at all by tabulator
        # the only 'PRE_PROCESSING' values that programatically dictate specific pre-processing behavior are 'nested_tabulator' and 'json_object'
        final_fields = tabulator_plan[tabulator_plan['PRE_PROCESSING'].notna()]['FIELD_NAME'].tolist()

        # additional fields created by this script
        final_fields.extend(['SpatialInfoType', 'VarAppURL'])

        # additional fields gathered from WFIGS
        final_fields.extend([
            'CpxName',
            'DispatchCenterID',
            'PercentContained',
            'PercentPerimeterToBeContained',
            'FireMgmtComplexity',
            'IncidentComplexityLevel',
            'EstimatedCostToDate',
            'TotalIncidentPersonnel',
            'POOProtectingAgency',
            'POOJurisdictionalAgency',
            'POOJurisdictionalUnit',
            'ICS209ReportDateTime',
            'ICS209ReportStatus',
            'IncidentManagementOrganization'
        ])

        # keeping this block in place for potential future use
        # initiates variables that will cause the script to query all 2025 fire information
        # used for first run of script to gather all 2025 fire information, or if a re-set is ever needed
        '''
        current_tables = {
            'akdof_perims_locs': pd.DataFrame(),
            'buf_1': pd.DataFrame(),
            'buf_3': pd.DataFrame(),
            'buf_5': pd.DataFrame() 
        }

        # 1/1/2025
        query_epoch_milliseconds = 1735693200000

        irwins_with_errors = set()

        '''
        current_tables = {
            'akdof_perims_locs': None,
            'buf_1': None,
            'buf_3': None,
            'buf_5': None
        }

        max_timestamps = set()
        irwins_with_errors = set()
        for name, _ in current_tables.items():

            with open(input_json_dir / f'{name}.json', 'r') as file:
                current_rows = json.load(file)

            current_df = pd.DataFrame(current_rows)
            current_df.sort_values('wfigs_ModifiedOnDateTime_dt', ascending=False, inplace=True)
            max_modified_dt = current_df.loc[0]['wfigs_ModifiedOnDateTime_dt']
            max_timestamps.add(max_modified_dt)

            error_irwins = current_df[current_df['HasError'] == 1]['wfigs_IrwinID'].to_list()
            irwins_with_errors.update(error_irwins)

            current_tables[name] = current_df

        # all max_timestamps should be the same, this is mostly just for peace of mind
        query_epoch_milliseconds = min(max_timestamps)

        nifc_token = checkout_token('NIFC_AGO', 120, 'NIFC_TOKEN', 5)

        wfigs_locs, akdof_perims_locs, buf_1, buf_3, buf_5, exception = asyncio.run(get_recent_fires_info(
                r'https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/AK_Wildfire_Values_at_Risk/FeatureServer/0',
                r'https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/WFIGS_Incident_Locations_YearToDate/FeatureServer/0',
                query_epoch_milliseconds,
                irwins_with_errors,
                nifc_token,
                testing=False
            ))
        
        if None in (wfigs_locs, akdof_perims_locs, buf_1, buf_3, buf_5):
            logger.critical('Unable to retrieve data required for building tables... exiting with code 1.')
            if exception:
                logger.critical(format_logged_exception(*exception))
            sys.exit(1)

        if all([obj['features'] == [] for obj in [wfigs_locs, akdof_perims_locs, buf_1, buf_3, buf_5]]):
            logger.info('No updates to process... exiting with code 0.')
            with open(input_json_dir / 'timestamp.json', 'w') as file:
                json.dump({
                    'datetime': datetime.now(tz=pytz.utc).timestamp() * 1000
                }, file)
            sys.exit(0)

        for name, dof_feats in {
            'akdof_perims_locs': akdof_perims_locs,
            'buf_1': buf_1,
            'buf_3': buf_3,
            'buf_5': buf_5
        }.items():
            
            new_df = prepare_dataframe_for_tabulator(wfigs_locs, dof_feats, tabulator_plan)
            old_df = current_tables[name]

            df = pd.concat((new_df, old_df))
            df.sort_values('wfigs_ModifiedOnDateTime_dt', ascending=False, inplace=True)
            df.drop_duplicates('wfigs_IrwinID', keep='first', inplace=True)

            df = df[final_fields]

            df = df.replace({np.nan: None})
            tabulator_rows = []
            for _,row in df.iterrows():
                tabulator_rows.append(row.to_dict())

            with open(input_json_dir / f'{name}.json', 'w') as file:
                json.dump(tabulator_rows, file, indent=4)

            logger.info(f'{len(tabulator_rows)} rows added to {name} table.')

        with open(input_json_dir / 'timestamp.json', 'w') as file:
            json.dump({
                'datetime': datetime.now(tz=pytz.utc).timestamp() * 1000
            }, file)

        logger.info('FINISHED PROCESS')

    except Exception:
        tb = traceback.format_exc()
        subject = f'ak-wildfire-values-at-risk, build_js_tables.py, general Exception.'
        send_email(subject, tb, sender, recipient, password)
        raise

if __name__ == "__main__":
    main()