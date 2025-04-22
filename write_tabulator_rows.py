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
        * dof_perims_locs_url -- Layer at index 0 for the AK WF VAR service.
        * query_epoch_millisceonds -- Timestamp used when querying AK WF services.
        * irwins_with_errors -- IrwinIDs of any fires with any errors in any of the current Tabulator JS tables. 
        Used when querying AK WF services to capture features that may have resolved errors but an unchanged wfig_ModifiedOnDateTime_dt value.
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
    Converts features JSON to dataframes, joins wfigs and akdof features based on IrwinID, and formats fields for Tabulator JS.

    Arguments:
        wfigs_features_json -- ArcGIS JSON features from WFIGS
        akdof_features_json -- ArcGIS JSON features from AK WF VAR service.

    Returns:
        pd.DataFrame -- used as input for creating rows of data for Tabulator JS.
        
    '''    

    wfigs_feats_df = arcgis_features_to_dataframe(wfigs_features_json)
    wfigs_feats_df.set_index('IrwinID', inplace=True)
    wfigs_feats_df.drop('geometry', axis=1, inplace=True)

    akdof_feats_gdf = arcgis_features_to_gdf(akdof_features_json)
    akdof_feats_gdf.set_index('wfigs_IrwinID', inplace=True, drop=False)

    akdof_feats_gdf['centroid_3338'] = akdof_feats_gdf['geometry'].centroid
    
    def _estimate_scale(row: pd.Series) -> int:
        
        bbox = row['geometry'].bounds

        # bbox == (minx, miny, maxx, maxy)
        x_distance = bbox[2] - bbox[0]
        y_distance = bbox[3] - bbox[1]
        max_distance = max((x_distance, y_distance))

        # max_distance is in meters (wkid 3338)
        # assuming average screen size of 16 inches, converted to meters
        scale = int(max_distance / (16 * 0.025))

        return scale
    
    akdof_feats_gdf['map_scale'] = akdof_feats_gdf.apply(_estimate_scale, axis=1)

    akdof_feats_gdf['VarAppURL'] = akdof_feats_gdf.apply(
        lambda x: f'https://experience.arcgis.com/experience/e44a6857abe84578971add4c5f862c7d/page/VALUES-AT-RISK/#widget_1=center:{round(x["centroid_3338"].x, 3)}%2C{round(x["centroid_3338"].y, 3)}%2C3338,scale:{x["map_scale"]}', axis=1
    )

    tabulator_df = akdof_feats_gdf.join(wfigs_feats_df, validate='1:1').drop('geometry', axis=1)

    tabulator_df['AkFireNumber'] = tabulator_df['AkFireNumber'].astype(str).str.zfill(3)

    tabulator_df['SpatialInfoType'] = tabulator_df['DefaultLabel'].apply(
        lambda x: x.split(',')[-1]
    )
        
    nested_table_fields = tabulator_plan[tabulator_plan['PRE_PROCESSING'] == 'nested_tabulator']['FIELD_NAME'].to_list()
    tabulator_df[nested_table_fields] = tabulator_df[nested_table_fields].map(
        lambda x: [{'key head': k, 'value head': v} for k,v in json.loads(x).items()] if x != '!error!' else x,
        na_action='ignore'
    )

    json_obj_fields = tabulator_plan[tabulator_plan['PRE_PROCESSING'] == 'json_object']['FIELD_NAME'].to_list()
    tabulator_df[json_obj_fields] = tabulator_df[json_obj_fields].map(
        lambda x: json.loads(x) if x != '!error!' else x,
        na_action='ignore'
    )

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

        # getting AK WF VAR service field names copied from .\planning\schema_plan.tsv
        # 'PRE_PROCESSING' column indicates whether (and how) a field is going to be used by tabulator
        tabulator_plan = pd.read_csv(plans_dir / 'tabulator_plan.tsv', delimiter='\t')

        # '_Nearest' and '_Interior' fields are renamed to '_Locations' and then duplicate field names are dropped
        # this is because '_Nearest' and '_Interior' fields will populate a tabulator column that is defined once for use in multiple tables
        # original '_Interior' fields will populate the perimeters & locations table, and original '_Nearest' fields will populate the buffer analysis tables
        final_fields = list(dict.fromkeys(
            field.replace('_Nearest','_Locations').replace('_Interior','_Locations')
            for field in tabulator_plan[tabulator_plan['PRE_PROCESSING'].notna()]['FIELD_NAME']
        ))

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
            'akdof_perims_locs': pd.DataFrame(columns=['wfigs_IrwinID']),
            'buf_1': pd.DataFrame(columns=['wfigs_IrwinID']),
            'buf_3': pd.DataFrame(columns=['wfigs_IrwinID']),
            'buf_5': pd.DataFrame(columns=['wfigs_IrwinID']) 
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
            max_modified_dt = current_df.iloc[0]['wfigs_ModifiedOnDateTime_dt']
            max_timestamps.add(max_modified_dt)

            error_irwins = current_df[current_df['HasError'] == 1]['wfigs_IrwinID'].to_list()
            irwins_with_errors.update(error_irwins)

            current_tables[name] = current_df

        # logging sanity check
        if len(max_timestamps) > 1:
            logger.warning(f'Existing tabulator tables have different maximum timestamp values! {max_timestamps}')

        # all max_timestamps should be the same, this is a safety measure
        query_epoch_milliseconds = min(max_timestamps)

        nifc_token = checkout_token('NIFC_AGO', 120, 'NIFC_TOKEN', 5)

        # query input feature layers
        wfigs_locs, akdof_perims_locs, buf_1, buf_3, buf_5, exception = asyncio.run(get_recent_fires_info(
                r'https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/AK_Wildfire_Values_at_Risk/FeatureServer/0',
                r'https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/WFIGS_Incident_Locations_YearToDate/FeatureServer/0',
                query_epoch_milliseconds,
                irwins_with_errors,
                nifc_token,
                testing=False
            ))
        
        # an error occurred, log error and exit with code 1
        if None in (wfigs_locs, akdof_perims_locs, buf_1, buf_3, buf_5):
            logger.critical('Unable to retrieve data required for building tables... exiting with code 1.')
            if exception:
                logger.critical(format_logged_exception(*exception))
            sys.exit(1)

        # no features were returned, record timestamp for last update and exit with code 0
        if all([obj['features'] == [] for obj in (wfigs_locs, akdof_perims_locs, buf_1, buf_3, buf_5)]):
            logger.info('No updates to process... exiting with code 0.')
            with open(input_json_dir / 'timestamp.json', 'w') as file:
                json.dump({
                    'datetime': datetime.now(tz=pytz.utc).timestamp() * 1000
                }, file)
            sys.exit(0)

        # logging sanity check
        # feature counts for each query response should always match
        # we should always be retrieving as many features as were recently retrieved and processed by main.py
        feat_counts = {len(obj['features']) for obj in (wfigs_locs, akdof_perims_locs, buf_1, buf_3, buf_5)}
        if len(feat_counts) > 1:
            logger.error(f'Feature counts do not match!')
            logger.error(f'{json.dumps({
                name: len(obj['features']) 
                for name, obj in {
                    'wfigs_locs': wfigs_locs,
                    'akdof_perims_locs': akdof_perims_locs,
                    'buf_1': buf_1,
                    'buf_3': buf_3,
                    'buf_5': buf_5
                }.items()
            })}')
        else:
            logger.info(f'Retrieved features for {next(iter(feat_counts))} distinct fire(s).')

        # used for extracting '_Nearest' and '_Interior' fields that will be used to create '_Locations' fields for tabulator
        akdof_perims_locs_gdf = arcgis_features_to_gdf(akdof_perims_locs)

        # create dataframe for populating columns in each of the buffer analysis tabulator js tables
        nearest_feats_fields = [col for col in akdof_perims_locs_gdf.columns if col.endswith('_Nearest')]
        nearest_feats_df = akdof_perims_locs_gdf[nearest_feats_fields + ['wfigs_IrwinID']].copy()
        nearest_feats_df[nearest_feats_fields] = nearest_feats_df[nearest_feats_fields].map(
            lambda x: json.loads(x) if x != '!error!' else x,
            na_action='ignore'
        )
        nearest_feats_df.set_index('wfigs_IrwinID', inplace=True)

        # create dataframe for populating columns in the perimeters & locations tabulator js table
        interior_feats_fields = [col for col in akdof_perims_locs_gdf.columns if col.endswith('_Interior')]
        interior_feats_df = akdof_perims_locs_gdf[interior_feats_fields + ['wfigs_IrwinID']].copy()
        interior_feats_df[interior_feats_fields] = interior_feats_df[interior_feats_fields].map(
            lambda x: json.loads(x) if x != '!error!' else x,
            na_action='ignore'
        )
        interior_feats_df.set_index('wfigs_IrwinID', inplace=True)

        # this loop writes each of the .json files in .\docs\input_json containing rows of data for tabulator
        # tuple structure is ( json file name , arcgis json features , dataframe containing '_Interior' or '_Nearest' field data )
        for tup in (
            ('akdof_perims_locs', akdof_perims_locs, interior_feats_df),
            ('buf_1', buf_1, nearest_feats_df.copy()),
            ('buf_3', buf_3, nearest_feats_df.copy()),
            ('buf_5', buf_5, nearest_feats_df.copy())
        ):
            name, dof_feats, buf_dist_feats_df = tup
            
            # creating dataframe of new features that will be used to update tabulator rows
            new_df = prepare_dataframe_for_tabulator(wfigs_locs, dof_feats, tabulator_plan)

            # features in '_Interior' fields can be used as-is
            if name == 'akdof_perims_locs':
                buf_dist_feats_df = buf_dist_feats_df.rename(
                    columns={col: col.replace('_Interior','_Locations') for col in buf_dist_feats_df.columns}
                )

            # features in '_Nearest' fields are filtered to include only those inside the analysis buffer distance
            # we know that each grouping of arcgis features can only have a single unique AnalysisBufferMiles attribute
            else:

                buf_dist = new_df['AnalysisBufferMiles'].unique()[0]
                buf_dist_feats_df = buf_dist_feats_df.map(
                    lambda x: {
                        'features': [feat for feat in x['features'] if feat['dist_mi'] <= buf_dist],
                        'popped': x['popped'] if isinstance(x['cutoff'], float) and x['cutoff'] <= buf_dist else 0,
                        'cutoff': x['cutoff'] if isinstance(x['cutoff'], float) and x['cutoff'] <= buf_dist else None
                    } if x not in ('!error!','wfigs_IrwinID') else x,
                    na_action='ignore'
                )
                buf_dist_feats_df = buf_dist_feats_df.map(
                    lambda x: None if isinstance(x, dict) and x['features'] == [] else x,
                    na_action='ignore'
                )
                buf_dist_feats_df = buf_dist_feats_df.rename(
                    columns={col: col.replace('_Nearest','_Locations') for col in buf_dist_feats_df.columns}
                )

            # format new rows for tabulator
            new_df = new_df.join(buf_dist_feats_df, validate='1:1')
            new_df = new_df[final_fields]

            # create dataframe containing old and new tabulator rows
            old_df = current_tables[name]
            old_df.set_index('wfigs_IrwinID', inplace=True, drop=False)
            df = pd.concat((new_df, old_df))
            df.sort_values('wfigs_ModifiedOnDateTime_dt', ascending=False, inplace=True)
            df.drop_duplicates('wfigs_IrwinID', keep='first', inplace=True)
            df = df.replace({np.nan: None})
            
            # save rows of json data for tabulator
            tabulator_rows = []
            df.sort_values('AkFireNumber', ascending=False, inplace=True, key=lambda col: col.astype(int))
            for _,row in df.iterrows():
                tabulator_rows.append(row.to_dict())
            with open(input_json_dir / f'{name}.json', 'w') as file:
                json.dump(tabulator_rows, file, indent=4)
            logger.info(f'{len(tabulator_rows)} rows added to {name} table.')

        # record timestamp for last update
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