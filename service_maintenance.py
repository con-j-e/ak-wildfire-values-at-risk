import asyncio
from datetime import datetime, timedelta, timezone
import geopandas as gpd
import json
import os
import numpy as np
import pandas as pd
import pathlib
import pickle as pkl
import pytz
import sys
import traceback
import time

from utils.arcgis_helpers import AsyncArcGISRequester, checkout_token
from utils.general import basic_file_logger, format_logged_exception, send_email
from process.output import find_apply_edits_failure, find_apply_edits_success

async def archive_dof_var_service(perims_locs_url: str, token: str) -> tuple[gpd.GeoDataFrame | None, list | None, Exception | None] | None:
    '''
    Deletes features for fires that are 'Out' from all layers in the target service.
    Loads all deleted features into a single GeoDataFrame.

    Arguments:
        * perims_locs_url -- Target service layer at index 0.
        * token -- ArcGIS Online token with NIFC portal access.

    Raises:
        * ValueError: Concurrent applyEdits_archiver() calls on the target service returned different object types.

    Returns:
        * tuple[gpd.GeoDataFrame | None, list | None, Exception | None] | None
            * gpd.GeoDataFrame -- Contains features returned by query? GET requests.
            * list -- Contains responses from applyEdits POST requests.
            * Exception -- exception attribute of the requester instance used (this is None under normal conditions).
    '''    
    archive_query = f"FireActivityStatus = 'Out'"
    
    one_mile_bufs_url = f'{perims_locs_url[:-1]}1'
    three_mile_bufs_url = f'{perims_locs_url[:-1]}2'
    five_mile_bufs_url = f'{perims_locs_url[:-1]}3'

    async with AsyncArcGISRequester() as requester:
        all_archiver_edits_response = await asyncio.gather(
            *(requester.applyEdits_archiver(
                url,
                token,
                archive_query
            ) for url in (perims_locs_url, one_mile_bufs_url, three_mile_bufs_url, five_mile_bufs_url))
        )

    if requester.exception:
        return (None, None, requester.exception)

    response_count = len(all_archiver_edits_response)
    none_response = [obj for obj in all_archiver_edits_response if obj is None]
    tup_response = [obj for obj in all_archiver_edits_response if isinstance(obj, tuple)]

    if len(none_response) == response_count:
        return None
    
    if len(tup_response) != response_count:
        raise ValueError('applyEdits_archiver() calls made concurrently on all layers in the AK WF VAR service should always return the same types of objects!')
    
    archive_gdfs = [tup[0] for tup in tup_response]
    archive_gdf = pd.concat(archive_gdfs, ignore_index=True, axis=0)
    archive_gdf.set_index(keys=['AnalysisBufferMiles','wfigs_IrwinID'], drop=False, inplace=True)

    archive_edits_responses = [tup[1] for tup in tup_response]

    return (archive_gdf, archive_edits_responses, requester.exception)

async def purge_features_gone_from_wfigs(perims_locs_url: str, wfigs_locs_url: str, token: str) -> tuple[list | None, set | None, Exception | None] | None:
    '''
    Deletes features from the AK WF VAR service with IrwinIDs no longer represented in WFIGS.

    Arguments:
        * perims_locs_url -- Target service layer at index 0.
        * wfigs_locs_url -- WFIGS incident locations.
        * token -- ArcGIS Online token with NIFC portal access.

    Raises:
        * KeyError: Expected keys not found in query response.

    Returns:
        * tuple[list | None, set | None] | None
            * list -- Contains responses from applyEdits POST requests.
            * set -- Contains attributes of features that were deleted from the AK WF VAR service.
            * Exception -- exception attribute of the requester instance used (this is None under normal conditions).
    '''    
    one_mile_bufs_url = f'{perims_locs_url[:-1]}1'
    three_mile_bufs_url = f'{perims_locs_url[:-1]}2'
    five_mile_bufs_url = f'{perims_locs_url[:-1]}3'

    async with AsyncArcGISRequester() as requester:

        current_ak_wf_var_feats = await asyncio.gather(
            *(requester.arcgis_rest_api_get(
                base_url=url,
                params={'f':'json', 'where':'1=1', 'token':token, 'outfields':'AkFireNumber,wfigs_IncidentName,wfigs_IrwinID', 'returnGeometry':'false'},
                operation='query?'
            ) for url in (perims_locs_url, one_mile_bufs_url, three_mile_bufs_url, five_mile_bufs_url))
        )

        ak_wf_var_irwins = set()
        for response in current_ak_wf_var_feats:
            try:
                ak_wf_var_irwins.update([feat['attributes']['wfigs_IrwinID'] for feat in response['features']])
            except KeyError:
                raise KeyError(f'Expected keys not found in query response: {response}')

        # Because excessively long request parameters can result in http 404 responses,
        # we make irwin id queries in batches of twenty.
        # This is not being done efficiently, but for the task at hand, effiency is not a factor.
        ak_wf_var_irwins_chunks = list(ak_wf_var_irwins)
        ak_wf_var_irwins_chunks = [tuple(ak_wf_var_irwins_chunks[i: i + 20]) for i in range(0, len(ak_wf_var_irwins), 20)]

        current_wfigs_irwins = set()
        for irwins_chunk in ak_wf_var_irwins_chunks:
            current_wfigs_feats = await requester.arcgis_rest_api_get(
                base_url=wfigs_locs_url,
                params={'f':'json', 'token':token, 'outfields':'IrwinID', 'where':f"IrwinID IN ({','.join(f"'{irwin}'" for irwin in irwins_chunk)})", 'returnGeometry':'false'},
                operation='query?'
            )
            try:
                current_wfigs_irwins.update([feat['attributes']['IrwinID'] for feat in current_wfigs_feats['features']])
            except KeyError:
                raise KeyError(f'Expected keys not found in query response: {current_wfigs_feats}')
            time.sleep(5)
        
        ak_wf_var_irwins.difference_update(current_wfigs_irwins)

        if len(ak_wf_var_irwins) < 1:
            return None
        
        purge_responses = await asyncio.gather(
            *(requester.applyEdits_request(
                url=url,
                token=token,
                features_to_add=[],
                get_oids_to_delete_query=f"wfigs_IrwinID IN ({','.join(f"'{irwin}'" for irwin in ak_wf_var_irwins)})"
            ) for url in (perims_locs_url, one_mile_bufs_url, three_mile_bufs_url, five_mile_bufs_url))
        )

    if requester.exception:
        return (None, None, requester.exception)

    purged_attrs = set()
    for response in current_ak_wf_var_feats:
        for feat in response['features']:
            if feat['attributes']['wfigs_IrwinID'] in ak_wf_var_irwins:
                purged_attrs.add((frozenset(feat['attributes'].items())))
 
    return (purge_responses, purged_attrs, requester.exception)

def main():

    # whole process is wrapped in a general try-except (fail safe for sending notifications if something unexpected goes wrong)
    # there is targeted exception handling at lower levels
    try:

        sender, recipient, password = os.getenv('SEND_EMAIL_PARAMS').split(',')

        logger = basic_file_logger('service_maintenance_info.log')
        logger.info('STARTING PROCESS')

        proj_dir = pathlib.Path().cwd()
        archive_dir = proj_dir / 'out_fires_archive'
        input_json_dir = proj_dir / 'docs' / 'input_json'

        # for tracking any IrwinIDs that get removed from the AK WF VAR service (due to archiving or purging)
        # so associated rows of data can be removed from the json inputs for Tabulator JS
        delete_tabulator_irwins = set()

        nifc_token = checkout_token('NIFC_AGO', 120, 'NIFC_TOKEN', 5)

        all_archiver_edits_response = asyncio.run(archive_dof_var_service(
            perims_locs_url=r'https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/AK_Wildfire_Values_at_Risk/FeatureServer/0',
            token=nifc_token
        ))

        if all_archiver_edits_response is not None:
            archive_gdf, archive_edits_responses, exception = all_archiver_edits_response
            if exception:
                logger.critical('Requester instance exited early with an exception! Exiting with code 1...')
                logger.critical(format_logged_exception(*exception))
                sys.exit(1)

            delete_tabulator_irwins.update(archive_gdf['wfigs_IrwinID'].to_list())

            log_gdf = archive_gdf[['AkFireNumber','wfigs_IncidentName','wfigs_IrwinID']].drop_duplicates()
            logger.info('Features for the following incidents were removed from the AK WF VAR service to be archived-->')
            logger.info(f'({len(log_gdf)} unique fire(s), {len(archive_gdf)} total features)')
            for _, row in log_gdf.iterrows():
                logger.info(json.dumps(row.to_dict()))

            # Because the service schema will change over time,
            # the archive of 'Out' fires will be built as a directory of timestamped geodataframe pickles.
            # Each geodataframe will contain features from all layers in the service,
            # with a multi-index using AnalysisBufferMiles and wfigs_IrwinID for unique feature identification.
            archive_dt = datetime.now(tz=pytz.utc).strftime('%Y%m%d_%H%M')
            with open(archive_dir / f'GeoDataFrame_{archive_dt}_utc.pkl', 'wb') as file:
                pkl.dump(archive_gdf, file)

            failures = find_apply_edits_failure(archive_edits_responses)
            if failures:
                logger.error('applyEdits failure detected!')
                for fail in failures:
                    logger.error(json.dumps(fail))
            successes = find_apply_edits_success(archive_edits_responses)
            if successes:
                logger.info('applyEdits success detected.')
                for success in successes:
                    logger.info(json.dumps(success))

        purge = asyncio.run(purge_features_gone_from_wfigs(
            perims_locs_url=r'https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/AK_Wildfire_Values_at_Risk/FeatureServer/0',
            wfigs_locs_url=r'https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/WFIGS_Incident_Locations_YearToDate/FeatureServer/0',
            token=nifc_token
        ))

        if purge is not None:
            purge_responses, purged_attrs, exception = purge
            if exception:
                logger.critical('Requester instance exited early with an exception! Exiting with code 1...')
                logger.critical(format_logged_exception(*exception))
                sys.exit(1)

            delete_tabulator_irwins.update([dict(attrs)['wfigs_IrwinID'] for attrs in purged_attrs])

            logger.warning('Features from the AK WF VAR service have been deleted, due to IrwinIDs no longer being found in WFIGS.')
            failures = find_apply_edits_failure(purge_responses)
            if failures:
                logger.error('applyEdits failure detected!')
                for fail in failures:
                    logger.error(json.dumps(fail))
            successes = find_apply_edits_success(purge_responses)
            if successes:
                logger.info('applyEdits success detected.')
                for success in successes:
                    logger.info(json.dumps(success))

            logger.info('The following incidents were deleted from the AK WF VAR service-->')
            logger.info(f'({len(purged_attrs)} unique fire(s))')
            for attrs in purged_attrs:
                logger.info(json.dumps(dict(attrs)))

        if delete_tabulator_irwins:
            current_tables = {
                'akdof_perims_locs': None,
                'buf_1': None,
                'buf_3': None,
                'buf_5': None
            }

            for name, _ in current_tables.items():

                with open(input_json_dir / f'{name}.json', 'r') as file:
                    current_rows = json.load(file)

                current_df = pd.DataFrame(current_rows)

                purged_df = current_df[~current_df['wfigs_IrwinID'].isin(delete_tabulator_irwins)].copy()
                purged_df.sort_values('AkFireNumber', ascending=False, inplace=True, key=lambda col: col.astype(int))
                purged_df = purged_df.replace({np.nan: None})

                tabulator_rows = []
                for _,row in purged_df.iterrows():
                    tabulator_rows.append(row.to_dict())
                with open(input_json_dir / f'{name}.json', 'w') as file:
                    json.dump(tabulator_rows, file, indent=4)
                logger.info(f'{len(current_df) - len(purged_df)} rows removed from {name} table.')
                    
        logger.info('FINISHED PROCESS')

    except Exception:
        tb = traceback.format_exc()
        subject = f'ak-wildfire-values-at-risk, service_maintenance.py, general Exception.'
        send_email(subject, tb, sender, recipient, password)
        raise

if __name__ == "__main__":
    main()