import asyncio
import geopandas as gpd
import json
import multiprocessing
import os
import pathlib
import pandas as pd
import shapely as shp
import sys
import time
import traceback

from utils.general import basic_file_logger, format_logged_exception, send_email
from utils.project import acdc_update_email
from utils.arcgis_helpers import checkout_token, fresh_pickles
from process.prepare_wfigs_inputs import get_wfigs_updates, prevent_perimeter_overwrite_by_point, create_wfigs_fire_points_gdf, create_wfigs_fire_polys_gdf, create_analysis_gdf
from process.queries import gather_query_bundles, send_all_queries, handle_query_response_pools
from process.analysis import gather_analysis_pairs, gather_processes, gather_results, create_attribute_dataframe, join_fires_bufs_attributes, parse_analysis_errors
from process.output import format_fields, create_output_feature_lists, apply_edits_to_dof_var_service, find_apply_edits_failure, find_apply_edits_success

def main():

    # whole process is wrapped in a general try-except (fail safe for sending notifications if something unexpected goes wrong)
    # there is targeted exception handling at lower levels
    try:

        #REGION SETUP

        sender, recipient, password = os.getenv('SEND_EMAIL_PARAMS').split(',')

        logger = basic_file_logger('main_info.log')
        logger.info('STARTING PROCESS')

        proj_dir = pathlib.Path.cwd()
        plans_dir = proj_dir / 'planning'

        # accessing the nifc portal is considered critical (cannot query or applyEdits to target service without a token)
        try:
            nifc_token = checkout_token('NIFC_AGO', 120, 'NIFC_TOKEN', 15)
        except Exception as e:
            logger.critical('Unable to access NIFC portal... exiting with code 1.')
            exc_type, exc_val, exc_tb = type(e), e, e.__traceback__
            logger.critical(format_logged_exception(exc_type, exc_val, exc_tb))
            sys.exit(1)

        # accessing the dnr portal is not considered critical (but queries against private dnr services will fail without a token)
        try:
            dnr_token = checkout_token('DNR_AGO', 120, 'DNR_TOKEN', 15)
        except Exception as e:
            logger.error('Unable to access DNR portal.')
            exc_type, exc_val, exc_tb = type(e), e, e.__traceback__
            logger.error(format_logged_exception(exc_type, exc_val, exc_tb))
            dnr_token = None

        token_dict = {'nifc': nifc_token, 'dnr': dnr_token}

        # these plans dictate which inputs are queried, how inputs are queried, how results are analyzed, and how outputs are represented
        # modifying plans is an easy way to adjust any of the above
        # viewing plans can provide a high level overview of what the main process is currently doing
        query_plan = pd.read_csv(plans_dir / 'query_plan.tsv', delimiter='\t')
        analysis_plan = pd.read_csv(plans_dir / 'analysis_plan.tsv', delimiter='\t')
        schema_plan = pd.read_csv(plans_dir / 'schema_plan.tsv', delimiter='\t')

        #ENDREGION

        #REGION PREPARE WFIGS INPUTS

        wfigs_points, wfigs_polys, irwins_with_errors, check_json_pickles, exception = asyncio.run(
            get_wfigs_updates(
                r'https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/AK_Wildfire_Values_at_Risk/FeatureServer',
                r'https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/WFIGS_Incident_Locations_YearToDate/FeatureServer/0',
                r'https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/WFIGS_Interagency_Perimeters_YearToDate/FeatureServer/0',
                token_dict['nifc'],
                testing=False
            )
        )

        # any error during get_wfigs_updates() will have the same implications for main
        # wfigs_points and/or wfigs_polys will remain None
        # so log the error and exit with code 1, no work should be done during this cycle
        if wfigs_points is None or wfigs_polys is None:
            logger.critical('Unable to retrieve WFIGS updates... exiting with code 1.')
            if exception:
                logger.critical(format_logged_exception(*exception))
            sys.exit(1)
        else:
            logger.info(
                json.dumps(
                    {
                        'WFIGS points retrieved': len(wfigs_points['features']),
                        'WFIGS polygons retrieved': len(wfigs_polys['features'])
                    }
                )
            )
        
        wfigs_cache = proj_dir / 'wfigs_json_pickles'
        wfigs_points['features'] = prevent_perimeter_overwrite_by_point(wfigs_cache, wfigs_points['features'])

        # filtering out features that have already been analyzed
        # fires recently called 'Out' can repeatedly be returned by timestamp query, because they are removed from the target service and archived
        # to further reduce/eliminate redundant processing, pass the ModifiedOnDateTime_dt attribute to fresh_pickles() ignore_attributes arg
            # note that this would create a discrepency between the modified dt attribute in WFIGS and the modified dt attribute in the target service!
        if check_json_pickles:
            wfigs_points['features'] = fresh_pickles(wfigs_cache, wfigs_points['features'], 'IrwinID', exempt_identifiers=irwins_with_errors)
            wfigs_polys['features'] = fresh_pickles(wfigs_cache, wfigs_polys['features'], 'attr_IrwinID', exempt_identifiers=irwins_with_errors)
            logger.info(
                json.dumps(
                    {
                        'New WFIGS points to process': len(wfigs_points['features']),
                        'New WFIGS polygons to process': len(wfigs_polys['features'])
                    }
                )
            )

        # assign place-holder empty GDFs as flags for exiting main if there are no updates to process
        # and to ensure arguments are always available for create_analysis_gdf()
        wfigs_points_gdf, wfigs_polys_gdf = gpd.GeoDataFrame(), gpd.GeoDataFrame()

        if 'features' in wfigs_points and len(wfigs_points['features']) > 0:
            wfigs_points_gdf = create_wfigs_fire_points_gdf(wfigs_points)

        if 'features' in wfigs_polys and len(wfigs_polys['features']) > 0:
            wfigs_polys_gdf = create_wfigs_fire_polys_gdf(wfigs_polys)

        if len(wfigs_points_gdf) < 1 and len(wfigs_polys_gdf) < 1:
            logger.info('No WFIGS updates to process... exiting with code 0.')
            sys.exit(0)

        analysis_gdf = create_analysis_gdf(wfigs_points_gdf, wfigs_polys_gdf)

        # this is not relevant to any core functionality
        acdc_update_email(analysis_gdf, sender, recipient, password)

        #ENDREGION

        #REGION QUERIES
        
        query_bundles = gather_query_bundles(
            analysis_gdf=analysis_gdf,
            query_plan=query_plan,
            token_dict=token_dict
            )
        
        t0 = time.time()

        query_responses, exception = asyncio.run(send_all_queries(query_bundles))

        # this condition should not even be possible
        # first exceptions will be present in query_responses as (result_identifier, url_alias, (exc_type, exc_val, exc_tb))
        # then with asyncio.gather(..., return_exceptions=True), query_responses would contain an Exception object if the expected tuple could not be returned
        # finally, the exception attribute of the requester class instance in get_var_features() would be populated with (exc_type, exc_val, exc_tb)
        if exception:
            logger.critical('Exception propogated during asynchronous queries... exiting with code 1.')
            logger.critical(format_logged_exception(*exception))
            sys.exit(1)

        # also should not be possible for Exception object to be present in query_responses
        # we check just in case, and reduce any Exception object to enable pickling during multiprocessing
        query_response_reduced_exceptions = [resp.__reduce__() for resp in query_responses if isinstance(resp, Exception)]
        query_responses = [resp for resp in query_responses if not isinstance(resp, Exception)]
        query_responses.extend(query_response_reduced_exceptions)

        t1 = time.time()

        logger.info(
            json.dumps(
                {
                    'queries completed': len(query_responses),
                    'seconds': round(t1-t0, 2)
                }
            )
        )

        # batch size determined dynamically based on ratio of query responses to analysis zones
        batch_size = len(query_responses) // len(analysis_gdf)

        t0 = time.time()

        query_features_dict, results_no_analysis, logger_dict = handle_query_response_pools(query_responses, analysis_plan, batch_size)

        t1 = time.time()

        logger.info(
            json.dumps(
                {
                    'function': 'handle_query_response_pools()',
                    'seconds': round(t1-t0, 2)
                }
            )
        )

        # _handle_query_responses() only ever passes along critical and/or error messages
        critical = False
        for level, messages in logger_dict.items():
            if level == 'error':
                for m in messages:
                    logger.error(m)
            elif level == 'critical':
                for m in messages:
                    logger.critical(m)
                critical = True
        if critical:
            sys.exit(1)

        #ENDREGION

        #REGION ANALYSIS

        analysis_gdf['geometry'].apply(shp.prepare)

        analysis_pairs = gather_analysis_pairs(analysis_gdf, query_features_dict)

        manager_queue = multiprocessing.Manager().Queue()

        all_processes = gather_processes(analysis_pairs, analysis_plan, manager_queue)

        t0 = time.time()

        results = gather_results(all_processes, manager_queue)

        t1 = time.time()

        analysis_gdf['geometry'].apply(shp.destroy_prepared)

        logger.info(
            json.dumps(
                {
                    'processes executed': len(all_processes),
                    'seconds': round(t1-t0, 2)
                }
            )
        )

        results = parse_analysis_errors(results)

        results.extend(results_no_analysis)
        
        attribute_dataframe = create_attribute_dataframe(results)

        fires_bufs_attrs_gdf = join_fires_bufs_attributes(analysis_gdf, attribute_dataframe)

        #ENDREGION

        #REGION OUTPUT
        fires_bufs_attrs_gdf = format_fields(fires_bufs_attrs_gdf, schema_plan)

        # logging warnings if any string value is approaching maximum allowed length
        # this could mean the value has already been truncated, or future truncation is probable
        # use-case is for json serialized field types with somewhat unpredictable content
        # helper functions in process.analysis 'pop' lower priority objects to shorten json structures that would cause applyEdits to fail if serialized in full
        str_cols = fires_bufs_attrs_gdf.select_dtypes(include=['string']).columns
        for col in str_cols:
            # it is fully expected _Nearest and _Interior fields will frequently be truncated, no reason to log warnings
            # the fields themselves contain information on how many features were popped and at what distance from (or within) the fire this popping began
            if '_Nearest' in col or '_Interior' in col:
                continue
            trimmed = fires_bufs_attrs_gdf[fires_bufs_attrs_gdf[col].str.len() > 4900][['wfigs_IncidentName', 'AnalysisBufferMiles', col]]
            for row in trimmed.itertuples(index=False):
                logger.warning(f'Value approaching max field length limit, data could be truncated. Incident: {row.wfigs_IncidentName}, Buffer Miles: {row.AnalysisBufferMiles}, Field: {col}.')

        feat_dict = create_output_feature_lists(fires_bufs_attrs_gdf)

        irwins_with_updates = fires_bufs_attrs_gdf[fires_bufs_attrs_gdf['AnalysisBufferMiles'] == 0]['wfigs_IrwinID'].to_list()

        all_edits_response, exception = asyncio.run(apply_edits_to_dof_var_service(
            akdof_var_service_url=r'https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/AK_Wildfire_Values_at_Risk/FeatureServer',
            token=token_dict['nifc'],
            irwins_with_updates=irwins_with_updates,
            feat_dict=feat_dict
        ))

        if exception:
            logger.error('Requester instance exited early with an exception!')
            logger.error(format_logged_exception(*exception))

        failures = find_apply_edits_failure(all_edits_response)

        if failures:
            logger.critical('applyEdits failure detected!')
            for fail in failures:
                logger.critical(json.dumps(fail))

        successes = find_apply_edits_success(all_edits_response)

        if successes:
            logger.info('applyEdits success detected.')
            for success in successes:
                logger.info(json.dumps(success))

        logger.info('PROCESS FINISHED')
    
        return
    
        #ENDREGION

    except Exception:
        tb = traceback.format_exc()
        subject = f'ak-wildfire-values-at-risk, main.py, general Exception.'
        send_email(subject, tb, sender, recipient, password)
        raise

if __name__ == "__main__":
    main()