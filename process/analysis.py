# many functions in this module still need type hints and doc strings

from copy import deepcopy
import geopandas as gpd
import itertools
import json
import math
import multiprocessing
import pandas as pd
import pathlib
from pyproj import CRS, Transformer
import queue
import shapely as shp
import sys

proj_root = pathlib.Path(__file__).parent.parent
sys.path.append(proj_root)

from utils.project import batch_write_attr_tups, write_analysis_types_dict
from utils.general import basic_file_logger, format_logged_exception

def gather_analysis_pairs(analysis_gdf: gpd.GeoDataFrame, query_features_dict: dict) -> list[tuple]:
    '''
    Pairs together GDF of all features for a specific fire with GDF of an analysis input for that fire.
    On return these GDFs are accompanied by the alias identifying the source of the analysis input GDF.

    Args:
        * analysis_gdf (gpd.GeoDataFrame) -- Contains all fire features used in values-at-risk analysis.
        * query_features_dict (dict) -- holds { IrwinID : [(var_alias, geodataframe), ... } pairs.

    Returns:
        * list[tuple] -- holds tuples for all unique combinations of IrwinID and analysis input.
            * tuple structure: ( GDF of all features for a specific fire , GDF of an analysis input  , alias for the analysis input )
    '''    
    analysis_gdf_var_gdf_pairs = []
    for identifier, gdf_lst in query_features_dict.items():
        for alias_gdf_tup in gdf_lst:
            alias, var_gdf = alias_gdf_tup
            fire_buf_gdf = analysis_gdf[analysis_gdf['IrwinID'] == identifier]
            analysis_gdf_var_gdf_pairs.append((fire_buf_gdf, var_gdf, alias))

    return analysis_gdf_var_gdf_pairs

def gather_processes(analysis_gdf_var_gdf_pairs: list[tuple], analysis_plan: pd.DataFrame, manager_queue: multiprocessing.Queue) -> list[multiprocessing.Process]:
    '''
    Prepare all analysis processes to be run concurrently.

    Args:
        analysis_gdf_var_gdf_pairs (list[tuple])
        analysis_plan (pd.DataFrame)
        manager_queue (multiprocessing.Queue)

    Returns:
        list[multiprocessing.Process]
    '''

    sub_processes = [_process_gdf_pair(tup, analysis_plan, manager_queue) for tup in analysis_gdf_var_gdf_pairs]

    all_processes = list(itertools.chain(*sub_processes))

    return all_processes

def gather_results(processes: list[multiprocessing.Process], manager_queue: multiprocessing.Queue, batch_size: int = multiprocessing.cpu_count()) -> list[list[tuple]]:
    '''
    Execute all processes in batches that are sized according to system cpu count.

    Args:
        processes (list[multiprocessing.Process])
        manager_queue (multiprocessing.Queue)
        batch_size (int, optional): Maximum number of processes in each batch. Defaults to multiprocessing.cpu_count().

    Returns:
        list[list[tuple]]: Deepest elements are attribution tuples formatted (IrwinID, buf_dist, attrName, attrVal).
    '''

    process_results = []

    # batch_size (system cpu count by default) determines number of running processes that live between p.start() and p.join()
    for i in range(0, len(processes), batch_size):

        batch = processes[i:i+batch_size]

        for p in batch:
            p.start()

        # to avoid deadlock scenario, must ensure all items put on manager_queue will be removed before the process is joined
        while True:
            try:
                result = manager_queue.get(block=False)
                process_results.append(result)
            except queue.Empty:
                pass
            all_exited = True
            for p in batch:
                if p.exitcode is None:
                    all_exited = False
                    break
            if all_exited and manager_queue.empty():
                break

        for p in batch:
            p.join()

    return process_results

def parse_analysis_errors(results: list[list[tuple]]) -> list[list[tuple]]:
    '''
    Identifies any exceptions that occurred during multiprocessing,
    logs exception details,
    and modifies the attribution tuple containing an exception to instead contain a string identifier for analysis errors.

    Args:
        results (list[list[tuple]]): Deepest elements are attribution tuples formatted (IrwinID, buf_dist, attrName, attrVal).

    Returns:
        list[list[tuple]]: Deepest elements are attribution tuples formatted (IrwinID, buf_dist, attrName, attrVal).
    ''' 

    logger = basic_file_logger('main_info.log')

    for attr_tups in results:
        for idx, tup in enumerate(attr_tups):
            if isinstance(tup[3], tuple) and issubclass(tup[3][0], Exception):
                new_tup = tup[:3] + ('!ANALYSISERROR!',)
                logger.error(f'{new_tup}')
                logger.error(tup[3][1])
                attr_tups[idx] = new_tup

    return results

def create_attribute_dataframe(results: list[list[tuple]]) -> pd.DataFrame:
    """
    - Convert attribution tuples generated during multiprocessing into a DataFrame.

    Args:
        - results (list[list[tuple]]) -- Object structure returned by gather_results(). Deepest elements are attribution tuples formatted (IrwinID, buf_dist, attrName, attrVal).

    Returns:
        - pd.DataFrame -- DataFrame with buf_dist & IrwinID multi-index, and columns for each unique attrName.
    """
    # flatten the attribute lists into a dataframe
    attributes_dataframe = pd.DataFrame(itertools.chain(*results), columns=['IrwinID', 'buf_dist', 'attrName', 'attrVal'])

    # assign buf_dist + IrwinID index (will be used for join)
    # create attrName column
    # pivot attrVal so values are written to the appropriate attrName column
    attributes_dataframe_pivot = pd.pivot(
        data=attributes_dataframe,
        columns='attrName',
        index=['buf_dist', 'IrwinID'],
        values='attrVal'
        )
    
    return attributes_dataframe_pivot

def join_fires_bufs_attributes(analysis_gdf: gpd.GeoDataFrame, attributes_df: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Join analysis attributions to input GeoDataFrame containing all fire features and their buffer features.

    Args:
        analysis_gdf (gpd.GeoDataFrame): Contains all fire features used in values-at-risk analysis.
        attributes_df (pd.DataFrame): Contains all attributes generated by values-at-risk analysis.

    Returns:
        gpd.GeoDataFrame: GeoDataFrame containing all fire features and their buffer features, attributed with all analysis outputs.
    """
    analysis_gdf = analysis_gdf.join(attributes_df, validate='1:1')
    analysis_gdf.reset_index(drop=True, inplace=True)
    return analysis_gdf

def _process_gdf_pair(analysis_pair: tuple, analysis_plan: pd.DataFrame, manager_queue: multiprocessing.Queue) -> tuple:

    fire_buf_gdf, var_gdf, var_alias = analysis_pair

    #* using heuristic that any gdf constructed from querying a single arcgis rest api endpoint can only have one general geometry type
    var_geom_type = var_gdf.loc[0,'geometry'].geom_type.casefold()
    poly_var = 'polygon' in var_geom_type
    line_var = 'line' in var_geom_type
    point_var = 'point' in var_geom_type

    analysis_types = write_analysis_types_dict(analysis_plan, var_alias)
    
    processes = []

    if 'NEAREST_FEATS_FIELDS' in analysis_types:
        process_args = [
            deepcopy(obj) for obj in (
                fire_buf_gdf[fire_buf_gdf['buf_dist'] == 0].iloc[0]['IrwinID'],
                fire_buf_gdf[fire_buf_gdf['buf_dist'] == 0].iloc[0]['geometry'],
                # for consistency sake, only run _nearest_feats_analysis() on the 5 mile buffer geometry (not full bbox query result)
                gpd.clip(var_gdf, fire_buf_gdf[fire_buf_gdf['buf_dist'] == 5].iloc[0]['geometry']),
                var_alias,
                analysis_types['NEAREST_FEATS_FIELDS']
            )
        ]
        process_args.append(manager_queue)
        p = multiprocessing.Process(
            target=_nearest_feats_analysis,
            args=process_args
        )
        processes.append(p)

    #! deep copy objects - watch out for memory usage. fire_buf_gdf will have 4 rows, so 4 copies of all objects will be made.
    if poly_var:
        for _, row in fire_buf_gdf.iterrows():
            process_args = [deepcopy(obj) for obj in (row['IrwinID'], row['geometry'], row['buf_dist'], var_gdf, var_alias, analysis_types)]
            process_args.append(manager_queue)
            p = multiprocessing.Process(
                target=_analyze_poly_var,
                args=process_args
            )
            processes.append(p)

    elif line_var:
        for _, row in fire_buf_gdf.iterrows():
            process_args = [deepcopy(obj) for obj in (row['IrwinID'], row['geometry'], row['buf_dist'], var_gdf, var_alias, analysis_types)]
            process_args.append(manager_queue)
            p = multiprocessing.Process(
                target=_analyze_line_var,
                args=process_args
            )
            processes.append(p)

    elif point_var:
        for _, row in fire_buf_gdf.iterrows():
            process_args = [deepcopy(obj) for obj in (row['IrwinID'], row['geometry'], row['buf_dist'], var_gdf, var_alias, analysis_types)]
            process_args.append(manager_queue)
            p = multiprocessing.Process(
                target=_analyze_point_var,
                args=process_args
            )
            processes.append(p)
    else:
        raise ValueError(f'Unrecognized geometry type in {var_gdf['geometry'].geom_type.unique()}')

    return processes

def _preprocess_poly_var_gdf(var_gdf: gpd.GeoDataFrame, fire_geometry: shp.Polygon | shp.MultiPolygon):

    var_gdf['intersect_geom'] = var_gdf['geometry'].clip(fire_geometry)

    var_gdf = var_gdf[var_gdf['intersect_geom'].notna()].copy() #* copy() because pandas SettingwithCopyWarning

    var_gdf['fire_intersect_ratio'] = (var_gdf['intersect_geom'].area / var_gdf['geometry'].area)

    var_gdf['geometry'] = var_gdf['intersect_geom']

    var_gdf['geometry_acres'] = var_gdf['geometry'].area / 4046.86

    var_gdf = var_gdf.drop(columns=['intersect_geom'])

    return var_gdf

def _preprocess_line_var_gdf(var_gdf: gpd.GeoDataFrame, fire_geometry: shp.Polygon | shp.MultiPolygon):

    var_gdf['intersect_geom'] = var_gdf['geometry'].clip(fire_geometry)

    var_gdf = var_gdf[var_gdf['intersect_geom'].notna()].copy() #* copy() because pandas SettingwithCopyWarning

    var_gdf['fire_intersect_ratio'] = (var_gdf['intersect_geom'].length / var_gdf['geometry'].length)

    var_gdf['geometry'] = var_gdf['intersect_geom']

    var_gdf['geometry_feet'] = var_gdf['geometry'].length * 3.281

    var_gdf = var_gdf.drop(columns=['intersect_geom'])

    return var_gdf

def _run_acres_sum_by_attr(var_gdf, field):
    acres_sum_attrs = {}
    groups = var_gdf.groupby(field, dropna=False)
    for g in groups:
        grouped_value = g[0]
        grouped_gdf = g[1]
        if pd.isna(grouped_value) or grouped_value.strip() == "":
            grouped_value = 'No Data'
        area_acres = grouped_gdf['geometry_acres'].sum()
        acres_sum_attrs[grouped_value] = round(area_acres, 2)
    return acres_sum_attrs

def _run_length_ft_sum_by_attr(var_gdf, field):
    length_ft_sum_attrs = {}
    groups = var_gdf.groupby(field, dropna=False)
    for g in groups:
        grouped_value = g[0]
        grouped_gdf = g[1]
        if pd.isna(grouped_value) or grouped_value.strip() == "":
            grouped_value = 'No Data'
        length_ft = grouped_gdf['geometry_feet'].sum()
        length_ft_sum_attrs[grouped_value] = int(length_ft)
    return length_ft_sum_attrs

def _run_value_sum(var_gdf, field):

    var_gdf.loc[:, field] = pd.to_numeric(var_gdf[field], errors='coerce')

    if 'fire_intersect_ratio' in var_gdf.columns:
        value_series = var_gdf['fire_intersect_ratio'] * var_gdf[field]
        value_sum = int(value_series.sum())
    else:
        value_sum = int(var_gdf[field].sum())

    return value_sum

def _get_cardinal_direction(point_a, point_b):
    '''
    Calculates the 16-point cardinal direction between two Shapely points.

    Args:
        - point_a (shapely.geometry.Point): The starting point.
        - point_b (shapely.geometry.Point): The ending point.

    Returns:
        - str: The cardinal direction on a 16-point compass.
    '''
    dx = point_b.x - point_a.x
    dy = point_b.y - point_a.y

    # describe direction as "None" if point coordinates are identical
    if dx == 0 and dy == 0:
        return 'None'

    angle = math.degrees(math.atan2(dy, dx))
    compass_angle = (90 - angle) % 360

    directions = [
        'N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE',
        'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW'
    ]

    index = int((compass_angle + 11.25) / 22.5) % 16

    return directions[index]

def _dd_to_ddm_lat(coord):
    degree_sign = u'\N{DEGREE SIGN}'
    deg = abs(int(coord))
    min = (abs(coord) - deg) * 60
    if coord > 0:
        dir = "N"
    else:
        dir = "S"
    return "%s%s %s' %s"%(deg, degree_sign, "{:06.3f}".format(min), dir)

def _dd_to_ddm_lng(coord):
    degree_sign = u'\N{DEGREE SIGN}' 
    deg = abs(int(coord))
    min = (abs(coord) - deg) * 60
    if coord > 0:
        dir = "E"
    else:
        dir = "W"
    return "%s%s %s' %s"%(deg, degree_sign, "{:06.3f}".format(min), dir)

def _get_lat_lng_ddm_from_3338_point(point_3338: shp.Point) -> tuple[str]:

    crs_3338 = CRS("EPSG:3338")
    crs_4326 = CRS("EPSG:4326")

    transformer = Transformer.from_crs(crs_3338, crs_4326)

    x_4326, y_4326 = transformer.transform(point_3338.x, point_3338.y)

    point_4326 = shp.Point(x_4326, y_4326)

    ddm_lat = _dd_to_ddm_lat(point_4326.x)

    ddm_lng = _dd_to_ddm_lng(point_4326.y)

    return (ddm_lat, ddm_lng)

def _nearest_feats_analysis(
    identifier: str,
    fire_geom: shp.Polygon | shp.MultiPolygon,
    var_gdf: gpd.GeoDataFrame,
    var_alias: str,
    included_fields: tuple,
    manager_queue: multiprocessing.Queue
    ) -> None:
    '''
    Determines distance and direction from fires edge to nearest point for a value-at-risk.
    Calculates lat & lng for nearest point of value-at-risk in DDM.
    Writes attributes specifying distance, direction, lat, lng, *value-at-risk attributes.
    Values-at-risk which intersect the fire are considered interior, their direction is described
    as interior and these features are written to a seperate attribution tuple from those beyond the fire edge.

    Arguments:
        * identifier -- GUID for a fire (IrwinID taken from WFIGS).
        * fire_geom -- Geometry for a fire.
        * var_gdf -- All features for a specific input that intersect the maximum size buffer created for the fire.
        * var_alias -- Identifies the value-at-risk input data source.
        * included_fields -- Fields unique to the input GDF to include with the output attributes.
        * manager_queue -- Holds all attribution tuples generated during multiprocessing analysis.
    '''       
    try:

        attr_tups = []

        # write null attribution if there are no features within max buffer analysis of the fire
        if len(var_gdf) < 1:
            attr_tups.extend([
                (identifier, 0, f'{var_alias}_nearest_feats', None),
                (identifier, 0, f'{var_alias}_interior_feats', None)
                ])
            manager_queue.put(attr_tups)
            return

        # features that intersect the fire polygon will be considered interior
        interior = var_gdf.intersects(fire_geom)

        # intermediary var_prox_df
        # will include index and columns required for assessing proximity
        var_prox_df = pd.DataFrame({'interior': interior})

        # determine nearest points for the value-at-risk and along fires edge
        var_prox_df[['var_nearest_pt','fire_nearest_pt']] = var_gdf['geometry'].apply(
            lambda x: pd.Series(shp.ops.nearest_points(x, fire_geom.boundary))
        )

        # determine distance in meters between nearest points
        var_prox_df['meters'] = var_prox_df.apply(
            lambda x: shp.distance(x['var_nearest_pt'], x['fire_nearest_pt']),
            axis=1
        )
        
        # handling interior features
        # truncating those furthest from the fires edge, saving baseline popped & cutoff variables
        interior_var_prox_df = var_prox_df[var_prox_df['interior'] == True].copy().sort_values(by='meters', ascending=True)
        interior_var_total_feats = len(interior_var_prox_df)
        # considering 50 features a safe upper limit for how many could possibly be serialized to 5000 characters or under
        interior_var_prox_df = interior_var_prox_df[:50]
        interior_var_popped = interior_var_total_feats - len(interior_var_prox_df)
        interior_var_cutoff = round((interior_var_prox_df.iloc[-1]['meters'] / 1609.34), 2) if interior_var_popped > 0 else None

        # handling nearest features
        # truncating those furthest from the fires edge, saving baseline popped & cutoff variables
        nearest_var_prox_df = var_prox_df[var_prox_df['interior'] != True].copy().sort_values(by='meters', ascending=True)
        nearest_var_total_feats = len(nearest_var_prox_df)
        # considering 50 features a safe upper limit for how many could possibly be serialized to 5000 characters or under
        nearest_var_prox_df = nearest_var_prox_df[:50]
        nearest_var_popped = nearest_var_total_feats - len(nearest_var_prox_df)
        nearest_var_cutoff = round((nearest_var_prox_df.iloc[-1]['meters'] / 1609.34), 2) if nearest_var_popped > 0 else None

        # fully attributed var_prox_df
        # to be used for analysis and for generating nearest features and interior features lists
        var_prox_df = pd.concat([interior_var_prox_df, nearest_var_prox_df], axis=0)
        var_prox_df = pd.merge(var_gdf, var_prox_df, left_index=True, right_index=True)

        # calculate distance in miles
        var_prox_df['dist_mi'] = var_prox_df['meters'] / 1609.34
        var_prox_df['dist_mi'] = var_prox_df['dist_mi'].round(2)

        # fill missing values (var_gdf can be unpredictably attributed)
        var_prox_df = var_prox_df.astype('object')
        var_prox_df.fillna(value='No Data', inplace=True)

        # get var coordinates in DDM
        var_prox_df[['lat','lng']] = var_prox_df['var_nearest_pt'].apply(
            lambda x: pd.Series(_get_lat_lng_ddm_from_3338_point(x))
        )

        # get cardinal direction between nearest points for non-interior var features
        var_prox_df.loc[var_prox_df['interior'] != True, 'dir'] = var_prox_df[var_prox_df['interior'] != True].apply(
            lambda x: _get_cardinal_direction(x['fire_nearest_pt'], x['var_nearest_pt']),
            axis=1
        )

        # describing the direction of features that intersect the fire as "Interior"
        var_prox_df.loc[var_prox_df['interior'] == True, 'dir'] = 'Interior'

        # organize output attributes
        # by placing 'dist_mi' at index 0, in the future json formatted attributes can easily be sorted by this key
        var_prox_df = var_prox_df[['dist_mi', 'dir', 'lat', 'lng', *included_fields]]

        # create _interior_feats attributes
        interior_feats_df = var_prox_df[var_prox_df['dir'] == 'Interior'].sort_values(by='dist_mi', ascending=True)
        interior_feats = [feat._asdict() for feat in interior_feats_df.itertuples(index=False)]
        if interior_feats:
            interior_fset = {
                'features': interior_feats,
                'popped': interior_var_popped,
                'cutoff': interior_var_cutoff
            }
            interior_fset_serialized = _trim_nearest_feats(interior_fset)
            attr_tups.append((identifier, 0, f'{var_alias}_interior_feats', interior_fset_serialized))
        else:
            attr_tups.append((identifier, 0, f'{var_alias}_interior_feats', None))

        # create _nearest_feats attributes
        nearest_feats_df = var_prox_df[var_prox_df['dir'] != 'Interior'].sort_values(by='dist_mi', ascending=True)
        nearest_feats = [feat._asdict() for feat in nearest_feats_df.itertuples(index=False)]
        if nearest_feats:
            nearest_fset = {
                'features': nearest_feats,
                'popped': nearest_var_popped,
                'cutoff': nearest_var_cutoff
            }
            nearest_fset_serialized = _trim_nearest_feats(nearest_fset)
            attr_tups.append((identifier, 0, f'{var_alias}_nearest_feats', nearest_fset_serialized))
        else:
            attr_tups.append((identifier, 0, f'{var_alias}_nearest_feats', None))

        manager_queue.put(attr_tups)

    except Exception as e:
        attr_tups = [
            (identifier, 0, f'{var_alias}_nearest_feats', (type(e), format_logged_exception(type(e), e, e.__traceback__))),
            (identifier, 0, f'{var_alias}_interior_feats', (type(e), format_logged_exception(type(e), e, e.__traceback__)))
        ]
        manager_queue.put(attr_tups)

def _trim_nearest_feats(nearest_feats_fset: dict) -> str:

    # return full serialized object right away if size is acceptable
    # otherwise continue to binary search logic
    full_serialized = json.dumps(nearest_feats_fset)
    if len(full_serialized) <= 5000:
        return full_serialized

    # going to search between 0 and len(features) to find 
    # the maximum number of features that can be serialized into the required string length
    features = nearest_feats_fset['features']
    low = 0
    high = len(features)

    # baseline values for popped and cutoff
    popped_base = nearest_feats_fset['popped']
    cutoff_base = nearest_feats_fset['cutoff']

    while low < high:

        # retreive subset of features up to the middle index between the current low and high values
        mid = (low + high) // 2
        test_subset = features[:mid]

        # produce test json structure based on feature subset
        test_dict = {
            'features': test_subset,
            'popped': (len(features) - mid) + popped_base,
            'cutoff': features[mid - 1]['dist_mi'] if mid < len(features) else cutoff_base
        }
        test_serialized = json.dumps(test_dict)
        
        # feature subset is an acceptable size, establishing the index of the last feature of the subset +1 as our new low value
        if len(test_serialized) <= 5000:
            best_serialized = test_serialized
            low = mid + 1

        # feature subset was too large, establishing the index of the last feature of the subset as our new high value
        else:
            high = mid

    return best_serialized

# this can be optimized using binary search logic from _trim_nearest_feats()
# however _sort_trim_attr_json() is very rarely passed attr_json that needs to be trimmed, and even more rarely attr_json that is well over 5000 chars serialized
def _sort_trim_attr_json(attr_json: dict, max_length: int = 5000) -> str:
    '''
    Sorts JSON formatted attribute by its values in descending order.
    Shortens JSON one k,v pair at a time while its serialized length exceeds specified max_length.
    '''
    attr_json_sorted = dict(sorted(attr_json.items(), key=lambda item: item[1], reverse=True))   

    attr_json_serialized = json.dumps(attr_json_sorted)

    while len(attr_json_serialized) > max_length:
        attr_json_sorted.popitem()
        attr_json_serialized = json.dumps(attr_json_sorted)

    return attr_json_serialized

def _analyze_poly_var(identifier, fire_geometry, buf_dist, var_gdf, var_alias, analysis_types, manager_queue):

    try:

        var_gdf = _preprocess_poly_var_gdf(var_gdf, fire_geometry)

        if len(var_gdf) < 1:
            attr_tups = batch_write_attr_tups(identifier, buf_dist, var_alias, analysis_types)
            manager_queue.put(attr_tups)
            return

        attr_tups = []

        if 'FEATURE_COUNT' in analysis_types:

            try:
                feat_count = len(var_gdf)
                attr_tups.append((identifier, buf_dist, f'{var_alias}_feat_count', feat_count))
            except Exception as e:
                attr_tups.append((identifier, buf_dist, f'{var_alias}_feat_count', (type(e), format_logged_exception(type(e), e, e.__traceback__))))

        if 'TOTAL_ACRES' in analysis_types:

            try:
                total_acres = round(var_gdf['geometry_acres'].sum(), 2)
                attr_tups.append((identifier, buf_dist, f'{var_alias}_total_acres', total_acres))
            except Exception as e:
                attr_tups.append((identifier, buf_dist, f'{var_alias}_total_acres', (type(e), format_logged_exception(type(e), e, e.__traceback__))))

        if 'ACRES_SUM_FIELDS' in analysis_types:

            for field in analysis_types['ACRES_SUM_FIELDS']:

                try:
                    acres_sum_by_attr = _run_acres_sum_by_attr(var_gdf, field)
                    acres_sum_by_attr_str = _sort_trim_attr_json(acres_sum_by_attr)
                    attr_tups.append((identifier, buf_dist, f'{var_alias}_{field}_acres_sum', acres_sum_by_attr_str))
                except Exception as e:
                    attr_tups.append((identifier, buf_dist, f'{var_alias}_{field}_acres_sum', (type(e), format_logged_exception(type(e), e, e.__traceback__))))


        if 'VALUE_SUM_FIELDS' in analysis_types:

            for field in analysis_types['VALUE_SUM_FIELDS']:

                try:
                    value_sum = _run_value_sum(var_gdf, field)
                    attr_tups.append((identifier, buf_dist, f'{var_alias}_{field}_value_sum', value_sum))
                except Exception as e:
                    attr_tups.append((identifier, buf_dist, f'{var_alias}_{field}_value_sum', (type(e), format_logged_exception(type(e), e, e.__traceback__))))


        if 'ATTR_COUNT_FIELDS' in analysis_types:

            for field in analysis_types['ATTR_COUNT_FIELDS']:

                try:
                    count_by_attr = var_gdf[field].fillna('No Data').value_counts(dropna=False).to_dict()
                    count_by_attr_str = _sort_trim_attr_json(count_by_attr)
                    attr_tups.append((identifier, buf_dist, f'{var_alias}_{field}_attr_count', count_by_attr_str))
                except Exception as e:
                    attr_tups.append((identifier, buf_dist, f'{var_alias}_{field}_attr_count', (type(e), format_logged_exception(type(e), e, e.__traceback__))))


        manager_queue.put(attr_tups)

    except Exception as e:
        attr_tups = batch_write_attr_tups(identifier, buf_dist, var_alias, analysis_types, (type(e), format_logged_exception(type(e), e, e.__traceback__)))
        manager_queue.put(attr_tups)

def _analyze_line_var(identifier, fire_geometry, buf_dist, var_gdf, var_alias, analysis_types, manager_queue):

    try:

        var_gdf = _preprocess_line_var_gdf(var_gdf, fire_geometry)

        if len(var_gdf) < 1:
            attr_tups = batch_write_attr_tups(identifier, buf_dist, var_alias, analysis_types)
            manager_queue.put(attr_tups)
            return

        attr_tups = []

        if 'TOTAL_LENGTH_FT' in analysis_types:

            try:
                total_feet = int(var_gdf['geometry_feet'].sum())
                attr_tups.append((identifier, buf_dist, f'{var_alias}_total_feet', total_feet))
            except Exception as e:
                attr_tups.append((identifier, buf_dist, f'{var_alias}_total_feet', (type(e), format_logged_exception(type(e), e, e.__traceback__))))

        if 'LENGTH_FT_SUM_FIELDS' in analysis_types:

            for field in analysis_types['LENGTH_FT_SUM_FIELDS']:

                try:
                    length_ft_sum_by_attr = _run_length_ft_sum_by_attr(var_gdf, field)
                    length_ft_sum_by_attr_str = _sort_trim_attr_json(length_ft_sum_by_attr)
                    attr_tups.append((identifier, buf_dist, f'{var_alias}_{field}_feet_sum', length_ft_sum_by_attr_str))
                except Exception as e:
                    attr_tups.append((identifier, buf_dist, f'{var_alias}_{field}_feet_sum', (type(e), format_logged_exception(type(e), e, e.__traceback__))))


        if 'VALUE_SUM_FIELDS' in analysis_types:

            for field in analysis_types['VALUE_SUM_FIELDS']:

                try:
                    value_sum = _run_value_sum(var_gdf, field)
                    attr_tups.append((identifier, buf_dist, f'{var_alias}_{field}_value_sum', value_sum))
                except Exception as e:
                    attr_tups.append((identifier, buf_dist, f'{var_alias}_{field}_value_sum', (type(e), format_logged_exception(type(e), e, e.__traceback__))))


        if 'ATTR_COUNT_FIELDS' in analysis_types:

            for field in analysis_types['ATTR_COUNT_FIELDS']:

                try:
                    count_by_attr = var_gdf[field].fillna('No Data').value_counts(dropna=False).to_dict()
                    count_by_attr_str = _sort_trim_attr_json(count_by_attr)
                    attr_tups.append((identifier, buf_dist, f'{var_alias}_{field}_attr_count', count_by_attr_str))                    
                except Exception as e:
                    attr_tups.append((identifier, buf_dist, f'{var_alias}_{field}_attr_count', (type(e), format_logged_exception(type(e), e, e.__traceback__))))

        manager_queue.put(attr_tups)
    
    except Exception as e:
        attr_tups = batch_write_attr_tups(identifier, buf_dist, var_alias, analysis_types, (type(e), format_logged_exception(type(e), e, e.__traceback__)))
        manager_queue.put(attr_tups)

def _analyze_point_var(identifier, fire_geometry, buf_dist, var_gdf, var_alias, analysis_types, manager_queue):

    try:

        var_gdf = gpd.clip(var_gdf, fire_geometry)

        if len(var_gdf) < 1:
            attr_tups = batch_write_attr_tups(identifier, buf_dist, var_alias, analysis_types)
            manager_queue.put(attr_tups)
            return

        attr_tups = []

        if 'FEATURE_COUNT' in analysis_types:

            try:
                feat_count = len(var_gdf)
                attr_tups.append((identifier, buf_dist, f'{var_alias}_feat_count', feat_count))
            except Exception as e:
                attr_tups.append((identifier, buf_dist, f'{var_alias}_feat_count', (type(e), format_logged_exception(type(e), e, e.__traceback__))))


        if 'VALUE_SUM_FIELDS' in analysis_types:

            for field in analysis_types['VALUE_SUM_FIELDS']:

                try:
                    value_sum = _run_value_sum(var_gdf, field)
                    attr_tups.append((identifier, buf_dist, f'{var_alias}_{field}_value_sum', value_sum))
                except Exception as e:
                    attr_tups.append((identifier, buf_dist, f'{var_alias}_{field}_value_sum', (type(e), format_logged_exception(type(e), e, e.__traceback__))))

        if 'ATTR_COUNT_FIELDS' in analysis_types:

            for field in analysis_types['ATTR_COUNT_FIELDS']:

                try:
                    count_by_attr = var_gdf[field].fillna('No Data').value_counts(dropna=False).to_dict()
                    count_by_attr_str = _sort_trim_attr_json(count_by_attr)
                    attr_tups.append((identifier, buf_dist, f'{var_alias}_{field}_attr_count', count_by_attr_str))                    
                except Exception as e:
                    attr_tups.append((identifier, buf_dist, f'{var_alias}_{field}_attr_count', (type(e), format_logged_exception(type(e), e, e.__traceback__))))


        manager_queue.put(attr_tups)

    except Exception as e:
        attr_tups = batch_write_attr_tups(identifier, buf_dist, var_alias, analysis_types, (type(e), format_logged_exception(type(e), e, e.__traceback__)))
        manager_queue.put(attr_tups)