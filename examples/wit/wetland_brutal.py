from datacube import Datacube
from datacube.virtual.impl import VirtualDatasetBox
from datacube.virtual import construct
from datacube.utils.geometry import CRS, Geometry
from datacube.utils.geometry.gbox import GeoboxTiles
from datacube_stats.utils.dates import date_sequence

from shapely.geometry import Polygon, MultiPolygon, mapping, box, shape
from pyproj import Proj, transform
import fiona
import fiona.crs
import yaml
import numpy as np
import xarray as xr
import csv

import sys
import os
import io
from os import path, walk
import logging
from datetime import datetime

import click
import copy
import pickle
import pandas as pd
import re
from zipfile import ZipFile

from mpi4py import MPI
from mpi4py.futures import MPIPoolExecutor
from wit_tooling.polygon_drill import cal_area
from wit_tooling.database.io import DIO
from wit_tooling import poly_wkt, convert_shape_to_polygon, query_wit_data, plot_to_png, query_wit_metrics, load_timeslice, generate_raster
from dea_tools import waterbodies

_LOG = logging.getLogger('wit_tool')
stdout_hdlr = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('[%(asctime)s.%(msecs)03d - %(levelname)s] %(message)s')
stdout_hdlr.setFormatter(formatter)
_LOG.addHandler(stdout_hdlr)
_LOG.setLevel(logging.DEBUG)
landsat_shp = '/g/data/u46/users/ea6141/aus_map/landsat_au.shp'
waterbody_str = 'water_body_polygons'

def db_insert_polygon(dio, poly_name, geometry, shapefile, feature_id):
    geometry = poly_wkt(geometry)
    poly_id, state = dio.insert_polygon(poly_name=poly_name, geometry=geometry, shapefile=shapefile, feature_id=feature_id)
    return poly_id, state

def db_last_update_time(dio, poly_list, reset=False):
    if not reset:
        time = dio.get_latest_time(poly_list)
    else:
        time = dio.get_min_time(poly_list)
    return np.datetime64(time)

def db_insert_catchment(dio, shape):
    geometry = shape['geometry']
    catchment_name = shape['properties']['RNAME']
    shapefile = shape['properties']['path']
    feature_id = shape['id']
    catchment_id = dio.insert_catchment(catchment_name=catchment_name,
            shapefile=shapefile, feature_id=feature_id, geometry=polygon_wkt(geometry))
    return catchment_id

def insert_catchment(catchment_shape):
    dio = DIO.get()
    with fiona.open(catchment_shape) as allshapes:
        crs = allshapes.crs_wkt
        for shape in allshapes:
            db_insert_catchment(dio,shape)
        return

def filter_store_result(args):
    poly_id, time, ready, result = args
    if not isinstance(time, str):
        time = time.astype('datetime64[us]').astype('str')
    dio = DIO.get()
    item_id, state = dio.insert_update_result(poly_id, ready, time, *(result.astype('float')))

def query_store_polygons(args):
    shape, crs, shapefile = args
    dio = DIO.get()
    shape_id = int(shape['id'])
    poly_name = get_polyName(shape)

    poly_id, state = db_insert_polygon(dio, poly_name, shape['geometry'], shapefile, shape_id)
    if not state:
        return (shape['geometry'], poly_id)
    else:
        return (None, -1)

def aggregate_data(fc_results, water_results):
    j = 1
    tmp = {}
    for var in fc_results.data_vars:
        tmp[var] = fc_results[var][0].copy()
    tmp_water = water_results[0].copy()
    while j < fc_results.time.size:
        valid_water = np.logical_or(tmp['TCW'] != fc_results['TCW'].attrs['nodata'], tmp_water)
        tmp_water = np.logical_or(tmp_water.where(valid_water, False),
                        water_results[j].where(~valid_water, False))
        for var in fc_results.data_vars:
            tmp[var] = (tmp[var].where(tmp[var] != fc_results[var].attrs['nodata'], 0)
                        + fc_results[var][j].where(tmp[var] == fc_results[var].attrs['nodata'], 0))
            tmp[var].attrs = fc_results[var].attrs
        j += 1
    tmp = xr.merge(tmp.values())
    tmp.attrs = fc_results.attrs
    tmp_water.attrs = water_results.attrs

    return tmp, tmp_water

def aggregate_over_timeslice(fc_product, grouped, i_start, aggregate, ready, poly_vessel, mask_array_dict):
    i_end = i_start + 1
    time = grouped.box.time.data[i_start]
    while i_end < grouped.box.time.size and (np.abs(time - grouped.box.time.data[i_end]).astype('timedelta64[D]')
                                                 < np.timedelta64(aggregate, 'D')):
        i_end += 1

    future_list = []
    future_time_list = []
    loaded = None
    merged = None
    max_geobox_size = 16500
    _LOG.debug("aggregate over %s", (time, grouped.box.time.data[i_end-1]))
    if sum(grouped.geobox.shape) > max_geobox_size * 2:
        split_shape = (max_geobox_size, max_geobox_size)
    else:
        split_shape = grouped.geobox.shape
    geobox_array = GeoboxTiles(grouped.geobox, split_shape)
    _LOG.debug("geobox_array shape %s", geobox_array.shape)

    with MPIPoolExecutor() as executor:
        for i in range(i_start, i_end):
            for j in range(geobox_array.shape[0]):
                for k in range(geobox_array.shape[1]):
                    to_split = VirtualDatasetBox(grouped.box.sel(time=grouped.box.time.data[i:i+1]), geobox_array[j, k],
                        grouped.load_natively, grouped.product_definitions, grouped.geopolygon)
                    future = executor.submit(load_timeslice, fc_product, to_split)
                    future_list.append(future)
            future_time_list.append(future_list)
            future_list = []

    for future_list in future_time_list:
        for future in future_list:
            if merged is None:
                merged = future.result()
            else:
                merged = xr.merge([merged, future.result()])
        _LOG.debug("finish loading time slice %s", merged.time.data[0])
        if loaded is None:
            loaded = merged
        else:
            loaded = xr.concat([loaded, merged], dim='time')
            fc_data, water_data = aggregate_data(loaded.drop('water'), loaded.water)
            loaded = xr.merge([fc_data, water_data.to_dataset()]).expand_dims('time')
        merged = None
    nodata = []
    for var in loaded.data_vars:
        nodata.append(loaded[var].attrs.get('nodata', 0))
    loaded = loaded.to_array()[:, 0, :, :]
    cal_result = []
    for key in poly_vessel.keys():
        perc, vfid_list = cal_area(loaded.data.astype('float32'), mask_array_dict[key].astype('int64'),
                        np.array(poly_vessel[key], dtype='int64'), np.array(nodata, dtype='float32'))
        cal_result.append((vfid_list, perc))
    with MPIPoolExecutor(max_workers=8) as executor:
        executor.map(filter_store_result, iter_args(time, ready, cal_result))
    return i_end

def get_polyName(feature):
    'function for QLD shapefile types'
    if feature.get('properties') is None:
        return "__"
    id_list = ['OBJECTID', 'Identifier']
    name_list = ['CATCHMENT', 'WetlandNam', 'Name']
    extra_name_list = ['HAB', 'Subwetland', 'SystemType']
    for it in id_list:
        ID = feature['properties'].get(it, '')
        if ID != '':
            break
    for it in name_list:
        CATCHMENT = feature['properties'].get(it, '')
        if CATCHMENT != '':
            break
    for it in extra_name_list:
        HAB = feature['properties'].get(it, '')
        if HAB != '':
            break
    polyName = f'{ID}_{CATCHMENT}_{HAB}'
    return(polyName)

def cal_timeslice(fc_product, to_split, mask_array_dict, poly_vessel, ready, nthreads):
    results = load_timeslice(fc_product, to_split)
    nodata = []
    for var in results.data_vars:
        nodata.append(results[var].attrs.get('nodata', 0))
    results = results.to_array()[:, 0, :, :]
    cal_result = []
    cal_start = datetime.now()
    for key in poly_vessel.keys():
        _LOG.debug("cal vessel %s", key)
        perc, vfid_list = cal_area(results.data.astype('float32'), mask_array_dict[key].astype('int64'),
                np.array(poly_vessel[key], dtype='int64'), np.array(nodata, dtype='float32'), nthreads)
        cal_result.append((vfid_list, perc))
    _LOG.debug("cal end %s", datetime.now() - cal_start)
    time = to_split.box.time.data[0]
    return time, cal_result

def iter_args(time, ready, cal_result):
    # cal_result = [(poly_list, perc),...]
    for ep in cal_result:
        for poly_id, result in zip(ep[0], ep[1]):
            if poly_id >= 0:
                yield((poly_id, time, ready, result))

def all_polygons(fc_product, grouped, poly_vessel, mask_array_dict, aggregate, time_chunk, reset=False):

    i = 0
    j = time_chunk

    # check last update time
    dio = DIO.get()
    fid_list = []
    for value in poly_vessel.values():
        fid_list += value
    time = db_last_update_time(dio, fid_list, reset)
    _LOG.debug("time from db %s", time)
    _LOG.debug("aggregate over %s", np.timedelta64(aggregate, 'D'))
    while i < grouped.box.time.size:
        if time >= grouped.box.time.data[i]:
            i += 1
        elif aggregate > 0 and (np.abs(time - grouped.box.time.data[i]).astype('timedelta64[D]')
                                                 < np.timedelta64(aggregate, 'D')):
            i += 1
        else:
            break
    ready = False
    nthreads = int(os.environ.get('OMP_NUM_THREADS', 8))//min(8, time_chunk)
    nthreads = max(nthreads, 1)
    while i < grouped.box.time.size:
        if aggregate > 0:
        # aggregate over time
            i = aggregate_over_timeslice(fc_product, grouped, i, aggregate, ready, poly_vessel, mask_array_dict)
            continue

        future_list = []
        # load data for j time slice
        with MPIPoolExecutor() as executor:
            for i_start in range(i, min(grouped.box.time.size, i+j), 1):
                to_split = VirtualDatasetBox(grouped.box.sel(time=grouped.box.time.data[i_start:i_start+1]), grouped.geobox,
                    grouped.load_natively, grouped.product_definitions, grouped.geopolygon)
                _LOG.debug("submit job for %s", to_split)
                future = executor.submit(cal_timeslice, fc_product, to_split, mask_array_dict, poly_vessel, ready, nthreads)
                future_list.append(future)

        for future in future_list:
            time, cal_result = future.result()
            insert_start = datetime.now()
            with MPIPoolExecutor(max_workers=8) as executor:
                executor.map(filter_store_result, iter_args(time, ready, cal_result))
            _LOG.debug("insert end %s", datetime.now() - insert_start)

        i += j

    # finished all the time slices, update result state to be ready
    time = grouped.box.time.data[-1]
    # this is bad, should be fixed later
    result = np.zeros(5)
    for poly_id in fid_list:
        filter_store_result((poly_id, time, True, result))

def get_intersect_polygons(poly_id, geometry, shapefile):
    dio = DIO.get()
    intersect_with = dio.get_intersect_polygons(poly_id, poly_wkt(geometry), shapefile)
    return ([geometry, poly_id], intersect_with)

def split_polygons(results, shapefile):
    initial_key = 1
    poly_vessel = dict({0:[]})
    shape_vessel = dict({0:[]})

    future_list = []
    with MPIPoolExecutor(max_workers=8) as executor:
        for re in results:
            if re[0] is not None:
                future = executor.submit(get_intersect_polygons, re[1], re[0], shapefile)
                future_list.append(future)

    for future in future_list:
        re, intersect_with = future.result()
        if intersect_with == ():
            shape_vessel[0].append(re)
            poly_vessel[0].append(re[1])
        else:
            j = 0
            while j < initial_key:
                append_match = False
                for sp in intersect_with:
                    if sp[0] in poly_vessel[j]:
                        append_match = True
                        break
                if append_match:
                    j += 1
                else:
                    break
            if j == initial_key:
                initial_key += 1
                shape_vessel[j] = []
                poly_vessel[j] = []
            shape_vessel[j].append(re)
            poly_vessel[j].append(re[1])
    return  shape_vessel, poly_vessel

def get_polygon_list(feature_list, shapefile, geo_hash=None):
    if shapefile == waterbody_str:
        crs = "epsg:3577"
    else:
        with fiona.open(shapefile) as allshapes:
            crs = allshapes.crs_wkt
    for shape in iter_shapes(feature_list, shapefile, geo_hash):
        yield((shape, crs, shapefile))

def shape_list(shapefile, geo_hash=None):
    if shapefile == waterbody_str:
        id_inc = 1
        with open(geo_hash, 'r') as f:
            hash_list = f.read().splitlines()
        for gh in hash_list:
            shape = convert_hash_to_shape(gh, id_inc)
            id_inc += 1
            yield shape
    else:
        with fiona.open(shapefile) as allshapes:
            for shape in allshapes:
                yield(shape)

def intersect_with_landsat(shape):
    contain = []
    intersect = []
    if shape.get('geometry') is None:
        return (shape['id'], contain, intersect)
    dio = DIO.get()
    results = dio.get_intersect_landsat_pathrow(poly_wkt(shape['geometry']))
    for re in results:
        if re[1] < 0.9:
            intersect.append(str(re[0]))
        else:
            contain.append(str(re[0]))
    return (shape['id'], contain, intersect)

def query_process(args):
    fc_product, query_poly, time = args
    dc = Datacube()
    query = {'geopolygon': query_poly, 'time': time}
    datasets = fc_product.query(dc, **query)
    grouped = fc_product.group(datasets, **query)
    _LOG.debug("query %s done", query['time'])
    return grouped

def convert_hash_to_shape(gh, id_inc):
    wb_poly = waterbodies.get_waterbody(geohash=gh).geometry.iloc[0]
    shape = dict({'geometry': mapping(wb_poly), 'id':str(id_inc),
        'properties':{'hash': gh}})
    return shape

def iter_shapes(t_file, shapefile, geo_hash=None):
    if shapefile == waterbody_str:
        with open(geo_hash, 'r') as f:
            hash_list = f.read().splitlines()
    else:
        shapes = shape_list(shapefile)
        shape = next(shapes)

    with open(t_file, 'r') as f:
        for line in f:
            shape_id = line.rstrip()
            if shapefile == waterbody_str:
                gh = hash_list[int(shape_id) - 1]
                shape = convert_hash_to_shape(gh, int(shape_id))
            else:
                while int(shape_id) != int(shape['id']):
                    shape = next(shapes)
            yield shape

def union_shapes(shape_list):
    query_poly = None
    for shape in shape_list:
        if query_poly is None:
            query_poly = box(*shape.bounds)
        else:
            query_poly = query_poly.union(box(*shape.bounds))
    return query_poly

def iter_queries(fc_product, query_poly, start_date, end_date):
    query_date = date_sequence(start=pd.to_datetime(start_date),
                                end=pd.to_datetime(end_date),
                                stats_duration='1y',
                                step_size='1y')
    query_date = list(query_date)
    for time in query_date:
        yield(fc_product, query_poly, time)

def normalize_name(name_str, str_length):
    replace_special_characters = ["/"]
    delete_special_characters = ["\"", "\'"]
    tmp = name_str
    for sc in replace_special_characters:
        if sc in name_str:
            tmp = tmp.replace(sc, "_")
    for sc in delete_special_characters:
        if sc in name_str:
            tmp = tmp.replace(sc, "")
    tmp = tmp[:str_length] if (len(tmp) > str_length) else tmp
    return tmp

def generate_file_name(shape, output_name):
    name_length = 127
    if len(output_name) == 0:
        file_name = shape['id']
    else:
        file_name = []
        str_length = (name_length - len(output_name) + 1) // len(output_name)
        for oe in output_name:
            tmp = str(shape['properties'].get(oe, shape['id']))
            tmp = normalize_name(tmp, str_length)
            file_name.append(tmp)
        file_name = '_'.join(file_name)
    return file_name

shapefile_path = click.argument('shapefile', type=str, default="")
product_definition = click.option('--product-yaml', type=str, help='yaml file of virtual product recipe',
        default='/g/data1a/u46/users/ea6141/wlinsight/fc_pd.yaml')

@click.group(help=__doc__)
def main():
    pass

@main.command(name='wit-output-metrics', help='Check the sainity of the shape file')
@shapefile_path
@click.option('--output', type=str, help='New shape file to save metrics', default='tmp.shp')
def wit_output_metrics(shapefile, output):
    metrics_properties = {'PV_TPC': 'float:5.3', 'Water_TPC': 'float:5.3', 'Wet_TPC': 'float:5.3',
            'PV_FOT': 'str:32', 'Water_FOT': 'str:32', 'Wet_FOT': 'str:32'}
    with fiona.open(shapefile, 'r') as s:
        crs = s.crs
        driver = s.driver
        schema = s.schema
    schema['properties'].update(metrics_properties)
    with fiona.open(output, 'w', crs=crs, driver=driver, schema=schema) as d:
        for shape in shape_list(shapefile):
            metrics = query_wit_metrics(shape)
            if metrics == []:
                continue
            metrics = list(metrics[0])
            for i in range(4, 7):
                metrics[i] = metrics[i].isoformat()
            properties = shape['properties']
            for key, val in zip(metrics_properties.keys(), metrics[1:]):
                properties.update({key: val})
            d.write({'geometry': shape['geometry'], 'properties': properties})
            break

@main.command(name='wit-check', help='Check the sainity of the shape file')
@shapefile_path
def wit_check(shapefile):
    check_pass = True
    with fiona.open(shapefile, 'r') as s:
        if '3577' not in s.crs_wkt:
            _LOG.warning("%s not comply", s.crs_wkt)
            check_pass = False
    for shape in shape_list(shapefile):
        if shape.get('geometry') is None:
            _LOG.warning("shape %s geometry %s not comply", shape['id'], shape['geometry'])
            check_pass = False
            break
    if check_pass:
        _LOG.info("All checks pass")
    else:
        _LOG.info("Fix the shape file %s", shapefile)

@main.command(name='wit-dump', help='Dump csv to database')
@shapefile_path
@click.option('--input-folder', type=str, help='Location to load the wit results', default='./results')
@click.option('--input-name','-n', type=str, help='A property from shape file used to get the data file name, default feature_id',
        multiple=True, default=None)
@click.option('--feature',  type=int, help='An individual polygon to dump', default=None)

def wit_dump(shapefile, input_folder, input_name, feature):
    with fiona.open(shapefile) as allshapes:
        crs = allshapes.crs_wkt
    for shape in shape_list(shapefile):
        if feature is not None:
            if int(shape['id']) != feature:
                continue
        _, poly_id = query_store_polygons((shape, crs, shapefile))
        _LOG.debug("dump result for %s", poly_id)
        if poly_id == -1:
            _LOG.info("result up-to-date for %s", poly_id)
            continue
        file_name = generate_file_name(shape, input_name)
        with open('/'.join([input_folder, file_name+'.csv']), newline='') as f:
            csv_reader = csv.reader(f, delimiter=',')
            for row in csv_reader:
                if row[0].upper() == 'TIME':
                    continue
                filter_store_result((poly_id, row[0], False, np.array(row[1:], dtype='float')))
            filter_store_result((poly_id, row[0], True, np.array(row[1:], dtype='float')))

@main.command(name='wit-plot', help='Plot png and dump csv from database')
@shapefile_path
@click.option('--geo-hash', '-g', type=str, help='File of a list of Geohash of water body polygons', default=None)
@click.option('--output-location', type=str, help='Location to save the query results', default='./results')
@click.option('--output-name','-n', type=str, help='A property from shape file used to populate file name, default feature_id',
        multiple=True, default=None)
@click.option('--feature',  type=int, help='An individual polygon to plot', default=None)
@click.option('--zip-file', '-z',  type=str, help='Zip the output files into the given file name', default=None)

def wit_plot(shapefile, geo_hash, output_location, output_name, feature, zip_file):
    if not path.exists(output_location):
        os.makedirs(output_location)

    if geo_hash is not None and shapefile == "":
        shapefile = waterbody_str
    for shape in shape_list(shapefile, geo_hash):
        if feature is not None:
            if int(shape['id']) != feature:
                continue

        _LOG.debug("shape id %s", shape['id'])
        poly_name, count = query_wit_data(shape)
        _LOG.debug("data size %s", count.size)
        if count.size == 0:
            continue
        file_name = generate_file_name(shape, output_name)
        poly_name = file_name
        _LOG.debug("shape name %s", poly_name)

        tmp_csv_file = '/'.join([output_location, file_name+'.csv'])
        tmp_png_file = '/'.join([output_location, file_name+'.png'])

        b_image = plot_to_png(count, poly_name)
        csv_buf = io.StringIO()
        pd.DataFrame(data=count, columns=['TIME', 'BS', 'NPV', 'PV', 'WET', 'WATER']).to_csv(csv_buf, index=False)
        csv_buf.seek(0)
        if zip_file is None:
            with open(tmp_csv_file, 'w') as f:
                f.write(csv_buf.read())
            with open(tmp_png_file, 'wb') as f:
                f.write(b_image.read())
        else:
            with ZipFile('/'.join([output_location, zip_file+'.zip']), 'a') as o_zip:
                o_zip.writestr(tmp_csv_file, csv_buf.read())
                o_zip.writestr(tmp_png_file, b_image.read())
        if feature is not None:
            break

@main.command(name='wit-query', help='Query datasets by path/row')
@shapefile_path
@click.option('--geo-hash', '-g', type=str, help='File of a list of Geohash of water body polygons', default=None)
@click.option('--input-folder', type=str, default='./')
@click.option('--start-date',  type=str, help='Start date, default=1987-01-01', default='1987-01-01')
@click.option('--end-date',  type=str, help='End date, default=2020-01-01', default='2021-01-01')
@click.option('--output-location',  type=str, help='Location to save the query results', default='./query_results')
@product_definition

def wit_query(shapefile, geo_hash, input_folder, start_date, end_date, output_location, product_yaml):
    tile_files = []

    if not path.exists(output_location):
        os.makedirs(output_location)

    with open(product_yaml, 'r') as f:
        recipe = yaml.safe_load(f)
    fc_product = construct(**recipe)

    for (_, _, filenames) in walk(input_folder):
        tile_files.extend(filenames)

    if geo_hash is not None and shapefile == "":
        shapefile = waterbody_str
        crs = "epsg:3577"
    else:
        with fiona.open(shapefile) as allshapes:
            crs = allshapes.crs_wkt

    for t_file in tile_files:
        tile_id = re.findall(r"\d+", t_file)
        _LOG.debug("query %s", '_'.join(tile_id))
        if path.exists(output_location+'/'+'_'.join(tile_id)+'.pkl'):
            continue
        future_list = []
        query_poly = []
        poly_list = iter_shapes('/'.join([input_folder, t_file]), shapefile, geo_hash)
        with MPIPoolExecutor() as executor:
            while True:
                if len(query_poly) == 1:
                    break
                if future_list != []:
                    poly_list = future_list
                    future_list = []
                    query_poly = []
                for poly in poly_list:
                    if isinstance(poly, dict):
                        query_poly.append(convert_shape_to_polygon(poly['geometry']))
                    else:
                        query_poly.append(poly.result())
                    if len(query_poly) == 10:
                        future = executor.submit(union_shapes, query_poly)
                        future_list.append(future)
                        query_poly = []
                if (len(future_list) > 0 and len(query_poly) == 1) or len(query_poly) > 1:
                    future = executor.submit(union_shapes, query_poly)
                    future_list.append(future)
                    query_poly = []
                _LOG.debug("len future list %s", len(future_list))

        query_poly = query_poly[0]
        query_poly = Geometry(mapping(box(*query_poly.bounds)), CRS(crs))
        query_list = iter_queries(fc_product, query_poly, start_date, end_date)
        with MPIPoolExecutor(max_workers=8) as executor:
            results = executor.map(query_process, query_list)

        data_box = None
        for grouped in results:
            if data_box is None:
                data_box = grouped.box
            else:
                data_box = xr.concat([data_box, grouped.box], dim='time')

        datasets = VirtualDatasetBox(data_box.sortby('time'), grouped.geobox,
                grouped.load_natively, grouped.product_definitions, grouped.geopolygon)

        _LOG.debug("query done %s", datasets)
        if datasets.box.data.size > 0:
            with open(output_location + '/' + '_'.join(tile_id) + '.pkl', 'wb') as f:
                pickle.dump(datasets,f)
    return

@main.command(name='wit-pathrow', help='Sort polygons into path/row')
@shapefile_path
@click.option('--geo-hash', '-g', type=str, help='File of a list of Geohash of water body polygons', default=None)
@click.option('--output-location', type=str, help='Location for output data files', default='./')
def match_pathrow(shapefile,  geo_hash, output_location):
    if not path.exists(output_location):
        os.makedirs(output_location)
    if geo_hash is not None and shapefile == "":
        shapefile = waterbody_str
        with open(geo_hash, 'r') as f:
            hash_list = f.read().splitlines()

    future_list = []
    total_poly = 0

    with MPIPoolExecutor(max_worker=8) as executor:
        for shape in shape_list(shapefile, geo_hash):
            future = executor.submit(intersect_with_landsat, shape)
            future_list.append(future)

    for future in future_list:
        pl_name, contain, intersect = future.result()
        total_poly += 1
        if contain != []:
            key = contain[0]
            with open(output_location + '/contain_' + key + '.txt', 'a') as f:
                f.write(pl_name+'\n')
        elif intersect != []:
            key = '_'.join(intersect)
            with open(output_location + '/intersect_' + key + '.txt', 'a') as f:
                    f.write(pl_name+'\n')

    _LOG.info("total poly %s", total_poly)
    return

@main.command(name='wit-cal', help='Compute area percentage of polygons')
@shapefile_path
@click.option('--geo-hash', '-g', type=str, help='File of a list of Geohash of water body polygons', default=None)
@click.option('--time-chunk',  type=int, help='Time slices to load in one go', default=8)
@click.option('--feature-list',  type=str, help='A file of features ID', default=None)
@click.option('--datasets',  type=str, help='Pickled datasets', default=None)
@click.option('--aggregate', type=int, help='If the polygon requires aggregation over path/row', default=0)
@click.option('--reset', type=bool, help='Reset the time to be 1987-01-01. Cautious: it will delete the results in database.', default=False)
@product_definition
def wit_cal(shapefile, geo_hash, time_chunk, feature_list, datasets, aggregate, reset, product_yaml):

    if feature_list is None:
        _LOG.error("feature list can't be none")
        sys.exit(0)
    elif not path.exists(feature_list):
        _LOG.error("can't find feature list at %s", feature_list)
        sys.exit(0)

    if datasets is None:
        _LOG.error("datasets can't be None")
        sys.exit(0)
    elif not path.exists(datasets):
        _LOG.error("can't find datasets at %s", datasets)
        sys.exit(0)

    with open(product_yaml, 'r') as f:
        recipe = yaml.safe_load(f)
    fc_product = construct(**recipe)

    if geo_hash is not None and shapefile == "":
        shapefile = waterbody_str

    poly_list =  get_polygon_list(feature_list, shapefile, geo_hash)

    time_start_insert = datetime.now()
    with MPIPoolExecutor(max_workers=8) as executor:
        result = executor.map(query_store_polygons, poly_list)

    shape_vessel, poly_vessel = split_polygons(result, shapefile)

    _LOG.debug("gone over all the polygons in %s", datetime.now() - time_start_insert)
    for j in range(len(shape_vessel)):
        _LOG.debug("number of polygons %s in vessel %s", len(poly_vessel[j]), j)

    if poly_vessel[0] == []:
        _LOG.info("all done")
        sys.exit(0)

    with open(datasets, 'rb') as f:
        grouped = pickle.load(f)
    _LOG.debug("grouped datasets %s", grouped)

    mask_array = {}
    future_list = []
    with MPIPoolExecutor() as executor:
        for key in shape_vessel.keys():
            future = executor.submit(generate_raster, shape_vessel[key], grouped.geobox)
            future_list.append(future)
    for key, future in zip(shape_vessel.keys(), future_list):
        mask_array[key] = future.result()
    all_polygons(fc_product, grouped, poly_vessel, mask_array, aggregate, time_chunk, reset)
    _LOG.info("all done")
    sys.exit(0)


if __name__ == '__main__':
    main()
