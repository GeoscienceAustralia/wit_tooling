from datacube import Datacube
from datacube.virtual.impl import VirtualDatasetBox
from datacube.virtual import construct
from datacube.utils.geometry import CRS, Geometry
from datacube_stats.utils.dates import date_sequence
from datacube.virtual.transformations import MakeMask, ApplyMask 
from datacube_stats.external import MaskByValue

from shapely.geometry import Polygon, MultiPolygon, mapping, box, shape
from pyproj import Proj, transform
from rasterio import features
from rasterio.warp import calculate_default_transform
import fiona
import fiona.crs
import yaml
import numpy as np
import xarray as xr

import sys
import os
from os import path, walk
import logging
from datetime import datetime

import click
import copy
import pickle
import pandas as pd
import re

from mpi4py import MPI
from mpi4py.futures import MPIPoolExecutor 
from wit_tooling.polygon_drill import cal_area
from wit_tooling.database.io import DIO
from wit_tooling import poly_wkt
from wit_tooling import query_wit_data, plot_to_png


_LOG = logging.getLogger('wit_tool')
stdout_hdlr = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('[%(asctime)s.%(msecs)03d - %(levelname)s] %(message)s')
stdout_hdlr.setFormatter(formatter)
_LOG.addHandler(stdout_hdlr)
_LOG.setLevel(logging.DEBUG)
landsat_shp = '/g/data/u46/users/ea6141/aus_map/landsat_au.shp'

def db_insert_polygon(dio, poly_name, geometry, shapefile, feature_id):
    geometry = poly_wkt(geometry)
    poly_id, state = dio.insert_polygon(poly_name=poly_name, geometry=geometry, shapefile=shapefile, feature_id=feature_id)
    return poly_id, state

def db_last_update_time(dio, poly_list):
    time = dio.get_latest_time(poly_list)
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

def generate_raster(shapes, geobox):
    yt, xt = geobox.shape
    transform, width, height = calculate_default_transform(
        geobox.crs, geobox.crs.crs_str, yt, xt, *geobox.extent.boundingbox)
    target_ds = features.rasterize(shapes,
        (yt, xt), fill=-1, transform=transform, all_touched=True)
    return target_ds

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

def aggregate_over_timeslice(fc_product, grouped, i_start, ready, fid_list, mask_array):
    i_end = i_start + 1
    time = grouped.box.time.data[i_start]
    while i_end < grouped.box.time.size and (np.abs(time - grouped.box.time.data[i_end]).astype('timedelta64[D]')
                                                 < np.timedelta64(15, 'D')):
        i_end += 1

    future_list = []
    loaded = None
    _LOG.debug("aggregate over %s", (time, grouped.box.time.data[i_end-1]))
    with MPIPoolExecutor(max_workers=8) as executor:
        for i in range(i_start, i_end):
            to_split = VirtualDatasetBox(grouped.box.sel(time=grouped.box.time.data[i:i+1]), grouped.geobox,
                grouped.load_natively, grouped.product_definitions, grouped.geopolygon)
            future = executor.submit(load_timeslice, fc_product, to_split)
            future_list.append(future)
        for future in future_list:
            if loaded is None:
                loaded = future.result()
            else:
                loaded = xr.concat([loaded, future.result()], dim='time')
                fc_data, water_data = aggregate_data(loaded.drop('water'), loaded.water)
                loaded = xr.merge([fc_data, water_data.to_dataset()]).expand_dims('time')
    nodata = []
    for var in loaded.data_vars:
        nodata.append(loaded[var].attrs.get('nodata', 0))
    loaded = loaded.to_array()[:, 0, :, :]
    perc, vfid_list = cal_area(loaded.data.astype('float32'), mask_array.astype('int64'),
                    np.array(fid_list, dtype='int64'), np.array(nodata, dtype='float32'))
    with MPIPoolExecutor(max_workers=8) as executor:
        executor.map(filter_store_result, iter_args(time, ready, vfid_list, perc))
    return i_end

def get_polyName(feature):
    'function for QLD shapefile types'
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

def load_timeslice(fc_product, to_split):
    load_start = datetime.now()
    results = fc_product.fetch(to_split)
    #results = fc_product.fetch(to_split,  dask_chunks={'time':1, 'y':2000, 'x':2000})
    results = ApplyMask('pixelquality', apply_to=['BS', 'PV', 'NPV', 'TCW']).compute(results)
    water_mask = water_value = results.water.to_dataset()
    flags = {'cloud': False,
            'cloud_shadow': False,
            'noncontiguous': False,
            'water_observed': False
            }
    water_mask = MakeMask('water', flags).compute(water_mask)

    flags = {'cloud': False,
            'cloud_shadow': False,
            'noncontiguous': False,
            'water_observed': True
            }

    water_value = MakeMask('water', flags).compute(water_value)
    results = results.drop('water').merge(water_mask)
    results = ApplyMask('water', apply_to=['BS', 'PV', 'NPV', 'TCW']).compute(results)
    results = results.merge(water_value)
    _LOG.debug("load end %s", datetime.now() - load_start)
    _LOG.debug("load time slice %s", results.time.data)
    return results

def cal_timeslice(fc_product, to_split, mask_array, fid_list, ready, nthreads):

    results = load_timeslice(fc_product, to_split)
    nodata = []
    for var in results.data_vars:
        nodata.append(results[var].attrs.get('nodata', 0))
    results = results.to_array()[:, 0, :, :]
    cal_start = datetime.now()
    perc, vfid_list = cal_area(results.data.astype('float32'), mask_array.astype('int64'),
            np.array(fid_list, dtype='int64'), np.array(nodata, dtype='float32'), nthreads)
    _LOG.debug("cal end %s", datetime.now() - cal_start)
    time = to_split.box.time.data[0]
    return time, perc, vfid_list

def iter_args(time, ready, poly_list, perc):
    for poly_id, result in zip(poly_list, perc):
        if poly_id >= 0:
            yield((poly_id, time, ready, result))

def all_polygons(fc_product, grouped, fid_list, mask_array, aggregate, time_chunk):

    i = 0 
    j = time_chunk 

    # check last update time
    dio = DIO.get()
    time = db_last_update_time(dio, fid_list)
    while i < grouped.box.time.size:
        if time >= grouped.box.time.data[i]:
            i += 1
        elif aggregate and (np.abs(time - grouped.box.time.data[i]).astype('timedelta64[D]')
                                                 < np.timedelta64(15, 'D')):
            i += 1
        else:
            break

    _LOG.debug("time from db %s", time)

    ready = False
    nthreads = int(os.environ.get('OMP_NUM_THREADS', 8))//min(8, time_chunk)
    nthreads = max(nthreads, 1)
    while i < grouped.box.time.size:
        if aggregate:
        # aggregate over time
            i = aggregate_over_timeslice(fc_product, grouped, i, ready, fid_list, mask_array)
            continue

        future_list = []
        # load data for j time slice
        with MPIPoolExecutor(max_workers=8) as executor:
            for i_start in range(i, min(grouped.box.time.size, i+j), 1):
                to_split = VirtualDatasetBox(grouped.box.sel(time=grouped.box.time.data[i_start:i_start+1]), grouped.geobox,
                    grouped.load_natively, grouped.product_definitions, grouped.geopolygon)
                _LOG.debug("submit job for %s", to_split)
                future = executor.submit(cal_timeslice, fc_product, to_split, mask_array, fid_list, ready, nthreads)
                future_list.append(future)

        for future in future_list:
            time, perc, vfid_list = future.result()
            insert_start = datetime.now()
            with MPIPoolExecutor(max_workers=8) as executor:
                executor.map(filter_store_result, iter_args(time, ready, vfid_list, perc))
            _LOG.debug("insert end %s", datetime.now() - insert_start)

        i += j

    # finished all the time slices, update result state to be ready
    time = grouped.box.time.data[-1]
    # this is bad, should be fixed later
    result = np.zeros(5)
    for poly_id in fid_list:
        filter_store_result((poly_id, time, True, result))


def get_polygon_list(feature_list, shapefile):
    with fiona.open(shapefile) as allshapes:
        crs = allshapes.crs_wkt
        start_f = iter(allshapes)
        shape = next(start_f)
        with open(feature_list, 'r') as f:
            for line in f:
                shape_id = int(line.rstrip())
                while shape_id != int(shape['id']):
                    shape = next(start_f)
                yield((shape, crs, shapefile))

def shape_list(shapefile):
    with fiona.open(shapefile) as allshapes:
        for shape in allshapes:
            yield(shape)

def intersect_with_landsat(shape, landsat=landsat_shp):
    with fiona.open(landsat) as shapes:
        crs = shapes.crs_wkt
    geometry = shape['geometry']
    if CRS(crs).epsg == 3577:
        crs_same = True
    else:
        crs_same = False
        o_crs = Proj('EPSG:3577')
        d_crs = Proj(crs)
    if geometry['type'] == 'MultiPolygon':
        pl_wetland = []
        for coords in geometry['coordinates']:
            if not crs_same:
                y, x = transform(o_crs, d_crs, np.array(coords[0])[:, 0],
                                np.array(coords[0])[:, 1])
                pl_wetland.append(Polygon(np.concatenate([[x], [y]]).transpose()))
            else:
                pl_wetland.append(Polygon(coords[0]))

        pl_wetland = MultiPolygon(pl_wetland)
    else:
        if not crs_same:
            y, x = transform(o_crs, d_crs, np.array(geometry['coordinates'][0])[:, 0],
                                     np.array(geometry['coordinates'][0])[:, 1])
            pl_wetland = Polygon(np.concatenate([[x], [y]]).transpose())   
        else:
            pl_wetland = Polygon(geometry['coordinates'][0])

    contain = []
    intersect = []
    with fiona.open(landsat) as shapes:
        for s in shapes:
            pl_landsat = Polygon(s['geometry']['coordinates'][0])
            if pl_wetland.within(pl_landsat):
                contain.append(s['id'])
            elif pl_wetland.intersects(pl_landsat):
                if (pl_wetland.intersection(pl_landsat).area / pl_wetland.area < 0.9):
                    intersect.append(s['id'])
                else:
                    contain.append(s['id'])
    return (shape['id'], contain, intersect)

def query_process(args):
    fc_product, query_poly, time = args
    dc = Datacube()
    query = {'geopolygon': query_poly, 'time': time}
    datasets = fc_product.query(dc, **query)
    grouped = fc_product.group(datasets, **query)
    _LOG.debug("query %s done", query)
    return grouped

def iter_shapes(t_file, shapefile):
    with fiona.open(shapefile) as allshapes:
        start_f = iter(allshapes)
        shape = next(start_f)
        with open(t_file, 'r') as f:
            for line in f:
                shape_id = line 
                while int(shape_id) != int(shape['id']):
                    shape = next(start_f)
                yield shape

def convert_shape_to_polygon(shape):
    if shape['geometry']['type'] == 'MultiPolygon':
        pl_wetland = []
        for coords in shape['geometry']['coordinates']:
            pl_wetland.append(Polygon(coords[0]))
        pl_wetland = MultiPolygon(pl_wetland)
    else:
        pl_wetland = Polygon(shape['geometry']['coordinates'][0])
    return pl_wetland

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


shapefile_path = click.argument('shapefile', type=str, default='/g/data/r78/rjd547/DES-QLD_Project/data/Wet_WGS84_P.shp')
product_definition = click.option('--product-yaml', type=str, help='yaml file of virtual product recipe',
        default='/g/data1a/u46/users/ea6141/wlinsight/fc_pd.yaml')

@click.group(help=__doc__)
def main():
    pass

@main.command(name='wit-plot', help='Plot png and dump csv from database')
@shapefile_path
@click.option('--output-location',  type=str, help='Location to save the query results', default='./results')
@click.option('--output-name',  type=str, help='A property from shape file used to populate file name, default feature_id', default=None)
@click.option('--feature',  type=int, help='An individual polygon to plot', default=None)

def wit_plot(shapefile, output_location, output_name, feature):
    if not path.exists(output_location):
        os.makedirs(output_location)

    with fiona.open(shapefile) as allshapes:
        start_f = iter(allshapes)
        while True:
            try:
                shape = next(start_f)
            except:
                break
            if feature is not None:
                if int(shape['id']) != feature:
                    continue

            poly_name, count = query_wit_data(shape)
            if count.size == 0:
                continue
            if output_name is None:
                file_name = shape['id']
                if poly_name == '__':
                    poly_name = str(shape['id'])
            else:
                file_name = shape['properties'].get(output_name, shape['id'])
                poly_name = file_name
            if "/" in poly_name:
                poly_name = poly_name.replace("/", " ")
            pd.DataFrame(data=count, columns=['TIME', 'BS', 'NPV', 'PV', 'WET', 'WATER']).to_csv(
                    '/'.join([output_location, file_name+'.csv']), index=False)
            b_image = plot_to_png(count, poly_name)
            with open('/'.join([output_location, file_name+'.png']), 'wb') as f:
                f.write(b_image.read())

            if feature is not None:
                break

@main.command(name='wit-query', help='Query datasets by path/row')
@shapefile_path
@click.option('--input-folder', type=str, default='./')
@click.option('--start-date',  type=str, help='Start date, default=1987-01-01', default='1987-01-01')
@click.option('--end-date',  type=str, help='End date, default=2020-01-01', default='2020-01-01')
@click.option('--output-location',  type=str, help='Location to save the query results', default='./query_results')
@click.option('--union',  type=bool, help='If union all the polygons', default=False)
@product_definition

def wit_query(shapefile, input_folder, start_date, end_date, output_location, union, product_yaml):
    tile_files = []

    if not path.exists(output_location):
        os.makedirs(output_location)

    with open(product_yaml, 'r') as f:
        recipe = yaml.safe_load(f)
    fc_product = construct(**recipe)

    with fiona.open(landsat_shp) as shapes:
        landsat_crs = shapes.crs_wkt
        tile_list = list(shapes)
                
    for (_, _, filenames) in walk(input_folder):
        tile_files.extend(filenames)
    
    with fiona.open(shapefile) as allshapes:
        crs = allshapes.crs_wkt

    for t_file in tile_files:
        tile_id = re.findall(r"\d+", t_file)
        _LOG.debug("query %s", '_'.join(tile_id))
        if path.exists(output_location+'/'+'_'.join(tile_id)+'.pkl'):
            continue
        if len(tile_id) > 1 or union:
            future_list = []
            query_poly = []
            poly_list = iter_shapes('/'.join([input_folder, t_file]), shapefile)
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
                            query_poly.append(convert_shape_to_polygon(poly))
                        else:
                            query_poly.append(poly.result())
                        if len(query_poly) == 10:
                            future = executor.submit(union_shapes, query_poly)
                            future_list.append(future)
                            query_poly = []
                    if len(query_poly) > 1:
                        future = executor.submit(union_shapes, query_poly)
                        future_list.append(future)
                    _LOG.debug("len future list %s", len(future_list))

            query_poly = query_poly[0]                      
            query_poly = Geometry(mapping(box(*query_poly.bounds)), CRS(crs))
            _LOG.debug("query_poly %s", query_poly)
        else:
            shape = tile_list[int(tile_id[0])]
            query_poly = Geometry(shape['geometry'], CRS(landsat_crs))

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
@click.option('--output-location', type=str, help='Location for output data files', default='./')
def match_pathrow(shapefile,  output_location):
    if not path.exists(output_location):
        os.makedirs(output_location)

    future_list = []
    total_poly = 0
    with MPIPoolExecutor() as executor:
        for shape in shape_list(shapefile):
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
@click.option('--start-date',  type=str, help='Start date, default=1987-01-01', default='1987-01-01')
@click.option('--end-date',  type=str, help='End date, default=2020-01-01', default='2020-01-01')
@click.option('--time-chunk',  type=int, help='Time slices to load in one go', default=8)
@click.option('--feature-list',  type=str, help='A file of features ID', default=None)
@click.option('--datasets',  type=str, help='Pickled datasets', default=None)
@click.option('--aggregate', type=bool, help='If the polygon requires aggregation over path/row', default=False)
@product_definition
def wit_cal(shapefile, start_date, end_date, time_chunk, feature_list, datasets, aggregate, product_yaml):

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

    poly_list =  get_polygon_list(feature_list, shapefile)

    poly_vessel = []
    shape_vessel = []
    time_start_insert = datetime.now()
    with MPIPoolExecutor(max_workers=8) as executor:
        result = executor.map(query_store_polygons, poly_list)

    for re in result:
        if re[0] is not None:
            shape_vessel.append(re)
            poly_vessel.append(re[1])
    _LOG.debug("gone over all the polygons in %s", datetime.now() - time_start_insert)
    _LOG.debug("number of polygons %s", len(poly_vessel))

    if poly_vessel == []:
        _LOG.info("all done")
        sys.exit(0)

    with open(datasets, 'rb') as f:
        grouped = pickle.load(f)
    _LOG.debug("grouped datasets %s", grouped)
    
    mask_array = generate_raster(shape_vessel, grouped.geobox)
    all_polygons(fc_product, grouped, poly_vessel, mask_array, aggregate, time_chunk)
    _LOG.info("finished")
    sys.exit(0)

if __name__ == '__main__':
    main()
