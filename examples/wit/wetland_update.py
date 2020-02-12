import fiona
import pandas as pd
import numpy as np
from mpi4py import MPI
from mpi4py.futures import MPIPoolExecutor 
from wetland_brutal import intersect_with_landsat, shape_list
from wit_tooling import hash_from_shape, poly_wkt
from wit_tooling.database.io import DIO
from shapely.geometry import Polygon, MultiPolygon
import json

def db_insert_catchment(dio, shape):
    geometry = shape['geometry']
    catchment_name = shape['properties']['BNAME']
    shapefile = shape['properties']['path']
    feature_id = shape['id']
    catchment_id = dio.insert_catchment(catchment_name=catchment_name,
            shapefile=shapefile, feature_id=feature_id, geometry=poly_wkt(geometry))
    return catchment_id 

def insert_catchment(catchment_shape):
    dio = DIO.get()
    with fiona.open(catchment_shape) as allshapes:
        crs = allshapes.crs_wkt
        for shape in allshapes:
            db_insert_catchment(dio,shape)
        return

def update_polygon_geom(shapefile):
    dio = DIO.get()
    query = """select feature_id, poly_id from polygons where poly_hash is null and 
    shapefile='/g/data/u46/users/ea6141/wlinsight/shapefiles/ramsar_wetlands_2018_exploded_3577.shp'
    """
    rows = dio.query_with_return(query)
    feature_list = list(np.array(rows)[:,0].astype('str'))
    poly_list = list(np.array(rows)[:,1].astype('str'))

    for shape in shape_list(shapefile):
        if str(shape['id']) in feature_list:
            poly_id = poly_list[feature_list.index(str(shape['id']))]
            poly_id = dio.update_polygon_geom(poly_id, poly_wkt(shape['geometry']))
            print("update poly", poly_id)

def update_polygon_properties(shapefile):
    dio = DIO.get()
    for shape in shape_list(shapefile):
        geometry = shape['geometry']
        poly_id, _ = dio.get_id_by_geom("polygons", geometry=poly_wkt(geometry))
        print("query", poly_id)
        properties = json.dumps(shape['properties'])
        poly_id = dio.update_polygon_properties(poly_id, properties)
        print("insert", poly_id)

def main(shapefile, catchment_shape):
    update_polygon_properties(shapefile)


if __name__ == "__main__":
    shapefile ='/g/data/u46/users/ea6141/wlinsight/shapefiles/MDB_ANAE_Aug2017_modified_2019_SB_3577.shp'
    catchment_shape = '/g/data/u46/users/ea6141/wlinsight/shapefiles/mdb_anae_catchments.shp'
    main(shapefile, catchment_shape)
