from datacube import Datacube
from datacube.virtual.impl import VirtualDatasetBox
from datacube.virtual import construct
from datacube.utils.geometry import CRS, Geometry
from . import convert_shape_to_polygon, load_timeslice
import yaml
from shapely.geometry import mapping, box

def construct_product(product_yaml):
    """
    Construct a virtual product with the given yaml file
    input:
    product_yaml: the yaml file path
    output:
    virtual product instance
    """
    with open(product_yaml, 'r') as f:
        recipe = yaml.safe_load(f)
    fc_product = construct(**recipe)
    return fc_product

def query_datasets(fc_product, shape, crs, time_range):
    """
    Query the datasets in datacube database with the given shape and time period
    input:
    fc_product: virtual product instance
    shape: a shape from shape file
    crs: crs string from shape file
    time_range: a tuple of (start_time, end_time)
    output:
    grouped datasets: VirtualDatasetBox
    """
    dc = Datacube()
    query_poly = convert_shape_to_polygon(shape['geometry'])
    query_poly = Geometry(mapping(box(*query_poly.bounds)), CRS(crs))
    query = {'geopolygon': query_poly, 'time': time_range}
    datasets = fc_product.query(dc, **query)
    grouped = fc_product.group(datasets, **query)
    return grouped

def load_wofs_fc(fc_product, grouped, time_slice):
    """
    Load cloud free wofs, TCW and FC data with the given time or a tuple of (start_time, end_time)
    input:
    fc_product: virtual product instance
    grouped: grouped datasets
    time_slice: a single time or tuple of (start_time, end_time)
    output:
    wofs, TCW and FC data: xr.Dataset
    """
    if not (isinstance(time_slice, list) or isinstance(time_slice, tuple)):
        time_slice = [time_slice]
    to_load = VirtualDatasetBox(grouped.box.loc[time_slice], grouped.geobox,
                                grouped.load_natively, grouped.product_definitions, grouped.geopolygon)
    fc_wofs_data = load_timeslice(fc_product, to_load)
    return fc_wofs_data
