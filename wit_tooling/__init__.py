from .poly_tools import convert_shape_to_polygon, poly_wkt, query_wit_data, plot_to_png, query_wit_metrics, load_timeslice, generate_raster, shape_list
from .datacube_util import construct_product, query_datasets, load_wofs_fc
from .database.io import DIO
from .aws_util import *
import pandas as pd
from datetime import datetime, timezone
import xarray as xr

C3 = False
if C3:
    ls_timezone = timezone.utc
else:
    ls_timezone = None

def ls8_on(dataset):
    LS8_START_DATE = datetime(2013, 1, 1, tzinfo=ls_timezone)
    return dataset.center_time >= LS8_START_DATE

def ls7_on(dataset):
    LS7_STOP_DATE = datetime(2003, 5, 31, tzinfo=ls_timezone)
    LS7_STOP_AGAIN = datetime(2013, 5, 31, tzinfo=ls_timezone)
    LS7_START_AGAIN = datetime(2010, 1, 1, tzinfo=ls_timezone)
    return dataset.center_time <= LS7_STOP_DATE or (dataset.center_time >= LS7_START_AGAIN
                                                    and dataset.center_time <= LS7_STOP_AGAIN)

def ls5_on_1ym(dataset):
    LS5_START_AGAIN = datetime(2003, 1, 1, tzinfo=ls_timezone)
    LS5_STOP_DATE = datetime(1999, 12, 31, tzinfo=ls_timezone)
    LS5_STOP_AGAIN = datetime(2011, 12, 31, tzinfo=ls_timezone)
    return dataset.center_time <= LS5_STOP_DATE or (dataset.center_time >= LS5_START_AGAIN
                                                    and dataset.center_time <= LS5_STOP_AGAIN)

def load_wit_data(**kwargs):
    """
    Load pre-computed wit data from 3 different sources with the given parameter. Source is chosen by the key
    in kwargs.
    input parameters:
    csv = csv_path: csv file path
    shape = a shape from shape file
    s3_url = url of s3 bucket: s3 bucket path
    output:
    pandas dataframe of wit data
    """
    if kwargs.get("csv") is not None:
        wit_data = pd.read_csv(kwargs['csv'])
    elif kwargs.get('shape') is not None:
        _, wit_data = query_wit_data(kwargs['shape'])
        wit_data = pd.DataFrame(data=wit_data, columns=['TIME', 'BS', 'NPV', 'PV', 'WET', 'WATER'])
        wit_data[wit_data.columns[1:]] = wit_data[wit_data.columns[1:]] * 100
    elif kwargs.get('s3_url') is not None:
        wit_data = pd.read_csv(kwargs['s3_url'], infer_datetime_format=True)

    wit_data.index.name = 'no'
    #Rename the columns so they are easier to understand and plot
    wit_data = wit_data.rename(columns={
        "TIME": "utc_time",
        "WATER" : "water",
        "WET" : "wet",
        "PV" : "green",
        "NPV" : "dry",
        "BS" : "bare",
        "time": "utc_time",
        "TCW": "wet"})
    # plot requires np.datetime64 input from csv will be np.obj
    # convert in case
    wit_data.utc_time = wit_data.utc_time.astype('datetime64')
    return wit_data

def spatial_wit(fc_wofs_data, mask):
    """
    Compute spatial wit with wofs, TCW and FC data with the given polygon mask
    input:
    fc_wofs_data: wofs, TCW and FC data: xr.Dataset
    mask: a polygon mask: np.array
    output:
    spatial wit results: xr.Dataset
    """
    none_water_vars = list(fc_wofs_data.data_vars)[:-1]
    water_var = list(fc_wofs_data.data_vars)[-1]
    fc_data = fc_wofs_data[none_water_vars].where(fc_wofs_data[water_var] < 1)
    tcw_percent = fc_data['TCW'] >= -350
    fc_percent = fc_data.drop('TCW')
    fc_percent = fc_percent.where((~tcw_percent & (fc_percent != fc_percent[list(fc_percent.data_vars)[0]].attrs['nodata'])), 0)
    sw_result = xr.merge([fc_percent, (tcw_percent.astype("int") * 100),
                          (fc_wofs_data[water_var].astype("int") * 100)])
    sw_result = sw_result.where(sw_result > 0, -127)
    sw_result = sw_result.where(mask >= 0, -127).astype("int16")
    sw_result.attrs.update(fc_wofs_data.attrs)
    for var in sw_result.data_vars:
        sw_result[var].attrs['nodata'] = -127
    return sw_result

def get_alltime_metrics(poly_list):
    dio = DIO.get()
    rows = dio.get_alltime_metrics(poly_list)
    return pd.DataFrame(rows, columns=['poly_id', 'pv_perc', 'openwater_penc',
                                       'wet_perc', 'pv_fot', 'openwater_fot', 'wet_fot'])

def get_wet_year_metrics(poly_list):
    dio = DIO.get()
    rows = dio.get_wet_year_metrics(poly_list)
    return pd.DataFrame(rows, columns=['poly_id', 'year', 'min', 'max', 'mean'])

def get_pv_year_metrics(poly_list):
    dio = DIO.get()
    rows = dio.get_pv_year_metrics(poly_list)
    return pd.DataFrame(rows, columns=['poly_id', 'year', 'min', 'max', 'mean'])

def get_year_metrics_with_type_area(poly_list, wtype):
    dio = DIO.get()
    rows = dio.get_year_metrics_with_type_area(poly_list, wtype)
    return pd.DataFrame(rows, columns=['poly_id', 'year', 'wet_min', 'wet_max', 'wet_mean',
                                        'water_min', 'water_max', 'water_mean',
                                        'pv_min', 'pv_max', 'pv_mean', 'poly_name', 'area', 'type'])

def get_event_metrics(poly_list):
    dio = DIO.get()
    rows = dio.get_event_metrics(poly_list)
    return  pd.DataFrame(rows, columns=['poly_id', 'start_time', 'end_time', 'duration', 'max', 'mean', 'area'])

def get_inundation(poly_list, start_date, end_date, min_area, max_rows):
    dio = DIO.get()
    rows = dio.get_inundation(poly_list, start_date, end_date, min_area, max_rows)
    return pd.DataFrame(rows, columns=['poly_id', 'poly_name', 'wet_years', 'percent', 'area'])

def get_area_by_poly_id(poly_list):
    dio = DIO.get()
    rows = dio.get_area_by_poly_id(poly_list)
    return pd.DataFrame(rows, columns=['time', 'area', 'bare soil', 'dry veg', 'green veg', 'wet', 'open water'])
