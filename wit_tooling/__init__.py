from .poly_tools import poly_wkt, hash_polygon, hash_from_shape, query_wit_data, plot_to_png
from .database.io import DIO
import pandas as pd

def get_alltime_metrics(poly_list):
    dio = DIO.get()
    rows = dio.get_alltime_metrics(poly_list)
    return pd.DataFrame(rows, columns=['poly_id', 'pv_perc', 'openwater_penc',
                                     'wet_perc', 'pv_fot', 'openwater_fot', 'wet_fot'])

def get_year_metrics(poly_list):
    dio = DIO.get()
    rows = dio.get_year_metrics(poly_list)
    return pd.DataFrame(rows, columns=['poly_id', 'year', 'min', 'max'])

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
