from shapely.geometry import Polygon, MultiPolygon, shape
from rasterio import features
from rasterio.warp import calculate_default_transform
import hashlib
import json
import numpy as np
import io
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.patches as mpatches
from matplotlib.patches import Rectangle
from textwrap import wrap
from datetime import datetime
import fiona
from pandas.plotting import register_matplotlib_converters
from datacube.virtual.transformations import MakeMask, ApplyMask
from .database.io import DIO

register_matplotlib_converters()
def shape_list(shapefile):
    with fiona.open(shapefile) as allshapes:
        for shape in allshapes:
            yield(shape)

def convert_shape_to_polygon(geometry):
    if geometry['type'] == 'MultiPolygon':
        pl_wetland = []
        for coords in geometry['coordinates']:
            ext = coords[0]
            if len(coords) > 1:
                poly = Polygon(ext, coords[1:])
            else:
                poly = Polygon(ext)
            if poly.is_valid == False:
                poly = poly.buffer(0)
                if poly.geom_type == 'MultiPolygon':
                    for ep in poly:
                        pl_wetland.append(ep)
                else:
                    pl_wetland.append(poly)
            else:
                pl_wetland.append(poly)
        pl_wetland = MultiPolygon(pl_wetland)
    else:
        coords = geometry['coordinates']
        ext = coords[0]
        if len(coords) > 1:
            pl_wetland = Polygon(ext, coords[1:])
        else:
            pl_wetland = Polygon(ext)
        if pl_wetland.is_valid == False:
            pl_wetland = pl_wetland.buffer(0)
    return pl_wetland

def poly_wkt(geometry, srid=3577):
    pl_wetland = convert_shape_to_polygon(geometry)
    return 'SRID=%s;' % (srid)+pl_wetland.to_wkt()

def query_wit_data(shape):
    dio = DIO.get()
    poly_hash = poly_wkt(shape['geometry'])
    poly_name, rows = dio.get_data_by_geom(poly_hash)
    return poly_name, np.array(rows)

def query_wit_metrics(shape, mtype='alltime', set_str=''):
    dio = DIO.get()
    poly_hash = poly_wkt(shape['geometry'])
    if mtype == 'alltime':
        rows = dio.get_alltime_metrics_by_geom(poly_hash)
    elif mtype == 'year':
        rows = dio.get_year_metrics_by_geom(poly_hash)
    elif mtype == 'event':
        rows = dio.get_event_metrics_by_geom(poly_hash, set_str)
    return rows

def load_timeslice(fc_product, to_split, mask_by_wofs=True):
    results = fc_product.fetch(to_split)
    #results = fc_product.fetch(to_split,  dask_chunks={'time':1, 'y':2000, 'x':2000})
    results = ApplyMask('pixelquality', apply_to=['BS', 'PV', 'NPV', 'TCW']).compute(results)
    water_mask = water_value = results.water.to_dataset()
    flags = {'cloud': False,
            'cloud_shadow': False,
            'noncontiguous': False,
            'water_observed': True
            }

    water_value = MakeMask('water', flags).compute(water_value)

    if mask_by_wofs:
        flags = {'cloud': False,
                'cloud_shadow': False,
                'noncontiguous': False,
                'water_observed': False
                }
        water_mask = MakeMask('water', flags).compute(water_mask)
    else:
        water_mask.water.data = ~water_value.water.data

    results = results.drop('water').merge(water_mask)
    results = ApplyMask('water', apply_to=['BS', 'PV', 'NPV', 'TCW']).compute(results)
    results = results.merge(water_value)
    return results

def generate_raster(shapes, geobox):
    yt, xt = geobox.shape
    transform, width, height = calculate_default_transform(
        geobox.crs, geobox.crs.crs_str, yt, xt, *geobox.extent.boundingbox)
    target_ds = features.rasterize(shapes,
        (yt, xt), fill=-1, transform=transform, all_touched=True)
    return target_ds

def plot_to_png(count, polyName):
    min_observe = 4
    pal = ['#030aa7',
            '#04d9ff',
            '#3f9b0b',
            '#e6daa6',
            '#60460f']
    labels = ['open water',
            'wet',
            'green veg',
            'dry veg',
            'bare soil',
            ]

    fig = plt.figure(figsize = (22,6))
    plt.stackplot(count[:, 0].astype('datetime64[s]'),
            count[:, 5].astype('float32') * 100,
            count[:, 4].astype('float32') * 100,
            count[:, 3].astype('float32') * 100,
            count[:, 2].astype('float32') * 100,
            count[:, 1].astype('float32') * 100,
            colors=pal, alpha = 0.6)
    #set axis limits to the min and max
    time_min = count[:, 0].astype('datetime64[s]')[0]
    time_max = count[:, 0].astype('datetime64[s]')[-1]
    plt.axis(xmin = time_min, xmax = time_max, ymin = 0, ymax = 100)
    #add a legend and a tight plot box
    legend_handles = []
    pal.reverse()
    labels.reverse()
    for p, l in zip(pal, labels):
        legend_handles.append(mpatches.Patch(color=p, alpha=0.6, label=l))

    plt.legend(handles=legend_handles, loc='lower left', framealpha=0.6)
    plt.tight_layout()
    years = mdates.YearLocator(1)
    yearsFmt = mdates.DateFormatter('%Y')
    ax = plt.gca()
    ax.xaxis.set_major_locator(years)
    ax.xaxis.set_major_formatter(yearsFmt)
    ax.set_xlabel(f'The Fractional Cover algorithm developed by the Joint Remote'
    f' Sensing Research Program and \n the Water Observations from Space algorithm '
    f'developed by Geoscience Australia are used in the production of this data',style='italic')

    gap_start = None
    gap_end = None

    def plot_patch(gap_start, gap_end):
        tmp_start = mdates.date2num(gap_start.astype('object'))
        tmp_end = mdates.date2num(gap_end.astype('object'))
        gap = tmp_end - tmp_start
        slc_rectangle= Rectangle((tmp_start,0), gap, 100,alpha = 0.5, facecolor='#ffffff',
                edgecolor='#ffffff', hatch="////",linewidth=2)
        ax.add_patch(slc_rectangle)

    ls7_gap_start = np.datetime64('2011-11-01')
    ls7_gap_end =  np.datetime64('2013-04-01')

    # mark observations < min_observe per year
    # and landsat7 gap
    for y in np.arange(time_min.astype('datetime64[Y]'), time_max.astype('datetime64[Y]')+1):
        if (np.count_nonzero((count[:, 0].astype('datetime64[s]') >= y) &
                (count[:, 0].astype('datetime64[s]') < y + 1)) < min_observe):
            if gap_start is None:
                gap_start = y
                gap_end = y+1
            elif y > gap_end:
                plot_patch(gap_start, gap_end)
                gap_start = y
            gap_end = max(gap_end, y+1)

        if y == ls7_gap_start.astype('datetime64[Y]'):
            if gap_end is not None:
                if y > gap_end:
                    plot_patch(gap_start, gap_end)
                    gap_start = ls7_gap_start
            else:
                gap_start = ls7_gap_start
            gap_end = ls7_gap_end
    if gap_start is not None and gap_end is not None:
        plot_patch(gap_start, gap_end)

    # this section wraps text for polygon names that are too long
    polyName=polyName.replace("'","\\'")
    title=ax.set_title("\n".join(wrap(f'Percentage of area dominated by WOfS, Wetness, Fractional Cover for {polyName}')))
    fig.tight_layout()
    title.set_y(1.05)

    bytes_image = io.BytesIO()
    plt.savefig(bytes_image, format='png')
    bytes_image.seek(0)
    plt.close()

    return bytes_image
