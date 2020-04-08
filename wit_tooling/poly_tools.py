from shapely.geometry import Polygon, MultiPolygon, shape
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
from pandas.plotting import register_matplotlib_converters
from .database.io import DIO

register_matplotlib_converters()

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

def hash_polygon(geometry, area_leng):
    bound = shape(geometry).bounds
    m = hashlib.md5()
    m.update(json.dumps(bound).encode('utf-8'))
    m.update(json.dumps(area_leng).encode('utf-8'))
    bound_hash = m.digest()
    return bound_hash

def hash_from_shape(shape):
    property_keys = ['shape_area', 'shape_leng', 'area']
    shape_property = 0
    for key in property_keys:
        for p_key in shape['properties'].keys():
            if p_key.lower() == key:
                shape_property = shape['properties'].get(p_key, 0)
                break
        if shape_property != 0:
            break

    shape_id = int(shape['id'])
    area_leng = dict({p_key:shape_property})
    print("area leng", area_leng)

    poly_hash = hash_polygon(shape['geometry'], area_leng)
    print("poly hash", poly_hash)
    return poly_hash

def query_wit_data(shape):
    dio = DIO.get()
    poly_hash = poly_wkt(shape['geometry']) 
    poly_name, rows = dio.get_data_by_geom(poly_hash)
    return poly_name, np.array(rows)

def plot_to_png(count, polyName):
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
    #ax.yaxis.set_ticks(np.arange(0,110,10))
    ax.set_xlabel(f'The Fractional Cover algorithm developed by the Joint Remote'
    f' Sensing Research Program and \n the Water Observations from Space algorithm '
    f'developed by Geoscience Australia are used in the production of this data',style='italic')
    LS5_8_gap_start = datetime(2011,11,1)
    LS5_8_gap_end = datetime(2013,4,1)

    # convert to matplotlib date representation
    gap_start = mdates.date2num(LS5_8_gap_start)
    gap_end = mdates.date2num(LS5_8_gap_end)
    gap = gap_end - gap_start

    # set up rectangle
    slc_rectangle= Rectangle((gap_start,0), gap, 100,alpha = 0.5, facecolor='#ffffff',
                edgecolor='#ffffff', hatch="////",linewidth=2)
    ax.add_patch(slc_rectangle)

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
