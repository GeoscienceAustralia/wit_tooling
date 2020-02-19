import fiona
import numpy as np
import pandas as pd
from wit_tooling import *
from wit_tooling.database.io import DIO
from datetime import datetime
import pandas as pd

from bokeh.io import curdoc
from bokeh.layouts import layout, column, row, WidgetBox, gridplot
from bokeh.models import CheckboxGroup, Select,  CategoricalColorMapper, ColumnDataSource,HoverTool, Label, SingleIntervalTicker, Slider, DatetimeTickFormatter, YearsTicker, Legend, TapTool, CustomJS, LegendItem, field
from bokeh.palettes import viridis, brewer
from bokeh.plotting import figure
from bokeh.transform import factor_cmap, LinearColorMapper 
from bokeh.events import DoubleTap

dio = DIO.get()

def get_catchments():
    catchment_names = {}
    for i in range(26):
        catchment_name = dio.get_name_by_id('catchments', i+1)[0][0]
        if catchment_name == '':
            continue
        catchment_names[i+1] = catchment_name
    return catchment_names

def metric_by_catchment(catchment_name):
    source = None
    for key, name in catchments.items():
        if name != catchment_name:
            continue
        rows = dio.get_polys_by_catchment_id(key, 2000)
        poly_list = list(np.array(rows)[:,0])
        print(len(poly_list))
        if source is None:
            start_time = datetime.now()
            source = get_year_metrics_with_type_area(poly_list)
            print("end query in", datetime.now()-start_time)
            source['catchment'] = catchment_name 
        else:
            start_time = datetime.now()
            tmp = get_year_metrics_with_type_area(poly_list)
            print("end query in", datetime.now()-start_time)
            tmp['catchment'] = catchment_name 
            source = pd.concat([source, tmp], ignore_index=True)
    return source

catchments = get_catchments()
data = metric_by_catchment(catchments[1])
data[data.columns[2:11]] = data[data.columns[2:11]] * 100
data.area = data.area/100 * np.pi
source = ColumnDataSource(data=data.loc[data.year==1987])

type_list = data.type.unique() 
color_map = viridis(len(type_list))
type_list = tuple(list(reversed(type_list)))

color_mapper = factor_cmap('type', palette=color_map, factors=type_list)

plot = figure(y_range=(0, 100), x_range=(0, 100), title='Metrics', plot_height=500, plot_width=600)
plot.xaxis.ticker = SingleIntervalTicker(interval=10)
plot.xaxis.axis_label = "water_max"
plot.yaxis.ticker = SingleIntervalTicker(interval=10)
plot.yaxis.axis_label = "pv_max"

label = Label(x=1.1, y=18, text='1987', text_font_size='70pt', text_color='#eeeeee')
plot.add_layout(label)
cc = plot.circle(
    x='water_max',
    y='pv_max',
    size='area',
    source = source,
    fill_color=color_mapper,
    fill_alpha=0.5,
    line_color='#7c7e71',
    line_width=0.5,
    line_alpha=0.5,
    )

catchment_legend = Legend(items=[LegendItem(label=field('type'), renderers=[cc])], location="top_left")
# this one is not working for single glypy
#catchment_legend.click_policy="hide"
plot.add_layout(catchment_legend, 'left')

def catchment_update(attrname, old, new):
    c_name = c_select.value
    global data
    data = metric_by_catchment(c_name)
    data[data.columns[2:11]] = data[data.columns[2:11]] * 100
    data.area = data.area/100 * np.pi
    select_update(attrname, old, new)

def select_update(attrname, old, new):
    year = year_slider.value
    x_axis = x_select.value
    y_axis = y_select.value
    types = []
    for i in checkbox_group.active:
        types.append(type_list[i])

    cc.glyph.x = x_axis 
    plot.xaxis.axis_label = x_axis 
    cc.glyph.y = y_axis 
    plot.yaxis.axis_label = y_axis 
    label.text = str(year) 

    legend_key = l_select.value
    if legend_key == "ANAE_type":
        color_map = viridis(len(types))
        color_mapper = factor_cmap('type', palette=color_map, factors=types)
        catchment_legend.items = [LegendItem(label=field('type'), renderers=[cc])]
        plot.legend.visible = True
    else:
        plot.legend.visible = False
        if legend_key == 'pv_max':
            color_mapper = {'field': legend_key, 'transform': LinearColorMapper(palette=brewer['Greens'][8], low=100, high=0)}
        elif legend_key == 'water_max':
            color_mapper = {'field': legend_key, 'transform': LinearColorMapper(palette=brewer['Blues'][8], low=100, high=0)}

    cc.glyph.fill_color=color_mapper
    source.data = data.loc[(data.year==int(year)) & (data.type.isin(types))]

plot.add_tools(HoverTool(tooltips=[('Id', "@poly_id"), ('Polygon', "@poly_name"), ("Catchment", "@catchment")],
show_arrow=False, point_policy='follow_mouse'))

year_slider = Slider(start=1987, end=2019, value=1987, step=1, title="Year", height=50, width=300, sizing_mode='fixed')
year_slider.on_change('value', select_update)
x_select = Select(title="X-axis", value='water_max', options=list(data.columns[2:11]), height=50, width=100, sizing_mode="fixed")
x_select.on_change('value', select_update)
y_select = Select(title="Y-axis", value='pv_max', options=list(data.columns[2:11]), height=50, width=100, sizing_mode="fixed")
y_select.on_change('value', select_update)
l_select = Select(title="Legend", value='ANAE_type', options=['ANAE_type', 'water_max', 'pv_max'], height=50, width=100, 
        sizing_mode="fixed")
l_select.on_change('value', select_update)
c_select = Select(title="Catchment", value=catchments[1], options=list(catchments.values()), height=50, width=100, sizing_mode="fixed")
c_select.on_change('value', catchment_update)

checkbox_group = CheckboxGroup(labels=list(type_list), active=list(np.arange(len(type_list))), height=600, width=300, sizing_mode="scale_height")
checkbox_group.on_change('active', select_update)

controls = column(x_select, y_select, l_select, c_select, checkbox_group, year_slider, height=100, width=400, sizing_mode='fixed')

layout = layout([
    [controls, plot],
], sizing_mode='scale_height')

curdoc().add_root(layout)
curdoc().title = "Metrics"
