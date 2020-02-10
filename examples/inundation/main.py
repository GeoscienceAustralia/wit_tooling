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
from bokeh.palettes import plasma
from bokeh.plotting import figure
from bokeh.transform import factor_cmap
from bokeh.events import DoubleTap

dio = DIO.get()

def inundation_by_catchment(start_year, end_year):
    source = None
    for i in range(26):
        catchment_name = dio.get_name_by_id('catchments', i+1)[0][0]
        if catchment_name == '':
            continue
        rows = dio.get_polys_by_catchment_id(i+1, 5000)
        poly_list = list(np.array(rows)[:,0])
        print(len(poly_list))
        if source is None:
            start_time = datetime.now()
            source = get_inundation(poly_list, start_year, end_year, 50, 1000)
            source = source.loc[source.poly_name != '__']
            print("end query in", datetime.now()-start_time)
            source['catchment'] = catchment_name 
        else:
            start_time = datetime.now()
            tmp = get_inundation(poly_list, start_year, end_year, 50, 1000)
            tmp = tmp.loc[tmp.poly_name != '__']
            print("end query in", datetime.now()-start_time)
            tmp['catchment'] = catchment_name 
            source = pd.concat([source, tmp], ignore_index=True)
    return source

decades = [(1990, 2000), (2000, 2010), (2010, 2020)]
data = None
for d in decades:
    if data is None:
        data = inundation_by_catchment(d[0], d[1])
        data['decade'] = d[1]
    else:
        tmp = inundation_by_catchment(d[0], d[1])
        tmp['decade'] = d[1]
        data = pd.concat([data, tmp], ignore_index=True)

data.percent = data.percent * 100
data.area = data.area/100 * np.pi

source = ColumnDataSource(data=data[data.decade==2020])

poly_id = data.poly_id.iloc[0]
poly_data = get_area_by_poly_id(int(poly_id))
poly_area = poly_data.area.unique()[0]
poly_data = poly_data.drop(columns=['area'])

single_source = ColumnDataSource(data=poly_data)

poly_id_source = ColumnDataSource(data=dict(poly_id=[]))

catchment_list = list(data.catchment.unique())
color_map = plasma(len(catchment_list))
print(color_map)

plot = figure(y_range=(0, 100), x_range=(0, 11), title='Inundation', tools="tap", plot_height=400, plot_width=500, sizing_mode='scale_height')
plot.xaxis.ticker = SingleIntervalTicker(interval=1)
plot.xaxis.axis_label = "Occurence in Years"
plot.yaxis.ticker = SingleIntervalTicker(interval=10)
plot.yaxis.axis_label = "Percent of Duration"

label = Label(x=1.1, y=18, text='2010-2020', text_font_size='70pt', text_color='#eeeeee')
plot.add_layout(label)
color_mapper = factor_cmap('catchment', palette=color_map, factors=catchment_list)
cc = plot.circle(
            x='wet_years',
            y='percent',
            size='area',
            source = source,
            fill_color=color_mapper,
            fill_alpha=0.5,
            line_color='#7c7e71',
            line_width=0.5,
            line_alpha=0.5,
            )
catchment_legend = Legend(items=[LegendItem(label=field('catchment'), renderers=[cc])], location="top_left")
# this one is not working for single glypy
#catchment_legend.click_policy="hide"
plot.add_layout(catchment_legend, 'left')

def poly_update(attrname, old, new):
    poly_id = poly_select.value
    if poly_id == '':
        return
    poly_data = get_area_by_poly_id(int(poly_id))
    poly_area = poly_data.area.unique()[0]
    poly_data = poly_data.drop(columns=['area'])
    sub_plot.y_range.end = poly_area
    single_source.data = poly_data
    sub_plot.title.text = data.poly_name.loc[data.poly_id == int(poly_id)].iloc[0] 

poly_select = Select(title="Polygons", value='', options=[''], height=50, width=100, sizing_mode="fixed")
poly_select.on_change('value', poly_update)

js_code = """
    const inds=cb_obj.indices;
    var data_s = source.data;
    var data_d = target.data;
    data_d['poly_id'] = [];
    for (var i=0; i<inds.length; i++) {
        data_d['poly_id'].push(String(data_s['poly_id'][inds[i]]));
    }
    select.options = data_d['poly_id']
    select.value = data_d['poly_id'][0]
"""
js_callback = CustomJS(args={'source': source, 'target': poly_id_source, 'select': poly_select}, code=js_code)
source.selected.js_on_change('indices', js_callback)

plot.add_tools(HoverTool(tooltips=[('Id', "@poly_id"), ('Polygon', "@poly_name"), ("Catchment", "@catchment")],
    show_arrow=False, point_policy='follow_mouse'))

def select_update(attrname, old, new):
    decade = int(select.value)
    catchments = []
    for i in checkbox_group.active:
        catchments.append(catchment_list[i])
    label.text = '-'.join([str(decade-10), str(decade)])
    refreshed_data = data.loc[(data.decade==decade) & data.catchment.isin(catchments)].reset_index()
    source.data = refreshed_data 
    source.selected.indices = refreshed_data.index[refreshed_data.poly_id.isin(poly_select.options)].tolist()
    color_map = plasma(len(catchments))
    color_mapper = factor_cmap('catchment', palette=color_map, factors=catchments)
    cc.glyph.fill_color=color_mapper

select = Select(title="Decade", value='2020', options=['2000', '2010', '2020'], height=50, width=100, sizing_mode="fixed")
select.on_change('value', select_update)

checkbox_group = CheckboxGroup(labels=catchment_list, active=list(np.arange(len(catchment_list))), height=600, width=300, sizing_mode="scale_height")
checkbox_group.on_change('active', select_update)

controls = column(select, checkbox_group, poly_select,  height=700, width=200, sizing_mode='fixed')

sub_plot = figure(y_range=(0, poly_area), x_range=(poly_data['time'].min(), poly_data['time'].max()), title=data.poly_name.iloc[0],
        plot_height=200, plot_width=900, sizing_mode='stretch_width')

sub_plot.xaxis.formatter = DatetimeTickFormatter()
sub_plot.xaxis.ticker = YearsTicker(interval=1)
sub_plot.yaxis.axis_label = "Area (hectare)" 

pal = [ '#030aa7',
        '#04d9ff', 
        '#3f9b0b',
        '#e6daa6',
        '#60460f'
    ]
v_stack = sub_plot.varea_stack(['open water', 'wet', 'green veg', 'dry veg', 'bare soil'], x='time', 
                         color=pal, source=single_source, alpha=0.6)
legend = Legend(items=[
("bare soil", [v_stack[4]]),
("dry veg", [v_stack[3]]),
("green veg", [v_stack[2]]),
("wet", [v_stack[1]]),
("open water", [v_stack[0]]),
], location="top_left")

sub_plot.add_layout(legend, 'left')

grid = gridplot([plot, sub_plot], ncols=1, plot_height=200, plot_width=400, sizing_mode='scale_height')

layout = layout([
    [controls, grid],
], sizing_mode='scale_both')

curdoc().add_root(layout)
curdoc().title = "Inundataion"
