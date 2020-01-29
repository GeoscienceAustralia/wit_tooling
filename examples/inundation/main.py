import fiona
import numpy as np
import pandas as pd
from wit_tooling import *
from wit_tooling.database.io import DIO
from datetime import datetime
import pandas as pd

from bokeh.io import curdoc
from bokeh.layouts import layout, column, row, WidgetBox
from bokeh.models import Tabs, Panel, CheckboxGroup, Select, Button, CategoricalColorMapper, ColumnDataSource,HoverTool, Label, SingleIntervalTicker, Slider
from bokeh.palettes import plasma, Spectral6
from bokeh.plotting import figure
from bokeh.transform import factor_cmap

dio = DIO.get()

def inundation_by_catchment(start_year, end_year):
    source = None
    for i in range(26):
        catchment_name = dio.get_name_by_id('catchments', i+1)
        if catchment_name == '':
            continue
        rows = dio.get_polys_by_catchment_id(i+1, 10000)
        poly_list = list(np.array(rows)[:,0])
        print(len(poly_list))
        if source is None:
            start_time = datetime.now()
            source = get_inundation(poly_list, start_year, end_year, 50, 1000)
            print("end query in", datetime.now()-start_time)
            source['catchment'] = catchment_name 
        else:
            start_time = datetime.now()
            tmp = get_inundation(poly_list, start_year, end_year, 50, 1000)
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

catchment_list = list(data.catchment.unique())
color_map = plasma(len(catchment_list))
print(color_map)

source = ColumnDataSource(data=data[data.decade==2020])

plot = figure(y_range=(0, 100), x_range=(0, 11), title='Inundation', plot_height=400, plot_width=900, sizing_mode='scale_both')
plot.xaxis.ticker = SingleIntervalTicker(interval=1)
plot.xaxis.axis_label = "Years"
plot.yaxis.ticker = SingleIntervalTicker(interval=10)
plot.yaxis.axis_label = "Percent"

label = Label(x=1.1, y=18, text='2010-2020', text_font_size='70pt', text_color='#eeeeee')
plot.add_layout(label)
color_mapper = factor_cmap('catchment', palette=color_map, factors=catchment_list)
plot.circle(
            x='wet_years',
            y='percent',
            size='area',
            source = source,
            fill_color=color_mapper,
            fill_alpha=0.5,
            line_color='#7c7e71',
            line_width=0.5,
            line_alpha=0.5,
            legend_field='catchment'
            )

plot.legend.location = "top_left"
plot.legend.click_policy="mute"

plot.add_tools(HoverTool(tooltips="@catchment", show_arrow=False, point_policy='follow_mouse'))

def slider_update(attrname, old, new):
    decade = int(select.value)
    catchments = []
    for i in checkbox_group.active:
        catchments.append(catchment_list[i])
    label.text = '-'.join([str(decade-10), str(decade)])
    source.data = data.loc[(data.decade==decade) & data.catchment.isin(catchments)]

select = Select(title="Decade", value='2020', options=['2000', '2010', '2020'], height=50, width=100, sizing_mode="fixed")
select.on_change('value', slider_update)

checkbox_group = CheckboxGroup(labels=catchment_list, active=list(np.arange(len(catchment_list))), height=400, width=200, sizing_mode="scale_both")
checkbox_group.on_change('active', slider_update)

controls = column(select, checkbox_group, height=400, width=200, sizing_mode='fixed')

layout = layout([
    [controls, plot],
], sizing_mode='scale_both')

curdoc().add_root(layout)
curdoc().title = "Inundation"
