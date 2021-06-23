import numpy as np
import pandas as pd
import fiona
import io
from shapely import geometry
import click
from wit_tooling import query_wit_data

def shape_list(key, values, shapefile):
    """
        Get a generator of shapes from the given shapefile
            key: the key to match in 'properties' in the shape file
            values: a list of property values
            shapefile: the name of your shape file
            e.g. key='ORIGID', values=[1, 2, 3, 4, 5], 
            shapefile='/g/data/r78/DEA_Wetlands/shapefiles/MDB_ANAE_Aug2017_modified_2019_SB_3577.shp'
    """
    count = len(values)
    with fiona.open(shapefile) as allshapes:
        for shape in allshapes:
            shape_id = shape['properties'].get(key)
            if shape_id is None:
                continue
            if isinstance(shape_id, float):
                shape_id = int(shape_id)
            if shape_id in values:
                yield(shape_id, shape)
                count -= 1
            if count <= 0:
                break
    
def get_areas(features, pkey='SYSID'):
    """
        Calculate the area of a list/generator of shapes
        input:
            features: a list of shapes indexed by the key
        output:
            a dataframe of area index by the key
    """
    re = pd.DataFrame()
    for f in features:
        va = pd.DataFrame([[f[0], geometry.shape(f[1]['geometry']).area/1e4]], columns=[pkey, 'area'])
        re = re.append(va, sort=False)
    return re.set_index(pkey)

def dump_wit_data(key, feature_list, output, batch=-1):
    """
        dump wit data from the database into a file
        input:
            key: Name to id the polygon
            feature_list: a list or generator of features
        output:
            a csv file to save all the wit data
    """
    count = 0
    if batch > 0:
        fname = output.split('.')[0]
        sub_fname = fname + '_0.csv' 
        appendix = 0
    else:
        sub_fname = output

    for f_id, f in feature_list:
        _, wit_data = query_wit_data(f)
        csv_buf = io.StringIO()
        wit_df = pd.DataFrame(data=wit_data, columns=['TIME', 'BS', 'NPV', 'PV', 'WET', 'WATER'])
        wit_df.insert(0, key, f_id)
        wit_df.to_csv(csv_buf, index=False, header=False)
        csv_buf.seek(0)
        with open(sub_fname, 'a') as f:
            f.write(csv_buf.read())
        if batch < 0:
            continue
        count += 1
        if count >= batch:
            with open(sub_fname, 'a') as f:
                f.write(','.join(list(wit_df.columns))) 
            count = 0
            appendix += 1
            sub_fname = fname + '_' + str(appendix) + '.csv'
    if count < batch or batch < 0:
         with open(sub_fname, 'a') as f:
            f.write(','.join(list(wit_df.columns))) 

def annual_metrics(wit_data, members=['PV', 'WET', 'WATER', 'BS', 'NPV', ['NPV', 'PV', 'WET'],
                                          ['PV', 'WET'], ['WATER', 'WET']], threshold=[25, 75], pkey='SYSID'):
    """
        Compute the annual max, min, mean, count with given wit data, members and threshold
        input:
            wit_data: dataframe of WIT
            members: the elements which the metrics are computed against, can be a column from wit_data, e.g. 'PV'
                         or the sum of wit columns, e.g. ['WATER', 'WET']
            threshold: a list of thresholds such that (elements >= threshold[i]) is True, 
                        where i = 0, 1...len(threshold)-1
        output:
            dataframe of metrics
    """
    years = wit_data['TIME']
    i = 0
    wit_df = wit_data.copy(deep=True)
    for m in members:
        if isinstance(m, list):
            wit_df.insert(wit_df.columns.size+i, '+'.join(m), wit_df[m].sum(axis=1))
    years = pd.DatetimeIndex(wit_df['TIME']).year.unique()
    shape_id_list = wit_df[pkey].unique()
    #shane changed 4 to 5 to accomodate median added below 
    wit_metrics = [pd.DataFrame()] * 5
    for y in years:
        wit_yearly = wit_df[pd.DatetimeIndex(wit_df['TIME']).year==y].drop(columns=['TIME']).groupby(pkey).max()
        wit_yearly.insert(0, 'YEAR', y)
        wit_yearly = wit_yearly.rename(columns={n: n+'_max' for n in wit_yearly.columns[1:]})
        wit_metrics[0] = wit_metrics[0].append(wit_yearly, sort=False)
    for y in years:
        wit_yearly = wit_df[pd.DatetimeIndex(wit_df['TIME']).year==y].drop(columns=['TIME']).groupby(pkey).min()
        wit_yearly.insert(0, 'YEAR', y)
        wit_yearly = wit_yearly.rename(columns={n: n+'_min' for n in wit_yearly.columns[1:]})
        wit_metrics[1] = wit_metrics[1].append(wit_yearly, sort=False)
    for y in years:
        wit_yearly = wit_df[pd.DatetimeIndex(wit_df['TIME']).year==y].drop(columns=['TIME']).groupby(pkey).mean()
        wit_yearly.insert(0, 'YEAR', y)
        wit_yearly = wit_yearly.rename(columns={n: n+'_mean' for n in wit_yearly.columns[1:]})
        wit_metrics[2] = wit_metrics[2].append(wit_yearly, sort=False)
        
    #*********************** START ADDED BY SHANE ***********************
    #adding median
    for y in years:
        wit_yearly = wit_df[pd.DatetimeIndex(wit_df['TIME']).year==y].drop(columns=['TIME']).groupby(pkey).median()
        wit_yearly.insert(0, 'YEAR', y)
        wit_yearly = wit_yearly.rename(columns={n: n+'_median' for n in wit_yearly.columns[1:]})
        wit_metrics[3] = wit_metrics[3].append(wit_yearly, sort=False)
    #*********************** END ADDED BY SHANE ***********************      
    for y in years:
        wit_yearly = wit_df[pd.DatetimeIndex(wit_df['TIME']).year==y][[pkey, 'BS']].groupby(pkey).count()
        wit_yearly.insert(0, 'YEAR', y)
        wit_yearly = wit_yearly.rename(columns={n: 'count' for n in wit_yearly.columns[1:]})
        #shane changed index from 3 to 4 to accomodate median added above 
        wit_metrics[4] = wit_metrics[4].append(wit_yearly, sort=False)
    for t in threshold:
        wit_df_ts = wit_df.copy(deep=True)
        wit_metrics += [pd.DataFrame()]
        wit_df_ts.loc[:, wit_df_ts.columns[2:]] = wit_df_ts.loc[:, wit_df_ts.columns[2:]].mask((wit_df_ts[wit_df_ts.columns[2:]] < t/100), np.nan)
        for y in years:
            wit_yearly = wit_df_ts[pd.DatetimeIndex(wit_df_ts['TIME']).year==y].drop(columns=['TIME']).groupby(pkey).count()
            wit_yearly.insert(0, 'YEAR', y)
            wit_yearly = wit_yearly.rename(columns={n: n+'_count'+str(t) for n in wit_yearly.columns[1:]})
            wit_metrics[-1] = wit_metrics[-1].append(wit_yearly, sort=False)
    wit_yearly_metrics = wit_metrics[0]
    for i in range(len(wit_metrics)-1):
        wit_yearly_metrics = pd.merge(wit_yearly_metrics, wit_metrics[i+1], on=[pkey, 'YEAR'], how='inner')
    return wit_yearly_metrics
    
def get_event_time(wit_ww, threshold, pkey='SYSID'):
    """
        Compute inundation event time by given threshold
        input:
            wit_df: wetness computed from wit data
            threshold: a value such that (WATER+WET > threshold) = inundation
        output:
            dateframe of inundation event time
    """
    if isinstance(threshold, pd.DataFrame):
        gid = wit_ww.index.unique()[0]
        poly_threshold = threshold.loc[gid].to_numpy()[0]
    else:
        poly_threshold = threshold
    i_start = wit_ww[wit_ww['WW'] >= poly_threshold]['TIME'].min()
    if pd.isnull(i_start):
        re = pd.DataFrame([[np.nan] * 4], columns=['start_time', 'end_time', 'duration', 'gap'], index=wit_ww.index.unique())
        re.index.name = pkey
        return re
    re_idx = np.searchsorted(wit_ww[(wit_ww['WW'] < poly_threshold)]['TIME'].values, 
                             wit_ww[(wit_ww['WW'] >= poly_threshold)]['TIME'].values)
    re_idx, count = np.unique(re_idx, return_counts=True)
    start_idx = np.zeros(len(count)+1, dtype='int')
    start_idx[1:] = np.cumsum(count)
    re_start = wit_ww[(wit_ww['WW'] >= poly_threshold)].iloc[start_idx[:-1]][['TIME']].rename(columns={'TIME': 'start_time'})
    re_end = wit_ww[(wit_ww['WW'] >= poly_threshold)].iloc[start_idx[1:] - 1][['TIME']].rename(columns={'TIME': 'end_time'})
    re = pd.concat([re_start, re_end], axis=1)
    re.insert(2, 'duration', 
              (re['end_time'] - re['start_time'] + np.timedelta64(1, 'D')).astype('timedelta64[D]').astype('timedelta64[D]'))
    re.insert(3, 'gap', np.concatenate([[np.timedelta64(0, 'D')],
                                        (re['start_time'][1:].values - re['end_time'][:-1].values - np.timedelta64(1, 'D')).astype('timedelta64[D]')]))
    re.insert(0, pkey, wit_ww.index.unique()[0])
    return re.set_index(pkey)
    
def get_im_stats(grouped_wit, im_time, wit_area):
    """
        Get inundation stats given wit data and events
        input:
            grouped_wit: wit data
            im_time: inundation events in time
        output:
            the stats of inundation events
    """
    gid = grouped_wit.index.unique()[0]
    if gid not in im_time.indices.keys():
        return pd.DataFrame([[np.nan]*5], columns=['start_time', 'max_wet', 'mean_wet', 'max_wet_area', 'mean_wet_area'],
                           index=[gid])
    re_left = np.searchsorted(grouped_wit['TIME'].values.astype('datetime64'),
                         im_time.get_group(gid)['start_time'].values, side='left')
    re_right = np.searchsorted(grouped_wit['TIME'].values.astype('datetime64'),
                         im_time.get_group(gid)['end_time'].values, side='right')
    re = pd.DataFrame()
    for a, b in zip(re_left, re_right):
        tmp = pd.concat([grouped_wit.iloc[a:a+1]['TIME'].rename('start_time').astype('datetime64'),
                         pd.Series(grouped_wit.iloc[a:b]['WW'].max(),index=[gid], name='max_wet'),
                         pd.Series(grouped_wit.iloc[a:b]['WW'].mean(),index=[gid], name='mean_wet')],
                        axis=1)
        tmp.insert(3, 'max_wet_area', tmp['max_wet'].values * wit_area[wit_area.index==gid].values)
        tmp.insert(4, 'mean_wet_area', tmp['mean_wet'].values * wit_area[wit_area.index==gid].values)
        re = re.append(tmp, sort=False)
    re.index.name = grouped_wit.index.name
    return re

def event_time(wit_df, threshold=0.01, pkey='SYSID'):
    """
        Compute the inundation events with given wit data and threshold
        input:
            wit_df: wetness computed from wit data
            threshold: a value such that (WATER+WET > threshold) = inundation,
        output:
            dataframe of events
    """
    return wit_df.groupby(pkey).apply(get_event_time, threshold=threshold, pkey=pkey).dropna().droplevel(0)

def event_stats(wit_df, wit_im, wit_area, pkey='SYSID'):
    """
        Compute inundation event stats with given wit wetness, events defined by (start_time, end_time) 
        and polygon areas
        input:
            wit_df: wetness computed from wit data
            wit_im: inundation event
            wit_area: polygon areas indexed by the key
        output:
            dataframe of event stats
    """
    grouped_im = wit_im[['start_time', 'end_time']].groupby(pkey)
    return wit_df.groupby(pkey).apply(get_im_stats, im_time=grouped_im, wit_area=wit_area).droplevel(0)

def inundation_metrics(wit_data, wit_area, threshold=0.01, pkey='SYSID'):
    """
        Compute inundation metrics with given wit data, polygon areas and threshold
        input:
            wit_data: a dataframe of wit_data
            wit_area: polygon areas indexed by the key
            threshold: a value such that (WATER+WET > threshold) = inundation
        output:
            dataframe of inundation metrics
    """
    wit_df = wit_data.copy(deep=True)
    wit_df.insert(2, 'WW', wit_df[['WATER', 'WET']].sum(axis=1))
    wit_df = wit_df.drop(columns=wit_df.columns[3:])
    wit_df['TIME'] = wit_df['TIME'].astype('datetime64')
    wit_df = wit_df.set_index(pkey)
    wit_im_time = event_time(wit_df, threshold, pkey)
    wit_im_stats = event_stats(wit_df, wit_im_time, wit_area, pkey)
    return pd.merge(wit_im_time, wit_im_stats.dropna(), on=[pkey, 'start_time'], how='inner')

def interpolate_wit(grouped_wit, pkey='SYSID'):
    daily_wit = pd.DataFrame({pkey: grouped_wit[pkey].unique()[0], 'TIME': pd.date_range(grouped_wit['TIME'].astype('datetime64[D]').min(), grouped_wit['TIME'].astype('datetime64[D]').max(), freq='D'),
                          'BS': np.nan, 'NPV': np.nan, 'PV': np.nan, 'WET': np.nan, 'WATER': np.nan})
    _, nidx, oidx = np.intersect1d(daily_wit['TIME'].to_numpy().astype('datetime64[D]'), grouped_wit['TIME'].to_numpy().astype('datetime64[D]'),
                  return_indices=True)
    daily_wit.loc[nidx, ["BS","NPV","PV","WET","WATER"]]  = grouped_wit[["BS","NPV","PV","WET","WATER"]].iloc[oidx].to_numpy()
    daily_wit = daily_wit.interpolate(axis=0)
    return daily_wit

def all_time_median(wit_data, members=[['WATER', 'WET']], pkey='SYSID'):
    """
        Compute the all time median
        input:
            wit_data: dataframe of WIT
            members: the elements which the metrics are computed against, can be a column from wit_data, e.g. 'PV'
                         or the sum of wit columns, e.g. ['WATER', 'WET']
        output:
            dataframe of median indexed by pkey
    """
    wit_df = wit_data.copy(deep=True)
    i = 0
    for m in members:
        if isinstance(m, list):
            wit_df.insert(wit_df.columns.size+i, '+'.join(m), wit_df[m].sum(axis=1))
        i += 1
    return wit_df.groupby(pkey).median()

shapefile_path = click.argument('shapefile', type=str, default='/g/data/r78/rjd547/DES-QLD_Project/data/Wet_WGS84_P.shp')
@click.group(help=__doc__)
def main():
    pass

@main.command(name='wit-dump', help='dump wit data from database')
@shapefile_path
@click.option('--pkey', type=str, help='the key to match polygon in the shapefile, e.g., SYSID', default=None)
@click.option('--input-file', type=str, help='input csv with pkey', default=None)
@click.option('--output-file', type=str, help='output csv', default=None)
@click.option('--batch', type=int, help='how manay polygons per file', default=3000)
def wit_dump(shapefile, pkey, input_file, output_file, batch):
    features = shape_list(pkey, pd.read_csv(input_file, header=None).values, shapefile)
    dump_wit_data(pkey, features, output_file, batch)

@main.command(name='wit-metrics', help='compute wit metrics')
@shapefile_path
@click.option('--pkey', type=str, help='the key to match polygon in the shapefile, e.g., SYSID', default=None)
@click.option('--input-file', type=str, help='input csv of wit', default=None)
@click.option('--output-prefix', type=str, help='prefix of output csv', default=None)
def wit_metrics(shapefile, pkey, input_file, output_prefix):
    wit_data = pd.read_csv(input_file, header=None, skipfooter=1, 
                       names=[pkey,"TIME","BS","NPV","PV","WET","WATER"], engine='python'
                      )
    features = shape_list(pkey, wit_data[pkey].unique(), shapefile)
    wit_area = get_areas(features, pkey=pkey)
    wit_yearly_metrics = annual_metrics(wit_data, pkey=pkey)
    ofn = output_prefix + "_yearly_metrics.csv"
    wit_yearly_metrics.to_csv(ofn)

    wit_median = all_time_median(wit_data, pkey=pkey)
    wit_median.loc[wit_median['WATER+WET'] < 0.01, 'WATER+WET'] = 0.01
    ofn = output_prefix + "_event_threshold.csv"
    wit_median[['WATER+WET']].to_csv(ofn)

    maxdate = pd.pivot_table(wit_data, index=pkey, values=['TIME'], aggfunc=np.max)
    maxdate['TIME'] = maxdate['TIME'].astype('datetime64')
    wit_im =inundation_metrics(wit_data, wit_area, wit_median[['WATER+WET']], pkey=pkey)
    ofn = output_prefix + '_inudation_metrics.csv'
    wit_im.to_csv(ofn)
    
    lastevent = pd.pivot_table(wit_im, index=pkey, values=['end_time'], aggfunc=np.max)
    time_since_last = pd.merge(maxdate, lastevent, on=[pkey], how='inner')
    time_since_last.insert(2, 'timesincelast', 
              (time_since_last['TIME'] - time_since_last['end_time']).astype('timedelta64[D]'))
    ofn = output_prefix + '_time_since_last_inundation.csv'
    time_since_last.to_csv(ofn)

if __name__ == '__main__':
    main()
