from datacube import Datacube
from datacube.virtual import construct
from datacube.utils.geometry import CRS, Geometry
from datacube_wps.processes.witprocess import *
import awswrangler as wr
import yaml
from datetime import datetime
import boto3
from dask_gateway import Gateway
import os

def compute_wit(shape, time, wit_path):
    dc = Datacube()
    config = yaml.load(""" 
       about:
           identifier: WIT
           version: '0.2'
           title: WIT
           abstract: WIT polygon drill
           store_supported: True
           status_supported: True
           geometry_type: polygon
           guard_rail: False
       input:
           reproject:
             output_crs: EPSG:3577
             resolution: [-30, 30]
             resampling: nearest
           input:
             transform: apply_mask
             mask_measurement_name: pmask
             apply_to: [bs, pv, npv, TCW]
             input:
               juxtapose:
                 - product: ga_ls_fc_3
                   measurements: [bs, pv, npv]
                 - transform: datacube_wps.processes.witprocess.TWnMask
                   input:
                     juxtapose:
                       - collate:
                           - product: ga_ls8c_ard_3
                             measurements: [blue, green, red, nir, swir1, swir2, fmask, nbart_contiguity]
                             gqa_iterative_mean_xy: [0, 1]
                             dataset_predicate: datacube_wps.processes.witprocess.ls8_on
                           - product: ga_ls7e_ard_3
                             measurements: [blue, green, red, nir, swir1, swir2, fmask, nbart_contiguity]
                             gqa_iterative_mean_xy: [0, 1]
                             dataset_predicate: datacube_wps.processes.witprocess.ls7_on
                           - product: ga_ls5t_ard_3
                             measurements: [blue, green, red, nir, swir1, swir2, fmask, nbart_contiguity]
                             gqa_iterative_mean_xy: [0, 1]
                             dataset_predicate: datacube_wps.processes.witprocess.ls5_on_1ym
                       - product: ga_ls_wo_3
                         measurements: [water]
       """, Loader=yaml.CLoader)
    fc_product = construct(**config['input'])
    wit = WIT(config['about'], fc_product, '')
    print("compute", shape[0]['id'])
    query_poly = Geometry(shape[0]['geometry'], crs=CRS('EPSG:3577'))
    print("start query time", datetime.now())
    results = wit.input_data(dc, time, query_poly)
    print("start create cluster")
    gateway = Gateway()
    options = gateway.cluster_options()
    options['profile'] = 'r5_L'
    options['jupyterhub_user'] = os.getenv('JUPYTERHUB_USER')
    cluster = gateway.new_cluster(options)
    cluster.adapt(minimum=2, maximum=10)
    client = cluster.get_client()
    print("get client dashboard", client.dashboard_link)
    print("start computation", datetime.now())
    try:
        re_wit = wit.process_data(results, dict(feature=query_poly, aggregate=shape[1]))
    except:
        client.close()
        cluster.shutdown()
        print("something wrong I need recycle the workers")
    else:
        print("end computation", datetime.now())
        re_wit['geometry'] = query_poly.geom.convex_hull.to_wkt()
        session = boto3.Session(profile_name='dev')
        wr.s3.to_parquet(df=re_wit.reset_index(), 
                 path=wit_path+shape[0]['id'], compression="snappy", dataset=True, mode='overwrite',boto3_session=session)
    return