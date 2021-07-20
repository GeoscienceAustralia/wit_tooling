import awswrangler as wr
from shapely import geometry

def load_wit_s3(geom, db_name, table_name, session):
    """
    load wit from s3
    input:
        geom: geometry of polygon
        db_name: the `catalog` on aws where the table_name exits
        table_name: a table to store the metadata from parquet files
        session: boto3 session instance
    output:
        wit data in pd.DataFrame
    """
    polygon = geometry.shape(geom).convex_hull
    data = wr.athena.read_sql_query("select time, bs, npv, pv,  wet, water from %s where time > cast('1987-01-01' as timestamp) and ST_Equals('%s', geometry)" % (table_name, polygon.to_wkt()),
                         database=db_name,boto3_session=session)
    return data

def set_output_name(shape, name_list=[]):
    """
        generate file name for the output given the itmes in shape['properties']
    """
    if name_list == []:
        return shape['id']
    output_name = [str(shape['properties'].get(a, '')) for a in name_list]
    return '_'.join(output_name)