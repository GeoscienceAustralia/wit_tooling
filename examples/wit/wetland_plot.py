import fiona
import pandas as pd
from wit_tooling import query_wit_data, plot_to_png

def main(shapefile, output_path):
    with fiona.open(shapefile) as allshapes:
        start_f = iter(allshapes)
        while True:
            shape = next(start_f)
            poly_name, count = query_wit_data(shape)
            if count.size == 0:
                continue
            pd.DataFrame(data=count, columns=['TIME', 'BS', 'NPV', 'PV', 'WET', 'WATER']).to_csv(output_path+str(shape['id'])+'.csv', index=False)
            b_image = plot_to_png(count, poly_name)
            with open(output_path+str(shape['id'])+'.png', 'wb') as f:
                f.write(b_image.read())

if __name__ == "__main__":
    shapefile = "/g/data1a/u46/users/ea6141/wlinsight/shapefiles/LTIMRamsarProj_3577.shp"
    output_path = 'ltim/results/'
    main(shapefile, output_path)
