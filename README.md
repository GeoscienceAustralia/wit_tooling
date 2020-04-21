Tooling for WIT

The tool which generates/plots the wit data `examples/wit/wetland_brutal.py`

The virtual product recipe `aux/fc_pd.yaml`

The job submit script `aux/*.sh`

Versin info
===========

2.0:
-----
- First working version of wit
  
2.1:
----
- Compute overlapping polygons correctly
- Popularize the names and titles of output csv and png file with the selected propoties from shape file
- Mark not enough observations (less than 4 per year) in the plots
- Add functionality of dumping csvs in database
- Fix minor bugs
  
How to generate wit data with the tool
==========================

Requirements:
-----------
OpenMPI >= 4.0.1.

datacube_stats on the refactor branch: https://github.com/opendatacube/datacube-stats/tree/refactor

A database with postgis enabled.

Other auxilliary data/scripts/shapefiles...

Installation:
-----------
- OpenMPI 4.0 can be manually installed or on NCI by `module load openmpi/4.0.1`

- Install datacube-stats/refactor
```
cd $yourworkfolder
git clone git@github.com:opendatacube/datacube-stats.git
git checkout refactor
cd datacube-stats
pip install --user -e .
```
Or if one uses the env offered by `module load dea`, a hard-set of `PYTHONPATH` is needed.
DONOT do 
```
pip install --user -e .
```
instead DO
```
export PYTHONPATH=$yourworkfolder/datacube-stats:$PYTHONPATH
```

Check `datacube-stats` is installed correctly, the version should be as shown below
```
In [1]: import datacube_stats                                                                                                                         

In [2]: datacube_stats.__version__                                                                                                                    
Out[2]: '2.0beta'
```

- Install wit_tooling
```
cd $yourworkfolder
git clone git@github.com:GeoscienceAustralia/wit_tooling.git
cd wit_tooling
pip install --user -e .
```

Note: substitute `$yourworkfolder` accordingly.

How To:
------
`cd $yourworkfolder/wit_tooling/examples/wit`

One'll need four steps. 

- Collect all the polygons interact with/contained by the landsat path/row and ouput the results as `txt` in `$out`. 
  Two reasons: 1. Aggregate by time if the polygon(s) would cover more than one path/row; 2. See `Secondly` in Section `Why`
  
`mpirun python -m mpi4py.futures wetland_brutal.py wit-pathrow --output-location $out $shapefile`

Example:

`
mpirun python -m mpi4py.futures wetland_brutal.py wit-pathrow --output-location /g/data1a/u46/users/ea6141/wlinsight/sadew/new /g/data1a/u46/users/ea6141/wlinsight/shapefiles/waterfowlandwetlands_3577.shp 
`

with `$out = /g/data1a/u46/users/ea6141/wlinsight/sadew/new`, 

and `$shapefile=/g/data1a/u46/users/ea6141/wlinsight/shapefiles/waterfowlandwetlands_3577.shp`

The results in your output folder would look like:
```
[ea6141@vdi-n24 wlinsight]$ ls sadew/new
contain_22.txt  contain_265.txt  contain_266.txt  contain_267.txt  contain_268.txt  contain_518.txt  contain_519.txt
```
Results filenames will either start with `contain` or with `intersect`. Full filename convention will look like `contain_$id.txt`. The naming convention `contain` means that all the polygons are either contained by the feature or the area of the intersection between the polygon and the feature is more than 90% of the area of the polygon. The feature here is the feature `$id` representing a landsat path row in the landsat path/row shapefile. For instance, the content of `contain_22.txt` would be
```
[ea6141@vdi-n24 wlinsight]$ cat sadew/new/contain_22.txt 
71
72
73
74
75
79
80
82
83
84
85
86
87
89
90
93
94
95
98
100
102
106
107
108
109
110
```
Where 71..110 are the feature `$id` representing the polygons in your input `$shapefile`.


When a polygon has a large area such that no single path/row would intersect with the polygon for more than 90% of the area. The intersect between polygons and path/row would be listed as
```
[ea6141@vdi-n24 wlinsight]$ cat anae/done/intersect_16_17_18_739_740_741.txt 
417688
```
which means polygon feature `417688` in `$shapefile` intersects with path/row `16, 17, 17, 749, 740 and 741`. In such case, an aggregation over time slice is required in `wit-cal` by setting `--aggregate True`.


- Query the datacube database with results from the last step. Reason: See `Firstly` in Section `Why`.

`mpirun python -m mpi4py.futures wetland_brutal.py wit-query --input-folder $in --output-location $out --union True --product-yaml $pd_yaml $shapefile`

Here `$in` is `$out` from the last step

`--union True` means that we want to union all the polygons and query with the unioned shape. If we set `--union False`, it will query by the shape of path/row. Usually `--union True` is a better idea, especially the tree algorithm is used and parallelized by `MPI`. It is faster than the time that is spent in querying by a larger shape and reading larger amount of data. The later slows down computation the most and reduces efficiency.

`$pd_yaml` is virtual product recipe

Example:

`mpirun python -m mpi4py.futures wetland_brutal.py wit-query --input-folder /g/data1a/u46/users/ea6141/wlinsight/sadew/new --output-location /g/data1a/u46/users/ea6141/wlinsight/sadew/query --union True --product-yaml /g/data1a/u46/users/ea6141/wlinsight/fc_pd.yaml /g/data1a/u46/users/ea6141/wlinsight/shapefiles/waterfowlandwetlands_3577.shp`

The result would look like
```
[ea6141@vdi-n24 wlinsight]$ ls sadew/query/
22.pkl  265.pkl  266.pkl  267.pkl  268.pkl  518.pkl  519.pkl

```
where each  `.pkl` file holds the datasets from the query from each path/row, e.g., `22.pkl` has the datasets from `contain_22.txt`.

- Perform the computation (Finally)

This step generates the results and saves them into database. A computational node with enough memory is required. Scripts of processing a single path/row and in bulk are provided https://github.com/GeoscienceAustralia/wit_tooling/tree/master/aux, refer for more details.

`mpirun python -m mpi4py.futures wetland_brutal.py wit-cal --feature-list $feature --datasets $datasets --aggregate $aggregate --product-yaml $pd_yaml $shapefile`

`$feature` is a `txt` file in folder `$in` from the last step, which contains all the polygons to be computed together;

`$datasets` is a `pkl` file in folder `$out` from the last step, which contains `datasets` results from query;

`$aggregate = True/False` where `True` means no single path/row contains the polygon(s) so that the aggregation over time is required, `False` means no aggregating is needed. Note, this can be told from the file name of `$feature`

For details of how the aggregation works, refer to the WIT documentation #fixme add link

Example

`mpirun python -m mpi4py.futures wetland_brutal.py wit-cal --feature-list /g/data1a/u46/users/ea6141/wlinsight/sadew/new/contain_22.txt --datasets /g/data1a/u46/users/ea6141/wlinsight/sadew/query/22.pkl --aggregate False --product-yaml /g/data1a/u46/users/ea6141/wlinsight/fc_pd.yaml /g/data1a/u46/users/ea6141/wlinsight/shapefiles/waterfowlandwetlands_3577.shp`

Or deal with the large polygons not contained by a single path/row, set `--aggregate True`, i.e., 
`mpirun python -m mpi4py.futures wetland_brutal.py wit-cal --feature-list /g/data1a/u46/users/ea6141/wlinsight/anae/intersect_16_17_18_739_740_741.txt --datasets /g/data1a/u46/users/ea6141/wlinsight/anane/query/16_17_18_739_740_741.pkl --aggregate True --product-yaml /g/data1a/u46/users/ea6141/wlinsight/fc_pd.yaml /g/data1a/u46/users/ea6141/wlinsight/shapefiles/waterfowlandwetlands_3577.shp`

- Plot the data

`python wetland_brutal.py wit-plot --output-location $folder --feature $id -n $property_1 -n $property_2 $shapefile`

`$folder` is where to store the csvs and pngs;

`$id` is `id` in shape file `$shapefile`, it can be left to default(`None`) such that all the polygons will be processed;

`$property_1`, `$property_2` is used to populate the output filename and plot title, it can be an entry in `properties` of polygons in the shape file `$shapefile`, or left as default(`None`) to be `id`;

Example:

`python wetland_brutal.py wit-plot --output-location sadew/results -n Site_Name shapefiles/waterfowlandwetlands_3577.shp`
, where `Site_Name` is an entry under `properties` for each polygon in the shapefile.

The results look like:
```
[ea6141@vdi-n24 wlinsight]$ ls sadew/results/
100_Lake St Clair.csv                     33_Aldinga Scrub Washpool area, Acacia Tce.csv                  67_Park Hill.csv
100_Lake St Clair.png                     33_Aldinga Scrub Washpool area, Acacia Tce.png                  67_Park Hill.png
101_Sheepwash Swamp.csv                   34_Boggy Lake.csv                                               68_Big Reedy.csv
101_Sheepwash Swamp.png                   34_Boggy Lake.png                                               68_Big Reedy.png
102_Lake George.csv                       35_Myponga Reservoir.csv                                        69_Jaffray Swamp.csv
102_Lake George.png                       35_Myponga Reservoir.png                                        69_Jaffray Swamp.png
103_Oschar Swamp.csv                      36_Tolderol Game Reserve.csv                                    6_Lake Woolpolool.csv
103_Oschar Swamp.png                      36_Tolderol Game Reserve.png                                    6_Lake Woolpolool.png
104_Iluka.csv                             37_Finniss River.csv                                            70_Lake Nadzeb.csv
104_Iluka.png                             37_Finniss River.png                                            70_Lake Nadzeb.png
105_Mullins Swamp.csv                     38_Finniss River.csv                                            71_Schofield Swamp.csv
105_Mullins Swamp.png                     38_Finniss River.png                                            71_Schofield Swamp.png
```
Why:
---
I know the workflow is a bit (or very?) stupid but just hear me out. First, datacube query will block you forever if you don't play nice with it and you don't want to waste time/ksu (or whatever it costs). Secondly, i/o slows down things a lot if open/close file happens too often, so we want to read in as much as we can when a file is opened. Partially datacube query reason stands here as well. Last, you want to do many small polygons all at once with parallization, current case, which is `MPI/OpenMP`

Run `wit-cal` in bulk:
----------------------
Normal case, `wit-pathrow` and `wit-query` doesn't require a PBS job, both can run comfortably and efficiently with resource on VDI. However, if you prefer to run it with a PBS job, a single node with `ncpus=9,mem=36GB` is enough.

Refer https://github.com/GeoscienceAustralia/wit_tooling/tree/master/aux on how to submit job to run `wit-cal` parallelly in bulk on NCI 
