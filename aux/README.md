`job_normal.sh` is used to submit individual job to PBS queue

`job_sub_in.sh` is used to submit jobs in bulk to PBS queue

`wetland.sh` is used to setup the environment to run wit tooling

`job_normal.sh`
======

usage:
---

`qsub -l ncpus=$num_cpus,mem=${mem}GB -v threads=$((num_cpus * 4)),feature=$feature,datasets=$file,aggregate=$aggregate,pdyaml=$PDYAML,shapefile=$shapefile job_normal.sh`

`$num_cpus`, `$mem` refer the manual of `qsub`

`threads`, `feature`, `datasets` and `aggregate` are the parameters required by `job_normal`.

- `threads` is the number of threads used in `OpenMP`. We oversubscribe it by 4 times of the CPUS for a). To employ the hyper-threading technique; b). increase the efficiency of CPU usage since the job is I/O bound.

- `$feature` is the parameter in `--feature-list $feature` in `wetland_brutal.py wit-cal`.

- `$datasets` is the parameter in `--datasets $datasets` in `wetland_brutal.py wit-cal`

- `$aggregate` is the parameter in `--aggregate $aggregate` in `wetland_brutal.py wit-cal`

- `$PDYAML` is the parameter in `--product-yaml $PDYAML` in `wetland_brutal.py wit-cal`

- `$shapefile` is the parameter in `wetland_brutal.py wit-cal`

Example
------

`qsub -N anae_1005 -l ncpus=48,mem=192GB -v threads=192,feature=anae//new/contain_1005.txt,datasets=anae//query/1005.pkl,aggregate=False,pdyaml=/g/data/u46/users/ea6141/wlinsight/fc_pd.yaml,shapefile=/g/data/r78/DEA_Wetlands/shapefiles/MDB_ANAE_Aug2017_modified_2019_SB_3577.shp job_normal.sh`

`job_sub_in.sh`
=============

usage: 
-----

`./job_sub_in.sh $input $shapefile`

`$input` is the folder where the feature list and query results are stored

Example:

`./job_sub_in.sh sadew/ shapefiles/waterfowlandwetlands_3577.shp`

Note: Add your work folder as prefix if needed.

In the file:
-----------

`PDYAML` is the virtual product recipe, which should be modified to the correct path, e.g, `$youworkingfolder/wit_tooling/aux/fc_pd.yaml`

`num_thread` is calculated regards to how many polygons would be parallelized, yet the minimum should be `9`, DONOT change it.

`mem` is calculated as how a job would be charged on NCI, the multiplier `UMEM=4` CAN be dialed up until total memory hits the limit of `192GB`. As in the script, when the aggregation over time slices is required, we set `UMEM=8`.


`wetland.sh`
============

usage: 
-----
`source wetland.sh`

Note: you need to run `wetland.sh` to set up the correct modules to run `wit_tooling`

In the file: 
-----

`module load dea/20200316` The file is currently loading the 16-03-2020 version of the datacube. This may change if the datacube gets updated.

`module load openmpi/4.0.1` Satisfying requirements for openmpi

`PYTHONUSERBASE` is where you installed customised packages. `wit_tooling` will be installed in this folder if you installed it as --user
`PYTHONPATH` adds the datacube-stats refactor branch to the front of your path so that we can use it.

Example:
`export PYTHONUSERBASE=/g/data/r78/rjd547/python_setup/`

`export PYTHONPATH=/g/data1a/r78/rjd547/jupyter_notebooks/datacube-stats:$PYTHONPATH`

