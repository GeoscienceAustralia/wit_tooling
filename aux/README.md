`job_sub_in.sh` is used to submit jobs in bulk to PBS queue

`wetland.sh` is used to setup the environment to run wit tooling

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

`num_thread` is caculated regards to how many polygons would be parallelized, yet the minimum should be `9`, DONOT change it.

`mem` is caculated as how a job would be charged on NCI, the multiplier `4` CAN be dialed up until total memory hits the limit of `192GB`

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

