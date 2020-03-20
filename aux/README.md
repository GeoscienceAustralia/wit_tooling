`job_sub_in.sh` is used to submit jobs in bulk to PBS queue

usage: 
-----

`./job_sub_in.sh $input $shapefile`

`$input` is the folder where the feature list and query results are stored

In the file:
-----------

`PDYAML` is the virtual product recipe, which should be modified to the correct path, e.g, `$youworkingfolder/wit_tooling/aux/fc_pd.yaml`

`num_thread` is caculated regards to how many polygons would be parallelized, yet the minimum should be `9`, DONOT change it.

`mem` is caculated as how a job would be charged on NCI, the multiplier `4` CAN be dialed up until total memory hits the limit of `192GB`
