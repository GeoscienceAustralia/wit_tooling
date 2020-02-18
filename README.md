Tooling for WIT

The tool which generates/plots the wit data `examples/wit/wetland_brutal.py`

How to generate wit data with the tool
==========================

Requirements:
-----------

A database with postgis enabled.

Other auxilliary data/scripts/shapefiles...

Bottom line is that you can't get on the task with whatever in this repo.

How To:
------

You'll need three steps. 

- Collect all the polygons interact with/contained by the landsat path/row and ouput the results as `txt` in `$out`. 
  Two reasons: 1. Aggregate by time if the polygon(s) would cover more than one path/row; 2. See `Secondly` in Section `Why`
  
`mpirun python -m mpi4py.futures wetland_brutal wit-pathrow --output-location $out $shapefile`

- Query the datacube database with results from the last step. Reason: See `Firstly` in Section `Why`.

`mpirun python -m mpi4py.futures wetland_brutal wit-query --input-folder $in --output-location $out --union True $shapefile`

Here `$in` is `$out` from the last step and `--union True` means we want to union all the polygons and query with the unioned shape. If we set `--union False`, it will query by the shape of path/row. Usually `--union True` is a better idea, especially the tree algorithm is used and parallelized by `MPI`. It is faster than the time that is spent in querying by a larger shape and reading larger amount of data. The later slows down computation the most and reduces efficiency.

- Perform the computation (Finally)

`mpirun python -m mpi4py.futures wetland_brutal wit-cal wit-cal --feature-list $feature --datasets $datasets --aggregate $aggregate $shapefile`

`$feature` is a `txt` file in folder `$in` from the last step, which contains all the polygons to be computed together;

`$datasets` is a `pkl` file in folder `$out` from the last step, which contains `datasets` results from query;

`$aggregate = True/False` where `True` means no single path/row contains the polygon(s) so that the aggregation over time is required, `False` means no aggregating is needed. Note, this can be told from the file name of `$feature`

Why:
---
I know the workflow is a bit (or very?) stupid but just hear me out. First, datacube query will block you forever if you don't play nice with it and you don't want to waste time/ksu (or whatever it costs). Secondly, i/o slows down things a lot if open/close file happens too often, so we want to read in as much as we can when a file is opened. Partially datacube query reason stands here as well. Last, you want to do many small polygons all at once with parallization, current case, which is `MPI/OpenMP`
