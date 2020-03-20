#!/bin/bash
#PBS -P r78 
#PBS -q normal
#PBS -l storage=gdata/rs0+gdata/fk4+gdata/v10+gdata/r78+gdata/u46+scratch/r78
#PBS -l walltime=5:00:00
#PBS -l jobfs=1GB
#PBS -l wd

source $HOME/setup-datacube-up2date.sh

echo $feature $datasets $aggregate $pdyaml $shapefile
mpirun -np 9 -bind-to none python3 -m mpi4py.futures wetland_brutal.py wit-cal --feature-list $feature --datasets $datasets --aggregate $aggregate --product-yaml $pdyaml $shapefile
